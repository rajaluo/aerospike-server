/*
 * msg.c
 *
 * Copyright (C) 2008-2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */


//==========================================================
// Includes.
//

#include "msg.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "fault.h"


//==========================================================
// Typedefs & constants.
//

typedef struct msg_type_entry_s {
	const msg_template *mt;
	uint16_t entry_count;
	uint32_t scratch_sz;
} msg_type_entry;

typedef struct msg_str_array_s {
	uint32_t alloc_size;
	uint32_t used_size;
	uint32_t count;
	uint32_t offset[];
} msg_str_array;

typedef struct msg_pbuf_s {
	uint32_t size;
	uint8_t data[];
} msg_pbuf;

typedef struct msg_buf_array_s {
	uint32_t alloc_size;
	uint32_t used_size;
	uint32_t count;
	uint32_t offset[];
} msg_buf_array;

// msg field header on wire.
typedef struct msg_field_hdr_s {
	uint16_t id;
	uint8_t type;
	uint8_t content[];
} __attribute__ ((__packed__)) msg_field_hdr;


//==========================================================
// Globals.
//

// Total number of "msg" objects allocated:
cf_atomic_int g_num_msgs = 0;

// Total number of "msg" objects allocated per type:
cf_atomic_int g_num_msgs_by_type[M_TYPE_MAX] = { 0 };

// Number of "msg" objects per type that can be allocated. (Default to -1,
// meaning there is no limit on allowed number of "msg" objects per type.)
static int64_t g_max_msgs_per_type = -1;

static msg_type_entry g_mte[M_TYPE_MAX];


//==========================================================
// Forward declarations.
//

static bool msg_field_is_match(msg_field_type ft1, msg_field_type ft2);
static size_t msg_get_field_wire_size(msg_field_type type, size_t field_sz, bool send_pk);
static uint32_t msg_field_stamp(const msg_field *mf, msg_type mtype, uint8_t *buf, bool send_pk);
static void msg_field_save(msg *m, msg_field *mf);
static msg_str_array *msg_str_array_create(uint32_t count, uint32_t total_sz);
static void msg_str_array_set(msg_str_array *str_a, uint32_t idx, const char *str);
static msg_buf_array *msg_buf_array_create(uint32_t count, uint32_t ele_sz);
static void msg_buf_array_set(msg_buf_array *buf_a, uint32_t idx, const uint8_t *buf, uint32_t sz);
static bool msg_str_array_get(msg_str_array *str_a, uint32_t idx, char **str_r, size_t *sz_r);
static bool msg_buf_array_get(const msg_buf_array *buf_a, uint32_t idx, uint8_t **buf_r, size_t *sz_r);


//==========================================================
// Inlines.
//

static inline msg_field_type
mf_type(const msg_field *mf, msg_type type)
{
	return g_mte[type].mt[mf->id].type;
}

static inline void
mf_destroy(msg_field *mf)
{
	if (mf->is_set) {
		if (mf->is_free) {
			cf_free(mf->u.any_buf);
			mf->is_free = false;
		}

		mf->is_set = false;
	}
}


//==========================================================
// Public API - object accounting.
//

// Limit the maximum number of "msg" objects per type (-1 means unlimited.)
void
msg_set_max_msgs_per_type(int64_t max_msgs)
{
	g_max_msgs_per_type = max_msgs;
}

// Call this instead of freeing msg directly, to keep track of all msgs.
void
msg_put(msg *m)
{
	cf_atomic_int_decr(&g_num_msgs);
	cf_atomic_int_decr(&g_num_msgs_by_type[m->type]);
	cf_rc_free(m);
}


//==========================================================
// Public API - lifecycle.
//

void
msg_type_register(msg_type type, const msg_template *mt, size_t mt_sz,
		size_t scratch_sz)
{
	cf_assert(type >= 0 && type < M_TYPE_MAX, CF_MSG, "invalid type %d", type);

	msg_type_entry *mte = &g_mte[type];
	uint16_t mt_count = (uint16_t)(mt_sz / sizeof(msg_template));

	cf_assert(! mte->mt, CF_MSG, "msg_type_register() type %d already registered", type);
	cf_assert(mt_count != 0, CF_MSG, "msg_type_register() empty template");

	uint16_t max_id = 0;

	for (uint16_t i = 0; i < mt_count; i++) {
		if (mt[i].id >= max_id) {
			max_id = mt[i].id;
		}
	}

	mte->entry_count = max_id + 1;

	msg_template *table = cf_calloc(mte->entry_count, sizeof(msg_template));

	cf_assert(table, CF_MSG, "memory");

	for (uint16_t i = 0; i < mt_count; i++) {
		table[mt[i].id] = mt[i];
	}

	mte->mt = table;
	mte->scratch_sz = (uint32_t)scratch_sz;
}

msg *
msg_create(msg_type type)
{
	if (type >= M_TYPE_MAX || ! g_mte[type].mt) {
		return NULL;
	}

	// Place a limit on the number of "msg" objects of each type that may be
	// allocated at a given time. (The default value of -1 means no limit.)
	if (g_max_msgs_per_type > 0 &&
			(int64_t)g_num_msgs_by_type[type] >= g_max_msgs_per_type) {
		cf_warning(CF_MSG, "refusing to allocate more than %ld msg of type %d", g_max_msgs_per_type, type);
		return NULL;
	}

	const msg_type_entry *mte = &g_mte[type];
	uint16_t mt_count = mte->entry_count;
	size_t u_sz = sizeof(msg) + (sizeof(msg_field) * mt_count);
	size_t a_sz = u_sz + (size_t)mte->scratch_sz;
	msg *m = cf_rc_alloc(a_sz);

	cf_assert(m, CF_MSG, "cf_rc_alloc");

	m->n_fields = mt_count;
	m->bytes_used = (uint32_t)u_sz;
	m->bytes_alloc = (uint32_t)a_sz;
	m->just_parsed = false;
	m->type = type;

	for (uint16_t i = 0; i < mt_count; i++) {
		msg_field *mf = &m->f[i];

		mf->id = i;
		mf->is_set = false;
		mf->is_free = false;
	}

	// Keep track of allocated msgs.
	cf_atomic_int_incr(&g_num_msgs);
	cf_atomic_int_incr(&g_num_msgs_by_type[type]);

	return m;
}

void
msg_destroy(msg *m)
{
	int cnt = cf_rc_release(m);

	if (cnt == 0) {
		for (uint32_t i = 0; i < m->n_fields; i++) {
			mf_destroy(&m->f[i]);
		}

		msg_put(m);
	}
	else {
		cf_assert(cnt > 0, CF_MSG, "msg_destroy(%p) extra call", m);
	}
}

void
msg_incr_ref(msg *m)
{
	cf_rc_reserve(m);
}


//==========================================================
// Public API - pack messages into flattened data.
//

size_t
msg_get_wire_size(const msg *m, bool send_pk)
{
	size_t sz = sizeof(msg_hdr);

	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			sz += msg_get_field_wire_size(mf_type(mf, m->type), mf->field_sz,
					send_pk);
		}
	}

	return sz;
}

size_t
msg_get_template_fixed_sz(const msg_template *mt, size_t mt_count, bool send_pk)
{
	size_t sz = sizeof(msg_hdr);

	for (size_t i = 0; i < mt_count; i++) {
		sz += msg_get_field_wire_size(mt[i].type, 0, send_pk);
	}

	return sz;
}

size_t
msg_to_wire(const msg *m, uint8_t *buf, bool send_pk)
{
	msg_hdr *hdr = (msg_hdr *)buf;

	hdr->type = cf_swap_to_be16(m->type);

	buf += sizeof(msg_hdr);

	const uint8_t *body = buf;

	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			buf += msg_field_stamp(mf, m->type, buf, send_pk);
		}
	}

	uint32_t body_sz = (uint32_t)(buf - body);

	hdr->size = cf_swap_to_be32(body_sz);

	return sizeof(msg_hdr) + body_sz;
}


//==========================================================
// Public API - parse flattened data into messages.
//

int
msg_parse(msg *m, const uint8_t *buf, size_t bufsz)
{
	if (bufsz < sizeof(msg_hdr)) {
		return -1;
	}

	const msg_hdr *hdr = (const msg_hdr *)buf;
	buf += sizeof(msg_hdr);

	uint32_t sz = cf_swap_from_be32(hdr->size);
	uint16_t type = cf_swap_from_be16(hdr->type);

	if (bufsz < sz + sizeof(msg_hdr)) {
		return -2;
	}

	if (m->type != type) {
		cf_ticker_warning(CF_MSG, "parsed type %d for msg type %d", type, m->type);
		return -3;
	}

	const uint8_t *eob = buf + sz;
	size_t left = sz;

	while (left != 0) {
		if (left < sizeof(msg_field_hdr) + sizeof(uint32_t)) {
			return -4;
		}

		const msg_field_hdr *fhdr = (const msg_field_hdr *)buf;
		buf += sizeof(msg_field_hdr);

		uint32_t id = (uint32_t)cf_swap_from_be16(fhdr->id);
		msg_field_type ft = (msg_field_type)fhdr->type;
		size_t fsz;
		uint32_t size = 0;

		switch (ft) {
		case M_FT_PK_UINT32:
			fsz = sizeof(uint32_t);
			break;
		case M_FT_PK_UINT64:
			fsz = sizeof(uint64_t);
			break;
		default:
			size = sizeof(uint32_t);
			fsz = cf_swap_from_be32(*(const uint32_t *)buf);
			buf += sizeof(uint32_t);
			break;
		}

		if (left < sizeof(msg_field_hdr) + size + fsz) {
			return -5;
		}

		msg_field *mf;

		if (id >= m->n_fields) {
			mf = NULL;
		}
		else {
			mf = &m->f[id];
		}

		if (mf && ! msg_field_is_match(ft, mf_type(mf, m->type))) {
			cf_ticker_warning(CF_MSG, "msg type %d: parsed type %d for field type %d", m->type, ft, mf_type(mf, m->type));
			mf = NULL;
		}

		if (mf) {
			mf->is_set = true;

			switch (mf_type(mf, m->type)) {
			case M_FT_UINT32:
				mf->u.ui32 = cf_swap_from_be32(*(uint32_t *)buf);
				break;
			case M_FT_UINT64:
				mf->u.ui64 = cf_swap_from_be64(*(uint64_t *)buf);
				break;
			case M_FT_STR:
			case M_FT_BUF:
			case M_FT_ARRAY_UINT32:
			case M_FT_ARRAY_UINT64:
			case M_FT_ARRAY_STR:
			case M_FT_ARRAY_BUF:
				mf->field_sz = (uint32_t)fsz;
				mf->u.any_buf = (void *)buf;
				mf->is_free = false;
				break;
			default:
				cf_ticker_detail(CF_MSG, "msg_parse: field type %d not supported - skipping", mf_type(mf, m->type));
				mf->is_set = false;
				break;
			}
		}

		if (eob < buf) {
			break;
		}

		buf += fsz;
		left = (size_t)(eob - buf);
	}

	m->just_parsed = true;

	return 0;
}

int
msg_get_initial(uint32_t *size_r, msg_type *type_r, const uint8_t *buf,
		uint32_t bufsz)
{
	if (bufsz < sizeof(msg_hdr)) {
		return -1;
	}

	const msg_hdr *hdr = (const msg_hdr *)buf;

	*size_r = cf_swap_from_be32(hdr->size) + (uint32_t)sizeof(msg_hdr);
	*type_r = (msg_type)cf_swap_from_be16(hdr->type);

	return 0;
}

void
msg_reset(msg *m)
{
	m->bytes_used = (uint32_t)((m->n_fields * sizeof(msg_field)) + sizeof(msg));
	m->just_parsed = false;

	for (uint16_t i = 0; i < m->n_fields; i++) {
		mf_destroy(&m->f[i]);
	}
}

void
msg_preserve_fields(msg *m, uint32_t n_field_ids, ...)
{
	bool reflect[m->n_fields];

	for (uint16_t i = 0; i < m->n_fields; i++) {
		reflect[i] = false;
	}

	va_list argp;
	va_start(argp, n_field_ids);

	for (uint32_t n = 0; n < n_field_ids; n++) {
		reflect[va_arg(argp, int)] = true;
	}

	va_end(argp);

	for (uint32_t i = 0; i < m->n_fields; i++) {
		msg_field *mf = &m->f[i];

		if (mf->is_set) {
			if (reflect[i]) {
				if (m->just_parsed) {
					msg_field_save(m, mf);
				}
			}
			else {
				mf->is_set = false;
			}
		}
	}

	m->just_parsed = false;
}

void
msg_preserve_all_fields(msg *m)
{
	if (! m->just_parsed) {
		return;
	}

	for (uint32_t i = 0; i < m->n_fields; i++) {
		msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_save(m, mf);
		}
	}

	m->just_parsed = false;
}


//==========================================================
// Public API - set fields in messages.
//

int
msg_set_uint32(msg *m, int field_id, uint32_t v)
{
	m->f[field_id].is_set = true;
	m->f[field_id].u.ui32 = v;

	return 0;
}

int
msg_set_int32(msg *m, int field_id, int32_t v)
{
	m->f[field_id].is_set = true;
	m->f[field_id].u.i32 = v;

	return 0;
}

int
msg_set_uint64(msg *m, int field_id, uint64_t v)
{
	m->f[field_id].is_set = true;
	m->f[field_id].u.ui64 = v;

	return 0;
}

int
msg_set_int64(msg *m, int field_id, int64_t v)
{
	m->f[field_id].is_set = true;
	m->f[field_id].u.i64 = v;

	return 0;
}

int
msg_set_str(msg *m, int field_id, const char *v, msg_set_type type)
{
	msg_field *mf = &m->f[field_id];

	mf_destroy(mf);

	mf->field_sz = (uint32_t)strlen(v) + 1;

	if (type == MSG_SET_COPY) {
		uint32_t fsz = mf->field_sz;

		if (m->bytes_alloc - m->bytes_used >= fsz) {
			mf->u.str = (char *)m + m->bytes_used;
			m->bytes_used += fsz;
			mf->is_free = false;
			memcpy(mf->u.str, v, fsz);
		}
		else {
			mf->u.str = cf_strdup(v);
			cf_assert(mf->u.str, CF_MSG, "malloc");
			mf->is_free = true;
		}
	}
	else if (type == MSG_SET_HANDOFF_MALLOC) {
		mf->u.str = (char *)v;
		mf->is_free = (v != NULL);

		if (! v) {
			cf_warning(CF_MSG, "handoff malloc with null pointer");
		}
	}

	mf->is_set = true;

	return 0;
}

int
msg_set_buf(msg *m, int field_id, const uint8_t *v, size_t sz,
		msg_set_type type)
{
	msg_field *mf = &m->f[field_id];

	mf_destroy(mf);

	mf->field_sz = (uint32_t)sz;

	if (type == MSG_SET_COPY) {
		if (m->bytes_alloc - m->bytes_used >= sz) {
			mf->u.buf = (uint8_t *)m + m->bytes_used;
			m->bytes_used += (uint32_t)sz;
			mf->is_free = false;
		}
		else {
			mf->u.buf = cf_malloc(sz);
			cf_assert(mf->u.buf, CF_MSG, "malloc");
			mf->is_free = true;
		}

		memcpy(mf->u.buf, v, sz);

	}
	else if (type == MSG_SET_HANDOFF_MALLOC) {
		mf->u.buf = (void *)v;
		mf->is_free = (v != NULL);

		if (! v) {
			cf_warning(CF_MSG, "handoff malloc with null pointer");
		}
	}

	mf->is_set = true;

	return 0;
}

int
msg_set_uint32_array_size(msg *m, int field_id, uint32_t count)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_uint32_array_size() field already set");

	mf->field_sz = (uint32_t)(count * sizeof(uint32_t));
	mf->u.ui32_a = cf_malloc(mf->field_sz);

	cf_assert(mf->u.ui32_a, CF_MSG, "malloc");

	mf->is_set = true;
	mf->is_free = true;

	return 0;
}

int
msg_set_uint32_array(msg *m, int field_id, uint32_t idx, uint32_t v)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_uint32_array() field not set");
	cf_assert(idx < (mf->field_sz >> 2), CF_MSG, "msg_set_uint32_array() idx out of bounds");

	mf->u.ui32_a[idx] = cf_swap_to_be32(v);

	return 0;
}

int
msg_set_uint64_array_size(msg *m, int field_id, uint32_t count)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_uint64_array_size() field already set");

	mf->field_sz = (uint32_t)(count * sizeof(uint64_t));
	mf->u.ui64_a = cf_malloc(mf->field_sz);

	cf_assert(mf->u.ui64_a, CF_MSG, "malloc");

	mf->is_set = true;
	mf->is_free = true;

	return 0;
}

int
msg_set_uint64_array(msg *m, int field_id, uint32_t idx, uint64_t v)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_uint64_array() field not set");
	cf_assert(idx < (mf->field_sz >> 3), CF_MSG, "msg_set_uint64_array() idx out of bounds");

	mf->u.ui64_a[idx] = cf_swap_to_be64(v);

	return 0;
}

int
msg_set_str_array_size(msg *m, int field_id, uint32_t count, uint32_t total_sz)
{
	cf_assert(count != 0 && total_sz != 0, CF_MSG, "Invalid params");

	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_str_array_size() field already set");

	mf->u.str_a = msg_str_array_create(count, total_sz);

	cf_assert(mf->u.str_a, CF_MSG, "malloc");

	mf->field_sz = mf->u.str_a->used_size;
	mf->is_free = true;
	mf->is_set = true;

	return 0;
}

int
msg_set_str_array(msg *m, int field_id, uint32_t idx, const char *str)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_str_array() field not set");

	msg_str_array *str_a = mf->u.str_a;

	msg_str_array_set(str_a, idx, str);
	mf->field_sz = str_a->used_size;

	return 0;
}

int
msg_set_buf_array_size(msg *m, int field_id, uint32_t count,
		uint32_t ele_sz)
{
	cf_assert(count != 0 && ele_sz != 0, CF_MSG, "Invalid params");

	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_buf_array_size() field already set");

	mf->u.buf_a = msg_buf_array_create(count, ele_sz);

	cf_assert(mf->u.buf_a, CF_MSG, "malloc");

	mf->field_sz = mf->u.buf_a->used_size;
	mf->is_free = true;
	mf->is_set = true;

	return 0;
}

int
msg_set_buf_array(msg *m, int field_id, uint32_t idx, const uint8_t *buf,
		size_t sz)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_buf_array() field not set");

	msg_buf_array *buf_a = mf->u.buf_a;

	msg_buf_array_set(buf_a, idx, buf, (uint32_t)sz);
	mf->field_sz = buf_a->used_size;

	return 0;
}


//==========================================================
// Public API - get fields from messages.
//

msg_field_type
msg_field_get_type(const msg *m, uint16_t id)
{
	return mf_type(&m->f[id], m->type);
}

int
msg_get_uint32(const msg *m, int field_id, uint32_t *val_r)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	*val_r = m->f[field_id].u.ui32;

	return 0;
}

int
msg_get_int32(const msg *m, int field_id, int32_t *val_r)
{
	if (! &m->f[field_id].is_set) {
		return -1;
	}

	*val_r = m->f[field_id].u.i32;

	return 0;
}

int
msg_get_uint64(const msg *m, int field_id, uint64_t *val_r)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	*val_r = m->f[field_id].u.ui64;

	return 0;
}

int
msg_get_int64(const msg *m, int field_id, int64_t *val_r)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	*val_r = m->f[field_id].u.i64;

	return 0;
}

int
msg_get_str(const msg *m, int field_id, char **str_r, size_t *sz_r,
		msg_get_type type)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	if (type == MSG_GET_DIRECT) {
		*str_r = m->f[field_id].u.str;
	}
	else if (type == MSG_GET_COPY_MALLOC) {
		*str_r = cf_strdup(m->f[field_id].u.str);
		cf_assert(*str_r, CF_MSG, "malloc");
	}
	else {
		cf_crash(CF_MSG, "msg_get_str: illegal msg_get_type");
	}

	if (sz_r) {
		*sz_r = m->f[field_id].field_sz;
	}

	return 0;
}

int
msg_get_buf(const msg *m, int field_id, uint8_t **buf_r, size_t *sz_r,
		msg_get_type type)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	if (type == MSG_GET_DIRECT) {
		*buf_r = m->f[field_id].u.buf;
	}
	else if (type == MSG_GET_COPY_MALLOC) {
		*buf_r = cf_malloc(m->f[field_id].field_sz);
		cf_assert(*buf_r, CF_MSG, "malloc");
		memcpy(*buf_r, m->f[field_id].u.buf, m->f[field_id].field_sz);
	}
	else {
		cf_crash(CF_MSG, "msg_get_buf: illegal msg_get_type");
	}

	if (sz_r) {
		*sz_r = m->f[field_id].field_sz;
	}

	return 0;
}

bool
msg_is_set(const msg *m, int field_id)
{
	cf_assert(field_id >= 0 && field_id < (int)m->n_fields, CF_MSG, "invalid field_id %d", field_id);

	return m->f[field_id].is_set;
}

int
msg_get_uint32_array(msg *m, int field_id, uint32_t index, uint32_t *val_r)
{
	msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*val_r = cf_swap_from_be32(mf->u.ui32_a[index]);

	return 0;
}

int
msg_get_uint64_array_count(msg *m, int field_id, uint32_t *count_r)
{
	msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*count_r = mf->field_sz >> 3;

	return 0;
}

int
msg_get_uint64_array(msg *m, int field_id, uint32_t index, uint64_t *val_r)
{
	msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*val_r = cf_swap_from_be64(mf->u.ui64_a[index]);

	return 0;
}

int
msg_get_str_array(msg *m, int field_id, uint32_t idx, char **str_r,
		size_t *sz_r, msg_get_type type)
{
	msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		cf_warning(CF_MSG, "msg_get_str_array() field not set");
		return -1;
	}

	if (! mf->u.str_a) {
		cf_warning(CF_MSG, "msg_get_str_array() is_set but no str array");
		return -2;
	}

	msg_str_array *str_a = mf->u.str_a;

	char *b;

	if (! sz_r) {
		sz_r = alloca(sizeof(size_t));
	}

	if (! msg_str_array_get(str_a, idx, &b, sz_r)) {
		return -3;
	}

	switch (type) {
	case MSG_GET_DIRECT:
		*str_r = b;
		break;
	case MSG_GET_COPY_MALLOC:
		*str_r = cf_malloc(*sz_r);
		memcpy(*str_r, b, *sz_r);
		break;
	default:
		cf_crash(CF_MSG, "unexpected msg_get_type");
		break;
	}

	return 0;
}

int
msg_get_buf_array_size(const msg *m, int field_id, int *count_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	if (! mf->u.buf_a) {
		cf_warning(CF_MSG, "msg_get_buf_array_size() is_set but no buf array");
		return -2;
	}

	msg_buf_array *buf_a = mf->u.buf_a;

	*count_r = (int)buf_a->count;

	return 0;
}

int
msg_get_buf_array(const msg *m, int field_id, uint32_t idx, uint8_t **buf_r,
		size_t *sz_r, msg_get_type type)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		cf_warning(CF_MSG, "msg_get_buf_array() field not set");
		return -1;
	}

	if (! mf->u.buf_a) {
		cf_warning(CF_MSG, "msg_get_buf_array() is set but no buf array");
		return -2;
	}

	const msg_buf_array *buf_a = mf->u.buf_a;

	uint8_t *b;

	if (! sz_r) {
		sz_r = alloca(sizeof(size_t));
	}

	if (! msg_buf_array_get(buf_a, idx, &b, sz_r)) {
		return -3;
	}

	switch (type) {
	case MSG_GET_DIRECT:
		*buf_r = b;
		break;
	case MSG_GET_COPY_MALLOC:
		*buf_r = cf_malloc(*sz_r);
		memcpy(*buf_r, b, *sz_r);
		break;
	default:
		cf_crash(CF_MSG, "unexpected msg_get_type");
		break;
	}

	return 0;
}


//==========================================================
// Public API - debugging only.
//

void
msg_dump(const msg *m, const char *info)
{
	cf_info(CF_MSG, "msg_dump: %s: msg %p rc %d n-fields %u bytes-used %u bytes-alloc'd %u type %d",
			info, m, (int)cf_rc_count((void*)m), m->n_fields, m->bytes_used,
			m->bytes_alloc, m->type);

	for (uint32_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf =  &m->f[i];

		cf_info(CF_MSG, "mf %02u: id %u is-set %d", i, mf->id, mf->is_set);

		if (mf->is_set) {
			switch (mf_type(mf, m->type)) {
			case M_FT_PK_UINT32:
			case M_FT_UINT32:
				cf_info(CF_MSG, "   type UINT32 value %u", mf->u.ui32);
				break;
			case M_FT_PK_UINT64:
			case M_FT_UINT64:
				cf_info(CF_MSG, "   type UINT64 value %lu", mf->u.ui64);
				break;
			case M_FT_STR:
				cf_info(CF_MSG, "   type STR sz %u free %c value %s",
						mf->field_sz, mf->is_free ? 't' : 'f', mf->u.str);
				break;
			case M_FT_BUF:
				cf_info_binary(CF_MSG, mf->u.buf, mf->field_sz,
						CF_DISPLAY_HEX_COLUMNS,
						"   type BUF sz %u free %c value ",
						mf->field_sz, mf->is_free ? 't' : 'f');
				break;
			case M_FT_ARRAY_UINT32:
				cf_info(CF_MSG, "   type ARRAY_UINT32: count %u n-uint32 %u free %c",
						mf->field_sz, mf->field_sz >> 2,
						mf->is_free ? 't' : 'f');
				{
					uint32_t n_ints = mf->field_sz >> 2;
					for (uint32_t j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %u value %u",
								j, ntohl(mf->u.ui32_a[j]));
					}
				}
				break;
			case M_FT_ARRAY_UINT64:
				cf_info(CF_MSG, "   type ARRAY_UINT64: count %u n-uint64 %u free %c",
						mf->field_sz, mf->field_sz >> 3,
						mf->is_free ? 't' : 'f');
				{
					uint32_t n_ints = mf->field_sz >> 3;
					for (uint32_t j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %u value %lu",
								j, __bswap_64(mf->u.ui64_a[j]));
					}
				}
				break;
			default:
				cf_info(CF_MSG, "   type %d unknown", mf_type(mf, m->type));
				break;
			}
		}
	}
}


//==========================================================
// Local helpers.
//

static bool
msg_field_is_match(msg_field_type ft1, msg_field_type ft2)
{
	switch (ft1) {
	case M_FT_UINT32:
	case M_FT_UINT64:
		if ((int)ft1 - 1 == (int)ft2) {
			return true;
		}
		break;
	case M_FT_PK_UINT32:
	case M_FT_PK_UINT64:
		if ((int)ft1 + 1 == (int)ft2) {
			return true;
		}
		break;
	default:
		break;
	}

	return ft1 == ft2;
}

static size_t
msg_get_field_wire_size(msg_field_type type, size_t field_sz, bool send_pk)
{
	switch (type) {
	case M_FT_UINT32:
		return send_pk ? sizeof(msg_field_hdr) + sizeof(uint32_t) :
				sizeof(msg_field_hdr) + sizeof(uint32_t) + sizeof(uint32_t);
	case M_FT_UINT64:
		return send_pk ? sizeof(msg_field_hdr) + sizeof(uint64_t) :
				sizeof(msg_field_hdr) + sizeof(uint32_t) + sizeof(uint64_t);
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
		break;
	default:
		cf_crash(CF_MSG, "unexpected field type %d", type);
		break;
	}

	return sizeof(msg_field_hdr) + sizeof(uint32_t) + field_sz;
}

// Returns the number of bytes written.
static uint32_t
msg_field_stamp(const msg_field *mf, msg_type mtype, uint8_t *buf, bool send_pk)
{
	msg_field_hdr *hdr = (msg_field_hdr *)buf;
	msg_field_type type = mf_type(mf, mtype);

	buf += sizeof(msg_field_hdr);

	hdr->id = cf_swap_to_be16((uint16_t)mf->id);

	if (send_pk) {
		switch (type) {
		case M_FT_UINT32:
			hdr->type = M_FT_PK_UINT32;
			*(uint32_t *)buf = cf_swap_to_be32(mf->u.ui32);
			return sizeof(msg_field_hdr) + sizeof(uint32_t);
		case M_FT_UINT64:
			hdr->type = M_FT_PK_UINT64;
			*(uint64_t *)buf = cf_swap_to_be64(mf->u.ui64);
			return sizeof(msg_field_hdr) + sizeof(uint64_t);
		default:
			break;
		}
	}

	hdr->type = (uint8_t)type;

	uint32_t fsz;
	uint32_t *p_fsz = (uint32_t *)buf;

	buf += sizeof(uint32_t);

	switch (type) {
	case M_FT_UINT32:
		fsz = sizeof(uint32_t);
		*(uint32_t *)buf = cf_swap_to_be32(mf->u.ui32);
		break;
	case M_FT_UINT64:
		fsz = sizeof(uint64_t);
		*(uint64_t *)buf = cf_swap_to_be64(mf->u.ui64);
		break;
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
		fsz = mf->field_sz;
		memcpy(buf, mf->u.any_buf, fsz);
		break;
	default:
		cf_crash(CF_MSG, "unexpected field type %d", type);
		return 0;
	}

	*p_fsz = cf_swap_to_be32(fsz);

	return (uint32_t)(sizeof(msg_field_hdr) + sizeof(uint32_t) + fsz);
}

static void
msg_field_save(msg *m, msg_field *mf)
{
	switch (mf_type(mf, m->type)) {
	case M_FT_PK_UINT32:
	case M_FT_UINT32:
	case M_FT_PK_UINT64:
	case M_FT_UINT64:
		break;
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
		// Should only preserve received messages where buffer pointers point
		// directly into a fabric buffer.
		cf_assert(! mf->is_free, CF_MSG, "invalid msg preserve");

		if (m->bytes_alloc - m->bytes_used >= mf->field_sz) {
			void *buf = ((uint8_t *)m) + m->bytes_used;

			memcpy(buf, mf->u.any_buf, mf->field_sz);
			mf->u.any_buf = buf;
			m->bytes_used += mf->field_sz;
			mf->is_free = false;
		}
		else {
			void *buf = cf_malloc(mf->field_sz);
			cf_assert(buf, CF_MSG, "malloc");

			memcpy(buf, mf->u.any_buf, mf->field_sz);
			mf->u.any_buf = buf;
			mf->is_free = true;
		}
		break;
	default:
		break;
	}
}

static msg_str_array *
msg_str_array_create(uint32_t count, uint32_t total_sz)
{
	size_t sz = sizeof(msg_str_array) + (count * sizeof(uint32_t)) + total_sz;
	msg_str_array *str_a = cf_malloc(sz);

	if (! str_a) {
		return NULL;
	}

	str_a->alloc_size = (uint32_t)sz;
	str_a->used_size =
			(uint32_t)(sizeof(msg_str_array) + (count * sizeof(uint32_t)));

	str_a->count = count;

	for (uint32_t i = 0; i < count; i++) {
		str_a->offset[i] = 0;
	}

	return str_a;
}

static void
msg_str_array_set(msg_str_array *str_a, uint32_t idx, const char *str)
{
	cf_assert(idx < str_a->count, CF_MSG, "msg_str_array_set() idx %u > %u out of bound", idx, str_a->count);

	uint32_t sz = (uint32_t)strlen(str) + 1;

	cf_assert(str_a->used_size + sz <= str_a->alloc_size, CF_MSG, "msg_str_array_set() used_sz %u > %u out of bound",
			str_a->used_size + sz, str_a->alloc_size);

	str_a->offset[idx] = str_a->used_size;

	char *data = (char *)str_a + str_a->offset[idx];

	strncpy(data, str, sz);
	str_a->used_size += sz;
}

static msg_buf_array *
msg_buf_array_create(uint32_t count, uint32_t ele_sz)
{
	size_t a_sz = sizeof(msg_buf_array) +
			(count * (sizeof(uint32_t) + ele_sz + sizeof(msg_pbuf)));
	msg_buf_array *buf_a = cf_malloc(a_sz);

	if (! buf_a) {
		return NULL;
	}

	buf_a->alloc_size = (uint32_t)a_sz;
	buf_a->used_size =
			(uint32_t)(sizeof(msg_buf_array) + (count * sizeof(uint32_t)));
	buf_a->count = count;

	for (uint32_t i = 0; i < count; i++) {
		buf_a->offset[i] = 0;
	}

	return buf_a;
}

static void
msg_buf_array_set(msg_buf_array *buf_a, uint32_t idx, const uint8_t *buf,
		uint32_t sz)
{
	cf_assert(idx < buf_a->count, CF_MSG, "msg_buf_array_set() idx %u > %u out of bound", idx, buf_a->count);

	cf_assert(buf_a->used_size + sz + (uint32_t)sizeof(msg_pbuf) <= buf_a->alloc_size, CF_MSG, "msg_buf_array_set() used_sz %u > %u out of bound",
			buf_a->used_size + sz + (uint32_t)sizeof(msg_pbuf), buf_a->alloc_size);

	buf_a->offset[idx] = buf_a->used_size;

	msg_pbuf *pbuf = (msg_pbuf *)((uint8_t *)buf_a + buf_a->used_size);

	pbuf->size = sz;
	memcpy(pbuf->data, buf, sz);

	buf_a->used_size += sz + (uint32_t)sizeof(msg_pbuf);
}

static bool
msg_str_array_get(msg_str_array *str_a, uint32_t idx, char **str_r,
		size_t *sz_r)
{
	if (idx > str_a->count) {
		cf_warning(CF_MSG, "msg_str_array_get: idx %u > %u out of bound", idx, str_a->count);
		return false;
	}

	if (str_a->offset[idx] == 0) {
		return false;
	}

	*str_r = (char *)str_a + str_a->offset[idx];
	*sz_r = strlen(*str_r) + 1;

	return true;
}

static bool
msg_buf_array_get(const msg_buf_array *buf_a, uint32_t idx, uint8_t **buf_r,
		size_t *sz_r)
{
	if (idx > buf_a->count) {
		cf_warning(CF_MSG, "msg_buf_array_get: idx %u > %u out of bound", idx, buf_a->count);
		return false;
	}

	if (buf_a->offset[idx] == 0) {
		return false;
	}

	msg_pbuf *pbuf = (msg_pbuf *)((uint8_t *)buf_a + buf_a->offset[idx]);

	*sz_r = pbuf->size;
	*buf_r = pbuf->data;

	return true;
}
