/*
 * transaction.h
 *
 * Copyright (C) 2008-2015 Aerospike, Inc.
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


#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "msg.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "storage/storage.h"


//==========================================================
// Microbenchmark macros.
//

#define MICROBENCHMARK_SET_TO_START() \
{ \
	if (g_config.microbenchmarks) { \
		tr.microbenchmark_time = tr.start_time; \
	} \
}

#define MICROBENCHMARK_SET_TO_START_P() \
{ \
	if (g_config.microbenchmarks) { \
		tr->microbenchmark_time = tr->start_time; \
	} \
}

#define MICROBENCHMARK_HIST_INSERT(__hist_name) \
{ \
	if (g_config.microbenchmarks && tr.microbenchmark_time != 0) { \
		histogram_insert_data_point(g_config.__hist_name, tr.microbenchmark_time); \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_P(__hist_name) \
{ \
	if (g_config.microbenchmarks && tr->microbenchmark_time != 0) { \
		histogram_insert_data_point(g_config.__hist_name, tr->microbenchmark_time); \
	} \
}

#define MICROBENCHMARK_RESET() \
{ \
	if (g_config.microbenchmarks) { \
		tr.microbenchmark_time = cf_getns(); \
	} \
}

#define MICROBENCHMARK_RESET_P() \
{ \
	if (g_config.microbenchmarks) { \
		tr->microbenchmark_time = cf_getns(); \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_AND_RESET(__hist_name) \
{ \
	if (g_config.microbenchmarks) { \
		if (tr.microbenchmark_time != 0) { \
			tr.microbenchmark_time = histogram_insert_data_point(g_config.__hist_name, tr.microbenchmark_time); \
		} \
		else { \
			tr.microbenchmark_time = cf_getns(); \
		} \
	} \
}

#define MICROBENCHMARK_HIST_INSERT_AND_RESET_P(__hist_name) \
{ \
	if (g_config.microbenchmarks) { \
		if (tr->microbenchmark_time != 0) { \
			tr->microbenchmark_time = histogram_insert_data_point(g_config.__hist_name, tr->microbenchmark_time); \
		} \
		else { \
			tr->microbenchmark_time = cf_getns(); \
		} \
	} \
}


//==========================================================
// Client socket information - as_file_handle.
//

typedef struct as_file_handle_s {
	char		client[64];		// client identifier (currently ip-addr:port)
	uint64_t	last_used;		// last ms we read or wrote
	int			fd;
	int			epoll_fd;		// the file descriptor of our epoll instance
	bool		reap_me;		// tells the reaper to come and get us
	bool		trans_active;	// a transaction is running on this connection
	uint32_t	fh_info;		// bitmap containing status info of this file handle
	as_proto	*proto;
	uint64_t	proto_unread;
	void		*security_filter;
} as_file_handle;

#define FH_INFO_DONOT_REAP	0x00000001	// this bit indicates that this file handle should not be reaped
#define FH_INFO_XDR			0x00000002	// the file handle belongs to an XDR connection

// Helpers to release transaction file handles.
void as_release_file_handle(as_file_handle *proto_fd_h);
void as_end_of_transaction(as_file_handle *proto_fd_h, bool force_close);
void as_end_of_transaction_ok(as_file_handle *proto_fd_h);
void as_end_of_transaction_force_close(as_file_handle *proto_fd_h);


//==========================================================
// Transaction.
//

// How to interpret the 'from' union.
//
// NOT a generic transaction type flag, e.g. batch sub-transactions that proxy
// are FROM_PROXY on the proxyee node, hence we still need a separate
// FROM_FLAG_BATCH_SUB.
//
typedef enum {
	// External, comes through demarshal or fabric:
	FROM_CLIENT	= 1,
	FROM_PROXY,

	// Internal, generated on local node:
	FROM_BATCH,
	FROM_IUDF,
	FROM_NSUP,

	FROM_UNDEF	= 0
} transaction_origin;

struct iudf_origin_s;
struct as_batch_shared_s;

typedef struct as_transaction_s {

	//------------------------------------------------------
	// transaction 'head' - copied onto queue.
	//

	cl_msg*		msgp;
	uint32_t	msg_fields;

	uint8_t		origin;
	uint8_t		from_flags;

	bool		microbenchmark_is_resolve; // will soon be gone
	// Spare byte.

	union {
		void*						any;
		as_file_handle*				proto_fd_h;
		cf_node						proxy_node;
		struct iudf_origin_s*		iudf_orig;
		struct as_batch_shared_s*	batch_shared;
	} from;

	union {
		uint32_t any;
		uint32_t proxy_tid;
		uint32_t batch_index;
	} from_data;

	cf_digest	keyd; // only batch sub-transactions require this on queue

	uint64_t	start_time;
	uint64_t	microbenchmark_time;

	//<><><><><><><><><><><> 64 bytes <><><><><><><><><><><>

	//------------------------------------------------------
	// transaction 'body' - NOT copied onto queue.
	//

	as_partition_reservation rsv;

	uint64_t	end_time;
	uint8_t		result_code;
	uint8_t		flags;
	uint16_t	generation;
	uint32_t	void_time;

} as_transaction;

#define AS_TRANSACTION_HEAD_SIZE (offsetof(as_transaction, rsv))

// 'from_flags' bits - set before queuing transaction head:
#define FROM_FLAG_NSUP_DELETE	0x0001
#define FROM_FLAG_BATCH_SUB		0x0002
#define FROM_FLAG_SHIPPED_OP	0x0004

// 'flags' bits - set in transaction body after queuing:
#define AS_TRANSACTION_FLAG_SINDEX_TOUCHED	0x0001


void as_transaction_init_head(as_transaction *tr, cf_digest *, cl_msg *);
void as_transaction_init_body(as_transaction *tr);

void as_transaction_copy_head(as_transaction *to, const as_transaction *from);

bool as_transaction_set_msg_field_flag(as_transaction *tr, uint8_t type);
bool as_transaction_demarshal_prepare(as_transaction *tr);
void as_transaction_proxyee_prepare(as_transaction *tr);

static inline bool
as_transaction_is_batch_sub(const as_transaction *tr)
{
	return (tr->from_flags & FROM_FLAG_BATCH_SUB) != 0;
}

static inline bool
as_transaction_has_set(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SET) != 0;
}

static inline bool
as_transaction_has_key(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_KEY) != 0;
}

static inline bool
as_transaction_has_digest(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_DIGEST_RIPE) != 0;
}

static inline bool
as_transaction_has_no_key_or_digest(const as_transaction *tr)
{
	return (tr->msg_fields & (AS_MSG_FIELD_BIT_KEY | AS_MSG_FIELD_BIT_DIGEST_RIPE)) == 0;
}

static inline bool
as_transaction_is_multi_record(const as_transaction *tr)
{
	return	(tr->msg_fields & (AS_MSG_FIELD_BIT_KEY | AS_MSG_FIELD_BIT_DIGEST_RIPE)) == 0 &&
			(tr->from_flags & FROM_FLAG_BATCH_SUB) == 0;
}

static inline bool
as_transaction_is_batch_direct(const as_transaction *tr)
{
	// Assumes we're already multi-record.
	return (tr->msg_fields & AS_MSG_FIELD_BIT_DIGEST_RIPE_ARRAY) != 0;
}

static inline bool
as_transaction_is_query(const as_transaction *tr)
{
	// Assumes we're already multi-record.
	return (tr->msg_fields & AS_MSG_FIELD_BIT_INDEX_RANGE) != 0;
}

static inline bool
as_transaction_is_udf(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_UDF_FILENAME) != 0;
}

static inline bool
as_transaction_has_udf_op(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_UDF_OP) != 0;
}

static inline bool
as_transaction_has_scan_options(const as_transaction *tr)
{
	return (tr->msg_fields & AS_MSG_FIELD_BIT_SCAN_OPTIONS) != 0;
}

// For now it's not worth storing the trid in the as_transaction struct since we
// only parse it from the msg once per transaction anyway.
static inline uint64_t
as_transaction_trid(const as_transaction *tr)
{
	if ((tr->msg_fields & AS_MSG_FIELD_BIT_TRID) == 0) {
		return 0;
	}

	as_msg_field *f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_TRID);

	return cf_swap_from_be64(*(uint64_t*)f->data);
}

int as_transaction_init_iudf(as_transaction *tr, as_namespace *ns, cf_digest *keyd);

void as_transaction_demarshal_error(as_transaction* tr, uint32_t error_code);
void as_transaction_error(as_transaction* tr, uint32_t error_code);
