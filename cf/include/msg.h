/*
 * msg.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

/*
 *
 * An independent message parser.
 * This forms a generic way of parsing messages.
 *
 * A 'msg_desc' is a message descriptor. This will commonly be a singleton created
 * by a unit once.
 * A 'msg' is a parsed-up and easy to read version of the message.
 */

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_types.h>
#include "dynbuf.h"

// NOTE: These values are actually used on the wire right now!
typedef enum msg_field_type_t {
	M_FT_INT32 = 1,
	M_FT_UINT32 = 2,
	M_FT_INT64 = 3,
	M_FT_UINT64 = 4,
	M_FT_STR = 5,
	M_FT_BUF = 6,
	M_FT_ARRAY_UINT32 = 7,
	M_FT_ARRAY_UINT64 = 8,
	M_FT_ARRAY_BUF = 9,
	M_FT_ARRAY_STR = 10
} msg_field_type;

// This is somewhat of a helper, since we never look at the values - they're for
// the caller to use. It's only important that the maximum value is respected.
typedef enum msg_type_t {
	M_TYPE_FABRIC = 0,		// fabric's internal msg
	M_TYPE_HEARTBEAT = 1,
	M_TYPE_PAXOS = 2,		// paxos' msg
	M_TYPE_MIGRATE = 3,
	M_TYPE_PROXY = 4,
	M_TYPE_TEST = 5,
	M_TYPE_WRITE = 6,
	M_TYPE_RW = 7,
	M_TYPE_INFO = 8,
	M_TYPE_SCAN = 9,
	M_TYPE_BATCH = 10,
	M_TYPE_XDR = 11,
	M_TYPE_FB_HEALTH_PING = 12, // deprecated
	M_TYPE_FB_HEALTH_ACK = 13,  // deprecated
	M_TYPE_TSCAN = 14,
	M_TYPE_SMD = 15,		// System MetaData msg
	M_TYPE_MULTIOP = 16,	// multiple op
	M_TYPE_SINDEX = 17,		// secondary index op
	M_TYPE_MAX = 18			// highest + 1 is correct
} msg_type;

typedef struct msg_field_template_t {
	int				id;
	msg_field_type	type;
} msg_field_template;

// TODO: consider that a msg_desc should have a human readable string
// Can play other interesting macro games to make sure we don't have
// the problem of needing to add a length field to the message descriptions

typedef msg_field_template msg_template;

//
// Types for string and buffer arrays.
//

typedef struct msg_str_array_s {
	uint32_t	alloc_size;	// number of bytes allocated
	uint32_t	used_size;	// bytes used in the string array
	uint32_t	len;		// number of string offsets
	uint32_t	offset[];	// array of pointers to the strings
} msg_str_array;

typedef struct msg_pbuf_s {
	uint32_t	len;
	uint8_t		data[];
} msg_pbuf;

typedef struct msg_buf_array_s {
	uint32_t	alloc_size;	// number of bytes allocated
	uint32_t	used_size;	// bytes used in the buffer array
	uint32_t	len;		// number of string offsets
	uint32_t	offset[];	// array of pointers to the buffers
} msg_buf_array;

// This is a very simple linear system for representing a message. Insert/read
// efficiency is paramount, but we think messages will tend to be compact
// without a lot of holes. If we expected sparse representation, we'd use a data
// structure better at sparse stuff.

typedef struct msg_field_t {
	int		 		id;
	msg_field_type	type;
	uint32_t 		field_len;
	bool			is_valid;
	bool			is_set;
	bool 			free;
	union {
		uint32_t		ui32;
		int32_t			i32;
		uint64_t		ui64;
		int64_t			i64;
		char 			*str;
		uint8_t			*buf;
		uint32_t		*ui32_a;
		uint64_t		*ui64_a;
		msg_str_array	*str_a;
		msg_buf_array	*buf_a;
		void			*any_buf;
	} u;
} msg_field;

typedef struct msg_t {
	uint32_t			n_fields; // number of msg_field's allocated
	uint32_t			bytes_used;
	uint32_t			bytes_alloc;
	bool				just_parsed; // fields point into fabric buffer
	msg_type			type;
	const msg_template	*mt;
	msg_field			f[];
} msg;

//
// "msg" Object Accounting:
//
//   Total number of "msg" objects allocated:
extern cf_atomic_int g_num_msgs;
//   Total number of "msg" objects allocated per type:
extern cf_atomic_int g_num_msgs_by_type[M_TYPE_MAX];

// Limit the maximum number of "msg" objects per type (-1 means unlimited.)
void msg_set_max_msgs_per_type(int64_t max_msgs);

//
// msg_create - Initialize an empty message. You can pass in a stack buff, too.
// If everything fits, it stays. We use the msg_desc as a hint. Slightly
// unusually, the 'md_sz' field is in bytes. This is a good shortcut to avoid
// terminator fields or anything similar.
int msg_create(msg **m, msg_type type, const msg_template *mt, size_t mt_sz);

// msg_parse - Parse a buffer into a message, which thus can be accessed.
int msg_parse(msg *m, const uint8_t *buf, const size_t buflen);

// If you've received a little bit of a buffer, grab the size header and type.
// return = -2 means "not enough to tell yet"
int msg_get_initial(uint32_t *size, msg_type *type, const uint8_t *buf, uint32_t buflen);

// msg_tobuf - Parse a message out into a buffer.
int msg_fillbuf(const msg *m, uint8_t *buf, size_t *buflen);

// msg_reset - After a message has been parsed, and the information consumed,
// reset all the internal pointers for another parse.
void msg_reset(msg *m);

void msg_preserve_fields(msg *m, uint32_t n_field_ids, ...);
void msg_preserve_all_fields(msg *m);

// Messages are reference counted. If you need to take a reference, call this
// function. Everyone calls destroy.
void msg_incr_ref(msg *m);
void msg_decr_ref(msg *m); // this isn't used much, but helps when unwinding errors


// Getters and setters.

typedef enum { MSG_GET_DIRECT, MSG_GET_COPY_MALLOC } msg_get_type;
typedef enum { MSG_SET_HANDOFF_MALLOC, MSG_SET_COPY } msg_set_type;

// Note! get_str and get_buf should probably be more complicated. To wit: both a
// 'copy' and 'dup' interface (where the 'copy' interface would return the
// length regardless, thus is also a 'getlet' method.

// Note about 'get_buf' and 'get_bytearray'. These both operate on 'buf' type
// fields. The 'buf' calls, however, will either consume your pointer (and not
// free it later, does this still make sense?) or will take a copy of the data.
// The cf_bytearray version of 'get' will malloc you up a new cf_bytearray that
// you can take away for yourself. The set_bytearray will do the same thing,
// take your cf_bytearray and free it later when the message is destroyed.

int msg_get_uint32(const msg *m, int field_id, uint32_t *r);
int msg_get_int32(const msg *m, int field_id, int32_t *r);
int msg_get_uint64(const msg *m, int field_id, uint64_t *r);
int msg_get_int64(const msg *m, int field_id, int64_t *r);
int msg_get_str(const msg *m, int field_id, char **r, size_t *len, msg_get_type type); // len returns strlen+1, the allocated size
int msg_get_buf(const msg *m, int field_id, uint8_t **r, size_t *len, msg_get_type type);

int msg_set_uint32(msg *m, int field_id, uint32_t v);
int msg_set_int32(msg *m, int field_id, int32_t v);
int msg_set_uint64(msg *m, int field_id, uint64_t v);
int msg_set_int64(msg *m, int field_id, int64_t v);
int msg_set_str(msg *m, int field_id, const char *v, msg_set_type type);
int msg_set_buf(msg *m, int field_id, const uint8_t *v, size_t len, msg_set_type type);

// Array functions. I'm not sure yet the best metaphore for these, trying a few things out.
int msg_get_uint32_array(msg *m, int field_id, int index, uint32_t *r);
int msg_get_uint64_array_size(msg *m, int field_id, int *size);
int msg_get_uint64_array(msg *m, int field_id, int index, uint64_t *r);
int msg_get_str_array(msg *m, int field_id, int index, char **r, size_t *len, msg_get_type type); // len returns strlen+1, the allocated size
int msg_get_buf_array_size(const msg *m, int field_id, int *size);
int msg_get_buf_array(const msg *m, int field_id, int index, uint8_t **r, size_t *len, msg_get_type type);

int msg_set_uint32_array_size(msg *m, int field_id, int size);
int msg_set_uint32_array(msg *m, int field_id, int index, uint32_t v);
int msg_set_uint64_array_size(msg *m, int field_id, int size);
int msg_set_uint64_array(msg *m, int field_id, int index, uint64_t v);
int msg_set_str_array_size(msg *m, int field_id, int size, int total_len);
int msg_set_str_array(msg *m, int field_id, int index, const char *v);
int msg_set_buf_array_size(msg *m, int field_id, int size, int elem_size);
int msg_set_buf_array(msg *m, int field_id, int index, const uint8_t *v, size_t len);

// Free up a "msg" object. Call this function instead of freeing the msg
// directly in order to keep track of all msgs.
void msg_put(msg *m);

// And, finally, the destruction of a message.
void msg_destroy(msg *m);

// A debug function for finding out what's in a message.
void msg_dump(const msg *m, const char *info);


// TODO - move when merged to new dev branch.
uint32_t msg_get_wire_size(const msg *m);
