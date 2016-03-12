/*
 * transaction.c
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

/*
 * Operations on transactions
 */

#include "base/transaction.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "fault.h"

#include "base/as_stap.h"
#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/scan.h"
#include "base/security.h"
#include "base/thr_demarshal.h"
#include "base/thr_proxy.h"
#include "base/udf_rw.h"

/* as_transaction_prepare
 * Prepare a transaction that has just been received from the wire.
 * NB: This should only be called once on any given transaction, because it swaps bytes! */

/*
** the layout for the digest is:
** type byte - set
** set's bytes
** type byte - key
** key's bytes
**
** Notice that in the case of the 'key', the payload includes a particle-type byte.
**
** return -2 means no digest no key
** return -3 means batch digest request
** return -4 means bad protocol data  (but no longer used??)
*/

/*
 * Code to init the fields in a transaction.  Use this instead of memset.
 *
 * NB: DO NOT SHUFFLE INIT ORDER .. it is based on elements found in the
 *     structure.
 */
void
as_transaction_init(as_transaction *tr, cf_digest *keyd, cl_msg *msgp)
{
	tr->msgp                      = msgp;
	tr->msg_fields                = 0;

	tr->keyd                      = keyd ? *keyd : cf_digest_zero;

	tr->result_code               = AS_PROTO_RESULT_OK;
	tr->generation                = 0;
	tr->microbenchmark_is_resolve = false;
	tr->flag                      = 0;

	tr->start_time                = 0;
	tr->end_time                  = 0;
	tr->microbenchmark_time       = 0;

	AS_PARTITION_RESERVATION_INIT(tr->rsv);

	tr->proto_fd_h                = 0;
	tr->proxy_node                = 0;
	tr->proxy_msg                 = 0;

	tr->udata                     = 0;

	tr->batch_shared              = 0;
	tr->batch_index               = 0;

	tr->void_time                 = 0;
}

bool
as_transaction_set_msg_field_flag(as_transaction *tr, uint8_t type)
{
	switch (type) {
	case AS_MSG_FIELD_TYPE_NAMESPACE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_NAMESPACE;
		break;
	case AS_MSG_FIELD_TYPE_SET:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SET;
		break;
	case AS_MSG_FIELD_TYPE_KEY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_KEY;
		break;
	case AS_MSG_FIELD_TYPE_DIGEST_RIPE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_DIGEST_RIPE;
		break;
	case AS_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_DIGEST_RIPE_ARRAY;
		break;
	case AS_MSG_FIELD_TYPE_TRID:
		tr->msg_fields |= AS_MSG_FIELD_BIT_TRID;
		break;
	case AS_MSG_FIELD_TYPE_SCAN_OPTIONS:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SCAN_OPTIONS;
		break;
	case AS_MSG_FIELD_TYPE_INDEX_NAME:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_NAME;
		break;
	case AS_MSG_FIELD_TYPE_INDEX_RANGE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_RANGE;
		break;
	case AS_MSG_FIELD_TYPE_INDEX_TYPE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_TYPE;
		break;
	case AS_MSG_FIELD_TYPE_UDF_FILENAME:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_FILENAME;
		break;
	case AS_MSG_FIELD_TYPE_UDF_FUNCTION:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_FUNCTION;
		break;
	case AS_MSG_FIELD_TYPE_UDF_ARGLIST:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_ARGLIST;
		break;
	case AS_MSG_FIELD_TYPE_UDF_OP:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_OP;
		break;
	case AS_MSG_FIELD_TYPE_QUERY_BINLIST:
		tr->msg_fields |= AS_MSG_FIELD_BIT_QUERY_BINLIST;
		break;
	case AS_MSG_FIELD_TYPE_BATCH: // shouldn't get here - batch parent handles this
		tr->msg_fields |= AS_MSG_FIELD_BIT_BATCH;
		break;
	case AS_MSG_FIELD_TYPE_BATCH_WITH_SET: // shouldn't get here - batch parent handles this
		tr->msg_fields |= AS_MSG_FIELD_BIT_BATCH_WITH_SET;
		break;
	default:
		return false;
	}

	return true;
}

// TODO - check m->n_fields against PROTO_NFIELDS_MAX_WARNING?
bool
as_transaction_demarshal_prepare(as_transaction *tr)
{
	uint64_t size = tr->msgp->proto.sz;

	if (size < sizeof(as_msg)) {
		cf_warning(AS_PROTO, "proto body size %lu smaller than as_msg", size);
		return false;
	}

	// The proto data is not smaller than an as_msg - safe to swap header.
	as_msg *m = &tr->msgp->msg;

	as_msg_swap_header(m);

	uint8_t* p_end = (uint8_t*)m + size;
	uint8_t* p_read = m->data;

	// Parse and swap fields first.
	for (uint16_t n = 0; n < m->n_fields; n++) {
		if (p_read + sizeof(as_msg_field) > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg_field");
			return false;
		}

		as_msg_field* p_field = (as_msg_field*)p_read;

		as_msg_swap_field(p_field);
		p_read = as_msg_field_skip(p_field);

		if (! p_read) {
			cf_warning(AS_PROTO, "bad as_msg_field");
			return false;
		}

		if (p_read > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg_field value");
			return false;
		}

		// Store which message fields are present - prevents lots of re-parsing.
		if (! as_transaction_set_msg_field_flag(tr, p_field->type)) {
			cf_debug(AS_PROTO, "skipping as_msg_field type %u", p_field->type);
		}
	}

	// Parse and swap bin-ops, if any.
	for (uint16_t n = 0; n < m->n_ops; n++) {
		if (p_read + sizeof(as_msg_op) > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg_op");
			return false;
		}

		as_msg_op* op = (as_msg_op*)p_read;

		as_msg_swap_op(op);
		p_read = as_msg_op_skip(op);

		if (! p_read) {
			cf_warning(AS_PROTO, "bad as_msg_op");
			return false;
		}

		if (p_read > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg_op data");
			return false;
		}
	}

	if (p_read != p_end) {
		cf_warning(AS_PROTO, "extra bytes follow fields and bin-ops");
		return false;
	}

	return true;
}

void
as_transaction_proxyee_prepare(as_transaction *tr)
{
	as_msg *m = &tr->msgp->msg;
	as_msg_field* p_field = (as_msg_field*)m->data;

	// Store which message fields are present - prevents lots of re-parsing.
	// Proto header, field sizes already swapped to host order by proxyer.
	for (uint16_t n = 0; n < m->n_fields; n++) {
		if (! as_transaction_set_msg_field_flag(tr, p_field->type)) {
			cf_debug(AS_PROTO, "skipping as_msg_field type %u", p_field->type);
		}

		p_field = as_msg_field_get_next(p_field);
	}
}

/* Create an internal transaction.
 * parameters:
 *     tr_create_data : Details for creating transaction
 *     tr             : to be filled
 *
 * return :  0  on success
 *           -1 on failure
 */
int
as_transaction_create_internal(as_transaction *tr, tr_create_data *  trc_data)
{
	tr_create_data * d = (tr_create_data*) trc_data;

	// Get Defensive
	memset(tr, 0, sizeof(as_transaction));

	if (d->fd_h) {
		cf_warning(AS_PROTO, "Foreground Internal Transation .. Ignoring");
		return -1;
	} else {
		tr->proto_fd_h = NULL;
	}

	tr->result_code  = AS_PROTO_RESULT_OK;

	AS_PARTITION_RESERVATION_INIT(tr->rsv);

	tr->udata = NULL;

	uint32_t n_fields = 2; // namespace and digest always added

	as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_NAMESPACE);
	as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_DIGEST_RIPE);

	// Get namespace and set lengths.
	int ns_len        = strlen(d->ns->name);
	int set_len       = strlen(d->set);

	// Figure out the size of the message.
	size_t  msg_sz = sizeof(cl_msg);

	msg_sz += sizeof(as_msg_field) + ns_len;

	if (set_len != 0) {
		msg_sz += sizeof(as_msg_field) + set_len;
		as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_SET);
		n_fields++;
	}

	msg_sz += sizeof(as_msg_field) + sizeof(cf_digest);
	msg_sz += sizeof(as_msg_field) + sizeof(d->trid);

	// Udf call structure will go as a part of the transaction udata.
	// Do not pack it in the message.
	cf_debug(AS_PROTO, "UDF : Msg size for internal transaction is %d", msg_sz);

	// Allocate space in the buffer.
	uint8_t * buf   = cf_malloc(msg_sz); memset(buf, 0, msg_sz);
	if (!buf) {
		return -1;
	}
	uint8_t * buf_r = buf;

	// Calculation of number of fields:
	// n_fields = ( ns ? 1 : 0 ) + (set ? 1 : 0) + (digest ? 1 : 0) + (trid ? 1 : 0) + (call ? 3 : 0);

	// Write the header in case the request gets proxied. Is it enough ??
	buf = as_msg_write_header(buf, msg_sz, 0, d->msg_type, 0, 0, 0, 0, n_fields, 0);
	buf = as_msg_write_fields(buf, d->ns->name, ns_len, d->set, set_len, &(d->digest), 0, 0, 0, 0);

	tr->msgp         = (cl_msg *) buf_r;

	// Do this last, to exclude the setup time in this function.
	tr->start_time   = cf_getns();
	MICROBENCHMARK_SET_TO_START_P();

	return 0;
}

void
as_transaction_demarshal_error(as_transaction* tr, uint32_t error_code)
{
	as_msg_send_reply(tr->proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, NULL, 0, NULL);
	tr->proto_fd_h = NULL;

	cf_free(tr->msgp);
	tr->msgp = NULL;
}

void
as_transaction_error(as_transaction* tr, uint32_t error_code)
{
	if (tr->proto_fd_h) {
		if (tr->batch_shared) {
			as_batch_add_error(tr->batch_shared, tr->batch_index, error_code);
			// Clear this transaction's msgp so calling code does not free it.
			tr->msgp = 0;
		}
		else {
			as_msg_send_reply(tr->proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, NULL, as_transaction_trid(tr), NULL);
			tr->proto_fd_h = 0;
			MICROBENCHMARK_HIST_INSERT_P(error_hist);
			cf_atomic_int_incr(&g_config.err_tsvc_requests);
			if (error_code == AS_PROTO_RESULT_FAIL_TIMEOUT) {
				cf_atomic_int_incr(&g_config.err_tsvc_requests_timeout);
			}
		}
	}
	else if (tr->proxy_msg) {
		as_proxy_send_response(tr->proxy_node, tr->proxy_msg, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr), NULL);
		tr->proxy_msg = NULL;
	}
	else if (tr->udata) {
		tr->udata->req_cb(tr, error_code);
		tr->udata = NULL;
	}
}

// Helper to release transaction file handles.
void
as_release_file_handle(as_file_handle *proto_fd_h)
{
	int rc = cf_rc_release(proto_fd_h);

	if (rc > 0) {
		return;
	}
	else if (rc < 0) {
		cf_warning(AS_PROTO, "release file handle: negative ref-count %d", rc);
		return;
	}

	close(proto_fd_h->fd);
	proto_fd_h->fh_info &= ~FH_INFO_DONOT_REAP;
	proto_fd_h->fd = -1;

	if (proto_fd_h->proto)	{
		as_proto *p = proto_fd_h->proto;

		if ((p->version != PROTO_VERSION) || (p->type >= PROTO_TYPE_MAX)) {
			cf_warning(AS_PROTO, "release file handle: bad proto buf, corruption");
		}
		else {
			cf_free(proto_fd_h->proto);
			proto_fd_h->proto = NULL;
		}
	}

	if (proto_fd_h->security_filter) {
		as_security_filter_destroy(proto_fd_h->security_filter);
		proto_fd_h->security_filter = NULL;
	}

	cf_rc_free(proto_fd_h);
	cf_atomic_int_incr(&g_config.proto_connections_closed);
}

void
as_end_of_transaction(as_file_handle *proto_fd_h, bool force_close)
{
	thr_demarshal_resume(proto_fd_h);

	if (force_close) {
		shutdown(proto_fd_h->fd, SHUT_RDWR);
	}

	as_release_file_handle(proto_fd_h);
}

void
as_end_of_transaction_ok(as_file_handle *proto_fd_h)
{
	as_end_of_transaction(proto_fd_h, false);
}

void
as_end_of_transaction_force_close(as_file_handle *proto_fd_h)
{
	as_end_of_transaction(proto_fd_h, true);
}
