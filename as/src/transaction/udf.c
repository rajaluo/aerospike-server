/*
 * udf.c
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include "transaction/udf.h"
#include "base/udf_rw.h" // TODO - subsume

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/thr_proxy.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward Declarations.
//

bool start_udf_dup_res(rw_request* rw, as_transaction* tr);
bool start_udf_repl_write(rw_request* rw, as_transaction* tr);
bool udf_dup_res_cb(rw_request* rw);
bool udf_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
void udf_repl_write_cb(rw_request* rw);

void send_udf_response(as_transaction* tr, cf_dyn_buf* db);
void udf_timeout_cb(rw_request* rw);

transaction_status udf_master(rw_request* rw, as_transaction* tr);

static inline void
client_udf_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_client_udf_complete);
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_udf_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_udf_error);
		break;
	}
}

static inline void
udf_sub_udf_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_udf_sub_udf_complete);
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_udf_sub_udf_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_udf_sub_udf_error);
		break;
	}
}


//==========================================================
// Public API.
//

transaction_status
as_udf_start(as_transaction* tr)
{
	BENCHMARK_START(tr, udf, FROM_CLIENT);
	BENCHMARK_START(tr, udf_sub, FROM_IUDF);

	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_FORBIDDEN;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Don't know if UDF is read or delete - check that we aren't backed up.
	if (as_storage_overloaded(tr->rsv.ns)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_DEVICE_OVERLOAD;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->id, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);
	transaction_status status = rw_request_hash_insert(&hkey, rw, tr);

	// If rw_request wasn't inserted in hash, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_udf_response(tr, NULL);
		}

		return status;
	}
	// else - rw_request is now in hash, continue...

	if (g_config.write_duplicate_resolution_disable) {
		// Note - preventing duplicate resolution this way allows
		// rw_request_destroy() to handle dup_msg[] cleanup correctly.
		tr->rsv.n_dupl = 0;
	}

	// If there are duplicates to resolve, start doing so.
	if (tr->rsv.n_dupl != 0) {
		if (! start_udf_dup_res(rw, tr)) {
			rw_request_hash_delete(&hkey);
			tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
			send_udf_response(tr, NULL);
			return TRANS_DONE_ERROR;
		}

		// Started duplicate resolution.
		return TRANS_IN_PROGRESS;
	}
	// else - no duplicate resolution phase, apply operation to master.

	status = udf_master(rw, tr);

	// TODO - really need to refactor so response isn't sent in udf_master()!
	if (status == TRANS_DONE_SUCCESS) {
		// UDF was a read, has already responded to origin.
		rw_request_hash_delete(&hkey);
		return status;
	}

	BENCHMARK_NEXT_DATA_POINT(tr, udf, master);
	BENCHMARK_NEXT_DATA_POINT(tr, udf_sub, master);

	if (status == TRANS_DONE_ERROR) {
		rw_request_hash_delete(&hkey);
		send_udf_response(tr, NULL);
		return status;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_getreplica_readall(tr->rsv.ns, tr->rsv.pid,
			rw->dest_nodes);

	// If we don't need replica writes, transaction is finished.
	// TODO - consider a single-node fast path bypassing hash and pickling?
	if (rw->n_dest_nodes == 0) {
		send_udf_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey);
		return TRANS_DONE_SUCCESS;
	}

	if (! start_udf_repl_write(rw, tr)) {
		rw_request_hash_delete(&hkey);
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		send_udf_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction flow.
//

bool
start_udf_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send dup-res message.

	if (! dup_res_make_message(rw, tr, true)) {
		return false;
	}

	rw->respond_client_on_master_completion = respond_on_master_complete(tr);

	pthread_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, udf_dup_res_cb, udf_timeout_cb);
	send_rw_messages(rw);

	pthread_mutex_unlock(&rw->lock);

	return true;
}


bool
start_udf_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	if (! repl_write_make_message(rw, tr)) { // TODO - split this?
		return false;
	}

	rw->respond_client_on_master_completion = respond_on_master_complete(tr);

	if (rw->respond_client_on_master_completion) {
		// Don't wait for replication. When replication is complete, we will
		// call send_udf_response() again, but it will no-op quietly.
		send_udf_response(tr, &rw->response_db);
	}

	pthread_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, udf_repl_write_cb, udf_timeout_cb);
	send_rw_messages(rw);

	pthread_mutex_unlock(&rw->lock);

	return true;
}


bool
udf_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT(rw, udf, dup_res);
	BENCHMARK_NEXT_DATA_POINT(rw, udf_sub, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	transaction_status status = udf_master(rw, &tr);

	// TODO - really need to refactor so response isn't sent in udf_master()!
	if (status == TRANS_DONE_SUCCESS) {
		// UDF was a read, has already responded to origin.
		return true;
	}

	BENCHMARK_NEXT_DATA_POINT((&tr), udf, master);
	BENCHMARK_NEXT_DATA_POINT((&tr), udf_sub, master);

	if (status == TRANS_DONE_ERROR) {
		send_udf_response(&tr, &rw->response_db);
		return true;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_getreplica_readall(tr.rsv.ns, tr.rsv.pid,
			rw->dest_nodes);

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		send_udf_response(&tr, &rw->response_db);
		return true;
	}

	if (! udf_repl_write_after_dup_res(rw, &tr)) {
		tr.result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		send_udf_response(&tr, NULL);
		return true;
	}

	// Started replica write - don't delete rw_request from hash.
	return false;
}


bool
udf_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	if (! repl_write_make_message(rw, tr)) { // TODO - split this?
		return false;
	}

	if (rw->respond_client_on_master_completion) {
		// Don't wait for replication. When replication is complete, we will
		// call send_udf_response() again, but it will no-op quietly.
		send_udf_response(tr, &rw->response_db);
	}

	repl_write_reset_rw(rw, tr, udf_repl_write_cb);
	send_rw_messages(rw);

	return true;
}


void
udf_repl_write_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT(rw, udf, repl_write);
	BENCHMARK_NEXT_DATA_POINT(rw, udf_sub, repl_write);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	send_udf_response(&tr, &rw->response_db);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

void
send_udf_response(as_transaction* tr, cf_dyn_buf* db)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	switch (tr->origin) {
	case FROM_CLIENT:
		if (db && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db);
		}
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, NULL, NULL, 0, NULL,
					as_transaction_trid(tr), NULL);
		}
		BENCHMARK_NEXT_DATA_POINT(tr, udf, response);
		HIST_TRACK_ACTIVATE_INSERT_DATA_POINT(tr, udf_hist);
		client_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_PROXY:
		if (db && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, NULL, NULL,
					0, NULL, as_transaction_trid(tr), NULL);
		}
		break;
	case FROM_IUDF:
		if (db && db->used_sz != 0) {
			cf_crash(AS_RW, "unexpected - internal udf has response");
		}
		tr->from.iudf_orig->cb(tr->from.iudf_orig->udata, tr->result_code);
		// TODO - is it worth it?
		BENCHMARK_NEXT_DATA_POINT(tr, udf_sub, response);
		udf_sub_udf_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_BATCH:
	case FROM_NSUP:
		// Should be impossible for batch reads and nsup deletes to get here.
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // inform timeout it lost the race
}


void
udf_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	switch (rw->origin) {
	case FROM_CLIENT:
		as_end_of_transaction_force_close(rw->from.proto_fd_h);
//		HIST_TRACK_ACTIVATE_INSERT_DATA_POINT(rw, udf_hist);
		client_udf_update_stats(rw->rsv.ns, AS_PROTO_RESULT_FAIL_TIMEOUT);
		break;
	case FROM_PROXY:
		break;
	case FROM_IUDF:
		rw->from.iudf_orig->cb(rw->from.iudf_orig->udata,
				AS_PROTO_RESULT_FAIL_TIMEOUT);
		// TODO - histograms?
		udf_sub_udf_update_stats(rw->rsv.ns, AS_PROTO_RESULT_FAIL_TIMEOUT);
		break;
	case FROM_BATCH:
	case FROM_NSUP:
		// Should be impossible for batch reads and nsup deletes to get here.
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	// Paranoia - shouldn't need this to inform other callback it lost race.
	rw->from.any = NULL;
}


//==========================================================
// Local helpers - UDF.
//

transaction_status
udf_master(rw_request* rw, as_transaction* tr)
{
	rw->has_udf = true;

	udf_def def;
	udf_call stack_call = { &def, tr };
	udf_call* call = tr->origin == FROM_IUDF ?
			udf_rw_call_def_init_internal(&stack_call, tr) :
			udf_rw_call_def_init_from_msg(&stack_call, tr);

	if (! call) {
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		return TRANS_DONE_ERROR;
	}

	udf_optype op = UDF_OPTYPE_NONE;

	udf_rw_local(call, rw, &op);
	udf_rw_call_destroy(call);

	if (UDF_OP_IS_READ(op) || op == UDF_OPTYPE_NONE) {
		if (tr->origin == FROM_IUDF) {
			// For internal UDFs, we skipped sending a response, so some work
			// normally handled by the response method needs to happen here.
			BENCHMARK_NEXT_DATA_POINT(tr, udf_sub, master);

			tr->from.iudf_orig->cb(tr->from.iudf_orig->udata, tr->result_code);
			tr->from.iudf_orig = NULL;

			// TODO - is it worth it?
			BENCHMARK_NEXT_DATA_POINT(tr, udf_sub, response);
			cf_atomic64_incr(&tr->rsv.ns->n_udf_sub_udf_complete);
		}

		// UDF is done, has responded to origin, no replica writes needed.
		return TRANS_DONE_SUCCESS;
	}

	if (UDF_OP_IS_LDT(op)) {
		rw->is_multiop = true;
	}

	// UDFs send original msg for replica deletes.
	// Note - not currently necessary to set this message flag.
	if (UDF_OP_IS_DELETE(op)) {
		tr->msgp->msg.info2 |= AS_MSG_INFO2_DELETE;
	}

	return TRANS_IN_PROGRESS;
}
