/*
 * write.c
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

#include "transaction/write.h"

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
#include "base/index.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "base/xdr_serverside.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Typedefs & constants.
//

#define STACK_PARTICLES_SIZE (1024 * 1024)

typedef struct index_metadata_s {
	uint32_t void_time;
	uint64_t last_update_time;
	uint16_t generation;
} index_metadata;


//==========================================================
// Forward Declarations.
//

bool start_write_dup_res(rw_request* rw, as_transaction* tr);
bool start_write_repl_write(rw_request* rw, as_transaction* tr);
bool write_dup_res_cb(rw_request* rw);
bool write_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
void write_repl_write_cb(rw_request* rw);

void send_write_response(as_transaction* tr, cf_dyn_buf* db);
void write_timeout_cb(rw_request* rw);

transaction_status write_master(rw_request* rw, as_transaction* tr);
void write_master_failed(as_transaction* tr, as_index_ref* r_ref,
		bool record_created, as_index_tree* tree, as_storage_rd* rd,
		int result_code);
int write_master_preprocessing(as_transaction* tr);
int write_master_policies(as_transaction* tr, bool* p_must_not_create,
		bool* p_record_level_replace, bool* p_must_fetch_data,
		bool* p_increment_generation);
bool check_msg_set_name(as_transaction* tr, const char* set_name);

int write_master_dim_single_bin(as_transaction* tr, as_storage_rd* rd,
		bool record_created, bool increment_generation, rw_request* rw,
		bool* is_delete, xdr_dirty_bins* dirty_bins);
int write_master_dim(as_transaction* tr, const char* set_name,
		as_storage_rd* rd, bool record_level_replace, bool increment_generation,
		rw_request* rw, bool* is_delete, xdr_dirty_bins* dirty_bins);
int write_master_ssd_single_bin(as_transaction* tr, as_storage_rd* rd,
		bool must_fetch_data, bool increment_generation, rw_request* rw,
		bool* is_delete, xdr_dirty_bins* dirty_bins);
int write_master_ssd(as_transaction* tr, const char* set_name,
		as_storage_rd* rd, bool must_fetch_data, bool record_level_replace,
		bool increment_generation, rw_request* rw, bool* is_delete,
		xdr_dirty_bins* dirty_bins);

void write_master_update_index_metadata(as_transaction* tr,
		bool increment_generation, index_metadata* old, as_record* r);
int write_master_bin_ops(as_transaction* tr, as_storage_rd* rd,
		cf_ll_buf* particles_llb, as_bin* cleanup_bins,
		uint32_t* p_n_cleanup_bins, cf_dyn_buf* db, uint32_t* p_n_final_bins,
		xdr_dirty_bins* dirty_bins);
int write_master_bin_ops_loop(as_transaction* tr, as_storage_rd* rd,
		as_msg_op** ops, as_bin* response_bins, uint32_t* p_n_response_bins,
		as_bin* result_bins, uint32_t* p_n_result_bins,
		cf_ll_buf* particles_llb, as_bin* cleanup_bins,
		uint32_t* p_n_cleanup_bins, xdr_dirty_bins* dirty_bins);
int write_master_bin_check(as_transaction* tr, as_bin* bin);
bool write_master_sindex_update(as_namespace* ns, const char* set_name,
		cf_digest* keyd, as_bin* old_bins, uint32_t n_old_bins,
		as_bin* new_bins, uint32_t n_new_bins);

void write_master_index_metadata_unwind(index_metadata* old, as_record* r);
void write_master_dim_single_bin_unwind(as_bin* old_bin, as_bin* new_bin,
		as_bin* cleanup_bins, uint32_t n_cleanup_bins);
void write_master_dim_unwind(as_bin* old_bins, uint32_t n_old_bins,
		as_bin* new_bins, uint32_t n_new_bins, as_bin* cleanup_bins,
		uint32_t n_cleanup_bins);

static inline void
client_write_update_stats(as_namespace* ns, uint8_t result_code, bool is_xdr_op)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_client_write_success);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_write_success);
		}
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_write_timeout);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_write_timeout);
		}
		break;
	default:
		cf_atomic64_incr(&ns->n_client_write_error);
		if (is_xdr_op) {
			cf_atomic64_incr(&ns->n_xdr_write_error);
		}
		break;
	}
}

static inline void
append_bin_to_destroy(as_bin* b, as_bin* bins, uint32_t* p_n_bins)
{
	if (as_bin_is_external_particle(b)) {
		bins[(*p_n_bins)++] = *b;
	}
}


//==========================================================
// Public API.
//

transaction_status
as_write_start(as_transaction* tr)
{
	BENCHMARK_START(tr, write, FROM_CLIENT);

	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_FORBIDDEN;
		send_write_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Check that we aren't backed up.
	if (as_storage_overloaded(tr->rsv.ns)) {
		tr->result_code = AS_PROTO_RESULT_FAIL_DEVICE_OVERLOAD;
		send_write_response(tr, NULL);
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
			send_write_response(tr, NULL);
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
		if (! start_write_dup_res(rw, tr)) {
			rw_request_hash_delete(&hkey, rw);
			tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
			send_write_response(tr, NULL);
			return TRANS_DONE_ERROR;
		}

		// Started duplicate resolution.
		return TRANS_IN_PROGRESS;
	}
	// else - no duplicate resolution phase, apply operation to master.

	status = write_master(rw, tr);

	BENCHMARK_NEXT_DATA_POINT(tr, write, master);

	// If error, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_hash_delete(&hkey, rw);
		send_write_response(tr, NULL);
		return status;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr->rsv.p,
			rw->dest_nodes);

	// If we don't need replica writes, transaction is finished.
	// TODO - consider a single-node fast path bypassing hash and pickling?
	if (rw->n_dest_nodes == 0) {
		clear_delete_response_metadata(rw, tr);
		send_write_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE_SUCCESS;
	}

	if (! start_write_repl_write(rw, tr)) {
		rw_request_hash_delete(&hkey, rw);
		tr->result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		send_write_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction flow.
//

bool
start_write_dup_res(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send dup-res message.

	if (! dup_res_make_message(rw, tr)) {
		return false;
	}

	rw->respond_client_on_master_completion = respond_on_master_complete(tr);

	pthread_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, write_dup_res_cb, write_timeout_cb);
	send_rw_messages(rw);

	pthread_mutex_unlock(&rw->lock);

	return true;
}


bool
start_write_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	if (! repl_write_make_message(rw, tr)) {
		return false;
	}

	rw->respond_client_on_master_completion = respond_on_master_complete(tr);

	if (rw->respond_client_on_master_completion) {
		// Don't wait for replication. When replication is complete, we won't
		// call send_write_response() again.
		send_write_response(tr, &rw->response_db);
	}

	pthread_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, write_repl_write_cb, write_timeout_cb);
	send_rw_messages(rw);

	pthread_mutex_unlock(&rw->lock);

	return true;
}


bool
write_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT(rw, write, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	transaction_status status = write_master(rw, &tr);

	BENCHMARK_NEXT_DATA_POINT((&tr), write, master);

	if (status == TRANS_DONE_ERROR) {
		send_write_response(&tr, NULL);
		return true;
	}

	// Set up the nodes to which we'll write replicas.
	rw->n_dest_nodes = as_partition_get_other_replicas(tr.rsv.p,
			rw->dest_nodes);

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		clear_delete_response_metadata(rw, &tr);
		send_write_response(&tr, &rw->response_db);
		return true;
	}

	if (! write_repl_write_after_dup_res(rw, &tr)) {
		tr.result_code = AS_PROTO_RESULT_FAIL_UNKNOWN;
		send_write_response(&tr, NULL);
		return true;
	}

	// Started replica write - don't delete rw_request from hash.
	return false;
}


bool
write_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	if (! repl_write_make_message(rw, tr)) {
		return false;
	}

	if (rw->respond_client_on_master_completion) {
		// Don't wait for replication. When replication is complete, we won't
		// call send_write_response() again.
		send_write_response(tr, &rw->response_db);
	}

	repl_write_reset_rw(rw, tr, write_repl_write_cb);
	send_rw_messages(rw);

	return true;
}


void
write_repl_write_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT(rw, write, repl_write);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	send_write_response(&tr, &rw->response_db);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

void
send_write_response(as_transaction* tr, cf_dyn_buf* db)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

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
		BENCHMARK_NEXT_DATA_POINT(tr, write, response);
		HIST_TRACK_ACTIVATE_INSERT_DATA_POINT(tr, write_hist);
		client_write_update_stats(tr->rsv.ns, tr->result_code,
				as_transaction_is_xdr(tr));
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
	case FROM_BATCH:
	case FROM_IUDF:
	case FROM_NSUP:
		// Should be impossible for batch reads, internal UDFs, and nsup deletes
		// to get here.
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // needed only for respond-on-master-complete
}


void
write_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_PROTO_RESULT_FAIL_TIMEOUT, 0,
				0, NULL, NULL, 0, NULL, rw_request_trid(rw), NULL);
		// Timeouts aren't included in histograms.
		// Note - rw->msgp can be null if it's a ship-op.
		client_write_update_stats(rw->rsv.ns, AS_PROTO_RESULT_FAIL_TIMEOUT,
				rw->msgp ? as_msg_is_xdr(&rw->msgp->msg) : false);
		break;
	case FROM_PROXY:
		break;
	case FROM_BATCH:
	case FROM_IUDF:
	case FROM_NSUP:
		// Should be impossible for batch reads, internal UDFs, and nsup deletes
		// to get here.
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - write master.
//

transaction_status
write_master(rw_request* rw, as_transaction* tr)
{
	CF_ALLOC_SET_NS_ARENA(tr->rsv.ns);

	//------------------------------------------------------
	// Perform checks that don't need to loop over ops, or
	// create or find (and lock) the as_index.
	//

	if (! write_master_preprocessing(tr)) {
		// Failure cases all call write_master_failed().
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Loop over ops to set some essential policy flags.
	//

	bool must_not_create;
	bool record_level_replace;
	bool must_fetch_data;
	bool increment_generation;

	int result = write_master_policies(tr, &must_not_create,
			&record_level_replace, &must_fetch_data, &increment_generation);

	if (result != 0) {
		write_master_failed(tr, 0, false, 0, 0, result);
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Find or create the as_index and get a reference -
	// this locks the record. Perform all checks that don't
	// need the as_storage_rd.
	//

	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_index_tree* tree = tr->rsv.tree; // sub-records don't use write_master()

	// Find or create as_index, populate as_index_ref, lock record.
	as_index_ref r_ref;
	r_ref.skip_lock = false;
	as_record* r = NULL;
	bool record_created = false;

	if (must_not_create) {
		if (0 != as_record_get_live(tree, &tr->keyd, &r_ref, ns)) {
			write_master_failed(tr, 0, record_created, tree, 0, AS_PROTO_RESULT_FAIL_NOTFOUND);
			return TRANS_DONE_ERROR;
		}

		r = r_ref.r;

		if (as_record_is_doomed(r, ns)) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_NOTFOUND);
			return TRANS_DONE_ERROR;
		}
	}
	else {
		int rv = as_record_get_create(tree, &tr->keyd, &r_ref, ns, false);

		if (rv < 0) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: fail as_record_get_create() ", ns->name);
			write_master_failed(tr, 0, record_created, tree, 0, AS_PROTO_RESULT_FAIL_UNKNOWN);
			return TRANS_DONE_ERROR;
		}

		r = r_ref.r;
		record_created = rv == 1;

		// If it's an expired or truncated record, pretend it's a fresh create.
		if (! record_created && as_record_is_doomed(r, ns)) {
			as_record_rescue(&r_ref, ns);
			record_created = true;
		}
	}

	// Enforce record-level create-only existence policy.
	if (! record_created && ! create_only_check(r, m)) {
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_RECORD_EXISTS);
		return TRANS_DONE_ERROR;
	}

	// Check generation requirement, if any.
	if (! generation_check(r, m)) {
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_GENERATION);
		return TRANS_DONE_ERROR;
	}

	// If creating record, write set-ID into index.
	if (record_created) {
		int rv_set = as_transaction_has_set(tr) ?
				set_set_from_msg(r, ns, m) : 0;

		if (rv_set == -1) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: set can't be added ", ns->name);
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
			return TRANS_DONE_ERROR;
		}
		else if (rv_set == -2) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_FORBIDDEN);
			return TRANS_DONE_ERROR;
		}

		// Don't write record if it would be truncated.
		if (as_truncate_now_is_truncated(ns, as_index_get_set_id(r))) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_FORBIDDEN);
			return TRANS_DONE_ERROR;
		}
	}

	// Shortcut set name.
	const char* set_name = as_index_get_set_name(r, ns);

	// If record existed, check that as_msg set name matches.
	if (! record_created && ! check_msg_set_name(tr, set_name)) {
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Open or create the as_storage_rd, and handle record
	// metadata.
	//

	as_storage_rd rd;

	if (record_created) {
		as_storage_record_create(ns, r, &rd);
	}
	else {
		as_storage_record_open(ns, r, &rd);
	}

	// Deal with delete durability (enterprise only).
	if ((result = set_delete_durablility(tr, &rd)) != 0) {
		write_master_failed(tr, &r_ref, record_created, tree, &rd, result);
		return TRANS_DONE_ERROR;
	}

	// Deal with key storage as needed.
	if ((result = handle_msg_key(tr, &rd)) != 0) {
		write_master_failed(tr, &r_ref, record_created, tree, &rd, result);
		return TRANS_DONE_ERROR;
	}

	bool was_ldt_parent = false;

	// Record-level replace can't maintain an LDT.
	if (ns->ldt_enabled && as_ldt_record_is_parent(r) && record_level_replace) {
		as_index_clear_flags(r, AS_INDEX_FLAG_SPECIAL_BINS);
		was_ldt_parent = true; // so we can unwind
	}

	// Assemble record properties from index information.
	size_t rec_props_data_size = as_storage_record_rec_props_size(&rd);
	uint8_t rec_props_data[rec_props_data_size];

	if (rec_props_data_size > 0) {
		as_storage_record_set_rec_props(&rd, rec_props_data);
	}

	// Convert message TTL special value if appropriate.
	if (record_created && m->record_ttl == TTL_DONT_UPDATE) {
		m->record_ttl = TTL_NAMESPACE_DEFAULT;
	}

	//------------------------------------------------------
	// Split write_master() according to configuration to
	// handle record bins.
	//

	xdr_dirty_bins dirty_bins;
	xdr_clear_dirty_bins(&dirty_bins);

	bool is_delete = false;

	if (ns->storage_data_in_memory) {
		if (ns->single_bin) {
			result = write_master_dim_single_bin(tr, &rd,
					record_created, increment_generation,
					rw, &is_delete, &dirty_bins);
		}
		else {
			result = write_master_dim(tr, set_name, &rd,
					record_level_replace, increment_generation,
					rw, &is_delete, &dirty_bins);
		}
	}
	else {
		if (ns->single_bin) {
			result = write_master_ssd_single_bin(tr, &rd,
					must_fetch_data, increment_generation,
					rw, &is_delete, &dirty_bins);
		}
		else {
			result = write_master_ssd(tr, set_name, &rd,
					must_fetch_data, record_level_replace, increment_generation,
					rw, &is_delete, &dirty_bins);
		}
	}

	if (result != 0) {
		if (was_ldt_parent) {
			as_index_set_flags(r, AS_INDEX_FLAG_SPECIAL_BINS);
		}

		write_master_failed(tr, &r_ref, record_created, tree, &rd, result);
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Done - complete function's output, release the record
	// lock, and do XDR write if appropriate.
	//

	tr->generation = r->generation;
	tr->void_time = r->void_time;
	tr->last_update_time = r->last_update_time;

	// Get set-id before releasing.
	uint16_t set_id = as_index_get_set_id(r_ref.r);

	// Collect more info for XDR.
	uint16_t generation = tr->generation;
	xdr_op_type op_type = XDR_OP_TYPE_WRITE;

	// Handle deletion if appropriate.
	if (is_delete) {
		write_delete_record(r_ref.r, tree);
		cf_atomic64_incr(&ns->n_deleted_last_bin);

		generation = 0;
		op_type = as_transaction_is_durable_delete(tr) ?
				XDR_OP_TYPE_DURABLE_DELETE : XDR_OP_TYPE_DROP;
	}
	// Or (normally) adjust max void-time.
	else if (r->void_time != 0) {
		cf_atomic64_setmax(&tr->rsv.p->max_void_time, r->void_time);
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	// Don't send an XDR delete if it's disallowed.
	if (is_delete && ! is_xdr_delete_shipping_enabled()) {
		return TRANS_IN_PROGRESS;
	}

	// Do an XDR write if the write is a non-XDR write or is an XDR write with
	// forwarding enabled.
	if (! as_msg_is_xdr(m) || is_xdr_forwarding_enabled() ||
			ns->ns_forward_xdr_writes) {
		xdr_write(ns, tr->keyd, generation, 0, op_type, set_id, &dirty_bins);
	}

	return TRANS_IN_PROGRESS;
}


void
write_master_failed(as_transaction* tr, as_index_ref* r_ref,
		bool record_created, as_index_tree* tree, as_storage_rd* rd,
		int result_code)
{
	if (r_ref) {
		if (record_created) {
			as_index_delete(tree, &tr->keyd);
		}

		if (rd) {
			as_storage_record_close(rd);
		}

		as_record_done(r_ref, tr->rsv.ns);
	}

	switch (result_code) {
	case AS_PROTO_RESULT_FAIL_GENERATION:
		cf_atomic64_incr(&tr->rsv.ns->n_fail_generation);
		break;
	case AS_PROTO_RESULT_FAIL_RECORD_TOO_BIG:
		cf_atomic64_incr(&tr->rsv.ns->n_fail_record_too_big);
		break;
	default:
		// These either log warnings or aren't interesting enough to count.
		break;
	}

	tr->result_code = (uint8_t)result_code;
}


int
write_master_preprocessing(as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;
	as_msg* m = &tr->msgp->msg;

	// ns->stop_writes is set by thr_nsup if configured threshold is breached.
	if (cf_atomic32_get(ns->stop_writes) == 1) {
		write_master_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_OUT_OF_SPACE);
		return false;
	}

	if (! as_storage_has_space(ns)) {
		cf_warning(AS_RW, "{%s}: write_master: drives full", ns->name);
		write_master_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_OUT_OF_SPACE);
		return false;
	}

	if (! is_valid_ttl(ns, m->record_ttl)) {
		cf_warning(AS_RW, "write_master: invalid ttl %u", m->record_ttl);
		write_master_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
		return false;
	}

	// Fail if disallow_null_setname is true and set name is absent or empty.
	if (ns->disallow_null_setname) {
		as_msg_field* f = as_transaction_has_set(tr) ?
				as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;

		if (! f || as_msg_field_get_value_sz(f) == 0) {
			cf_warning(AS_RW, "write_master: null/empty set name not allowed for namespace %s", ns->name);
			write_master_failed(tr, 0, false, 0, 0, AS_PROTO_RESULT_FAIL_PARAMETER);
			return false;
		}
	}

	return true;
}


int
write_master_policies(as_transaction* tr, bool* p_must_not_create,
		bool* p_record_level_replace, bool* p_must_fetch_data,
		bool* p_increment_generation)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	if (m->n_ops == 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: bin op(s) expected, none present ", ns->name);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	bool info1_get_all = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	bool must_not_create =
			(m->info3 & AS_MSG_INFO3_UPDATE_ONLY) != 0 ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) != 0;

	bool record_level_replace =
			(m->info3 & AS_MSG_INFO3_CREATE_OR_REPLACE) != 0 ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) != 0;

	bool must_fetch_data = false;

	bool increment_generation = false;

	bool has_read_all_op = false;
	bool generates_response_bin = false;

	// Loop over ops to check and modify flags.
	as_msg_op* op = NULL;
	int i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (op->op != AS_MSG_OP_MC_TOUCH) {
			increment_generation = true;
		}

		if (OP_IS_TOUCH(op->op)) {
			if (record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: touch op can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			must_not_create = true;
			must_fetch_data = true;
			continue;
		}

		if (ns->data_in_index &&
				! is_embedded_particle_type(op->particle_type) &&
				// Allow AS_PARTICLE_TYPE_NULL, although bin-delete operations
				// are not likely in single-bin configuration.
				op->particle_type != AS_PARTICLE_TYPE_NULL) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: can't write data type %u in data-in-index configuration ", ns->name, op->particle_type);
			return AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}

		if (op->name_sz >= AS_ID_BIN_SZ) {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: bin name too long (%d) ", ns->name, op->name_sz);
			return AS_PROTO_RESULT_FAIL_BIN_NAME;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			if (op->particle_type == AS_PARTICLE_TYPE_NULL &&
					record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: bin delete can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}
		}
		else if (OP_IS_MODIFY(op->op)) {
			if (record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: modify op can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			must_fetch_data = true;
		}
		else if (op_is_read_all(op, m)) {
			if (respond_all_ops) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: read-all op can't have respond-all-ops flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			if (has_read_all_op) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: can't have more than one read-all op ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			has_read_all_op = true;
			must_fetch_data = true;
		}
		else if (op->op == AS_MSG_OP_READ) {
			generates_response_bin = true;
			must_fetch_data = true;
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			if (record_level_replace) {
				cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: cdt modify op can't have record-level replace flag ", ns->name);
				return AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			generates_response_bin = true; // CDT modify may generate a response bin
			must_fetch_data = true;
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			generates_response_bin = true;
			must_fetch_data = true;
		}
	}

	if (has_read_all_op && generates_response_bin) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: read-all op can't mix with ops that generate response bins ", ns->name);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	if (info1_get_all && ! has_read_all_op) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: get-all flag set with no read-all op ", ns->name);
		return AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	*p_must_not_create = must_not_create;
	*p_record_level_replace = record_level_replace;
	*p_must_fetch_data = must_fetch_data;
	*p_increment_generation = increment_generation;

	return 0;
}


bool
check_msg_set_name(as_transaction* tr, const char* set_name)
{
	as_msg_field* f = as_transaction_has_set(tr) ?
			as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET) : NULL;

	if (! f || as_msg_field_get_value_sz(f) == 0) {
		if (set_name) {
			cf_warning_digest(AS_RW, &tr->keyd, "overwriting record in set '%s' but msg has no set name ",
					set_name);
		}

		return true;
	}

	size_t msg_set_name_len = as_msg_field_get_value_sz(f);

	if (! set_name ||
			strncmp(set_name, (const char*)f->data, msg_set_name_len) != 0 ||
			set_name[msg_set_name_len] != 0) {
		CF_ZSTR_DEFINE(msg_set_name, AS_SET_NAME_MAX_SIZE + 4, f->data,
				msg_set_name_len);

		cf_warning_digest(AS_RW, &tr->keyd, "overwriting record in set '%s' but msg has different set name '%s' ",
				set_name ? set_name : "(null)", msg_set_name);
		return false;
	}

	return true;
}


//==========================================================
// write_master() splits based on configuration -
// data-in-memory & single-bin.
//
// These handle the bin operations part of write_master()
// which are very different per configuration.
//

int
write_master_dim_single_bin(as_transaction* tr, as_storage_rd* rd,
		bool record_created, bool increment_generation, rw_request* rw,
		bool* is_delete, xdr_dirty_bins* dirty_bins)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;

	rd->n_bins = 1;

	// Set rd->bins!
	// For data-in-memory:
	// - if just created record - sets rd->bins to empty bin embedded in index
	// - otherwise - sets rd->bins to existing embedded bin
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = 0;

	if (! record_created) {
		memory_bytes = as_storage_record_get_n_bytes_memory(rd);
	}

	//------------------------------------------------------
	// Copy existing bin into old_bin to enable unwinding.
	//

	uint32_t n_old_bins = as_bin_inuse_has(rd) ? 1 : 0;
	as_bin old_bin;

	as_single_bin_copy(&old_bin, rd->bins);

	// Collect bins (old or intermediate versions) to destroy on cleanup.
	as_bin cleanup_bins[m->n_ops];
	uint32_t n_cleanup_bins = 0;

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_master_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bin to write.
	//

	uint32_t n_new_bins = 0;
	int result = write_master_bin_ops(tr, rd, NULL, cleanup_bins,
			&n_cleanup_bins, &rw->response_db, &n_new_bins, dirty_bins);

	if (result != 0) {
		write_master_index_metadata_unwind(&old_metadata, r);
		write_master_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
		return result;
	}

	//------------------------------------------------------
	// Created the new bin to write.
	//

	if (n_new_bins == 0) {
		if (n_old_bins == 0) {
			write_master_index_metadata_unwind(&old_metadata, r);
			write_master_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
			return AS_PROTO_RESULT_FAIL_NOTFOUND;
		}

		*is_delete = true;
	}

	// Pickle before writing - can't fail after.
	if (! pickle_all(rd, rw)) {
		write_master_index_metadata_unwind(&old_metadata, r);
		write_master_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_record_write() ", ns->name);
		write_master_index_metadata_unwind(&old_metadata, r);
		write_master_dim_single_bin_unwind(&old_bin, rd->bins, cleanup_bins, n_cleanup_bins);
		return -result;
	}

	//------------------------------------------------------
	// Cleanup - destroy relevant bins, can't unwind after.
	//

	destroy_stack_bins(cleanup_bins, n_cleanup_bins);

	as_storage_record_adjust_mem_stats(rd, memory_bytes);

	return 0;
}


int
write_master_dim(as_transaction* tr, const char* set_name, as_storage_rd* rd,
		bool record_level_replace, bool increment_generation, rw_request* rw,
		bool* is_delete, xdr_dirty_bins* dirty_bins)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;

	// Set rd->n_bins!
	// For data-in-memory - number of bins in existing record.
	as_storage_rd_load_n_bins(rd);

	// Set rd->bins!
	// For data-in-memory:
	// - if just created record - sets rd->bins to NULL
	// - otherwise - sets rd->bins to existing (already populated) bins array
	as_storage_rd_load_bins(rd, NULL);

	// For memory accounting, note current usage.
	uint64_t memory_bytes = as_storage_record_get_n_bytes_memory(rd);

	//------------------------------------------------------
	// Copy existing bins to new space, and keep old bins
	// intact for sindex adjustment and so it's possible to
	// unwind on failure.
	//

	uint32_t n_old_bins = (uint32_t)rd->n_bins;
	uint32_t n_new_bins = n_old_bins + m->n_ops; // can't be more than this

	size_t old_bins_size = n_old_bins * sizeof(as_bin);
	size_t new_bins_size = n_new_bins * sizeof(as_bin);

	as_bin* old_bins = rd->bins;
	as_bin new_bins[n_new_bins];

	if (old_bins_size == 0 || record_level_replace) {
		memset(new_bins, 0, new_bins_size);
	}
	else {
		memcpy(new_bins, old_bins, old_bins_size);
		memset(new_bins + n_old_bins, 0, new_bins_size - old_bins_size);
	}

	rd->n_bins = (uint16_t)n_new_bins;
	rd->bins = new_bins;

	// Collect bins (old or intermediate versions) to destroy on cleanup.
	as_bin cleanup_bins[m->n_ops];
	uint32_t n_cleanup_bins = 0;

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_master_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bins to write.
	//

	int result = write_master_bin_ops(tr, rd, NULL, cleanup_bins,
			&n_cleanup_bins, &rw->response_db, &n_new_bins, dirty_bins);

	if (result != 0) {
		write_master_index_metadata_unwind(&old_metadata, r);
		write_master_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return result;
	}

	//------------------------------------------------------
	// Created the new bins to write.
	//

	as_bin_space* new_bin_space = NULL;

	// Adjust - the actual number of new bins.
	rd->n_bins = n_new_bins;

	if (n_new_bins != 0) {
		new_bins_size = n_new_bins * sizeof(as_bin);
		new_bin_space = (as_bin_space*)
				cf_malloc_ns(sizeof(as_bin_space) + new_bins_size);

		if (! new_bin_space) {
			cf_warning(AS_RW, "write_master: failed alloc new as_bin_space");
			write_master_index_metadata_unwind(&old_metadata, r);
			write_master_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
			return AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}
	else {
		if (n_old_bins == 0) {
			write_master_index_metadata_unwind(&old_metadata, r);
			write_master_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
			return AS_PROTO_RESULT_FAIL_NOTFOUND;
		}

		*is_delete = true;
	}

	// Pickle before writing - can't fail after.
	if (! pickle_all(rd, rw)) {
		if (new_bin_space) {
			cf_free(new_bin_space);
		}

		write_master_index_metadata_unwind(&old_metadata, r);
		write_master_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_record_write() ", ns->name);

		if (new_bin_space) {
			cf_free(new_bin_space);
		}

		write_master_index_metadata_unwind(&old_metadata, r);
		write_master_dim_unwind(old_bins, n_old_bins, new_bins, n_new_bins, cleanup_bins, n_cleanup_bins);
		return -result;
	}

	//------------------------------------------------------
	// Success - adjust sindex, looking at old and new bins.
	//

	if (record_has_sindex(r, ns) &&
			write_master_sindex_update(ns, set_name, &tr->keyd, old_bins,
					n_old_bins, new_bins, n_new_bins)) {
		tr->flags |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
	}

	//------------------------------------------------------
	// Cleanup - destroy relevant bins, can't unwind after.
	//

	if (record_level_replace) {
		destroy_stack_bins(old_bins, n_old_bins);
	}

	destroy_stack_bins(cleanup_bins, n_cleanup_bins);

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Fill out new_bin_space.
	if (n_new_bins != 0) {
		new_bin_space->n_bins = rd->n_bins;
		memcpy((void*)new_bin_space->bins, new_bins, new_bins_size);
	}

	// Swizzle the index element's as_bin_space pointer.
	as_bin_space* old_bin_space = as_index_get_bin_space(r);

	if (old_bin_space) {
		cf_free(old_bin_space);
	}

	as_index_set_bin_space(r, new_bin_space);

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
		// TODO - should we check allocation failure?
		as_record_allocate_key(r, rd->key, rd->key_size);
		as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	as_storage_record_adjust_mem_stats(rd, memory_bytes);

	return 0;
}


int
write_master_ssd_single_bin(as_transaction* tr, as_storage_rd* rd,
		bool must_fetch_data, bool increment_generation, rw_request* rw,
		bool* is_delete, xdr_dirty_bins* dirty_bins)
{
	// Shortcut pointers.
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;

	rd->ignore_record_on_device = ! must_fetch_data;
	rd->n_bins = 1;

	as_bin stack_bin;

	// Set rd->bins!
	// For non-data-in-memory:
	// - if just created record, or must_fetch_data is false - sets rd->bins to
	//		empty stack_bin
	// - otherwise - sets rd->bins to stack_bin, reads existing record off
	//		device and populates bin (including particle pointer into block
	//		buffer)
	int result = as_storage_rd_load_bins(rd, &stack_bin);

	if (result < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_rd_load_bins()", ns->name);
		return -result;
	}

	uint32_t n_old_bins = as_bin_inuse_has(rd) ? 1 : 0;

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_master_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bin to write.
	//

	cf_ll_buf_define(particles_llb, STACK_PARTICLES_SIZE);

	uint32_t n_new_bins = 0;

	if ((result = write_master_bin_ops(tr, rd, &particles_llb, NULL, NULL,
			&rw->response_db, &n_new_bins, dirty_bins)) != 0) {
		cf_ll_buf_free(&particles_llb);
		write_master_index_metadata_unwind(&old_metadata, r);
		return result;
	}

	//------------------------------------------------------
	// Created the new bin to write.
	//

	if (n_new_bins == 0) {
		if (n_old_bins == 0) {
			cf_ll_buf_free(&particles_llb);
			write_master_index_metadata_unwind(&old_metadata, r);
			return AS_PROTO_RESULT_FAIL_NOTFOUND;
		}

		*is_delete = true;
	}

	// Pickle before writing - bins may disappear on as_storage_record_close().
	if (! pickle_all(rd, rw)) {
		cf_ll_buf_free(&particles_llb);
		write_master_index_metadata_unwind(&old_metadata, r);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_record_write() ", ns->name);
		cf_ll_buf_free(&particles_llb);
		write_master_index_metadata_unwind(&old_metadata, r);
		return -result;
	}

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
		as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	cf_ll_buf_free(&particles_llb);

	return 0;
}


int
write_master_ssd(as_transaction* tr, const char* set_name, as_storage_rd* rd,
		bool must_fetch_data, bool record_level_replace,
		bool increment_generation, rw_request* rw, bool* is_delete,
		xdr_dirty_bins* dirty_bins)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;
	bool has_sindex = record_has_sindex(r, ns);

	// If it's not touch or modify, determine if we must read existing record.
	if (! must_fetch_data) {
		must_fetch_data = has_sindex || ! record_level_replace;
	}

	rd->ignore_record_on_device = ! must_fetch_data;

	// Set rd->n_bins!
	// For non-data-in-memory:
	// - if just created record, or must_fetch_data is false - 0
	// - otherwise - number of bins in existing record
	int result = as_storage_rd_load_n_bins(rd);

	if (result < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_rd_load_n_bins()", ns->name);
		return -result;
	}

	uint32_t n_old_bins = (uint32_t)rd->n_bins;
	uint32_t n_new_bins = n_old_bins + m->n_ops; // can't be more than this

	// Needed for as_storage_rd_load_bins() to clear all unused bins.
	rd->n_bins = (uint16_t)n_new_bins;

	// Stack space for resulting record's bins.
	as_bin old_bins[n_old_bins];
	as_bin new_bins[n_new_bins];

	// Set rd->bins!
	// For non-data-in-memory:
	// - if just created record, or must_fetch_data is false - sets rd->bins to
	//		empty new_bins
	// - otherwise - sets rd->bins to new_bins, reads existing record off device
	//		and populates bins (including particle pointers into block buffer)
	if ((result = as_storage_rd_load_bins(rd, new_bins)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_rd_load_bins()", ns->name);
		return -result;
	}

	//------------------------------------------------------
	// Copy old bins (if any) - which are currently in new
	// bins array - to old bins array, for sindex purposes.
	//

	if (has_sindex && n_old_bins != 0) {
		memcpy(old_bins, new_bins, n_old_bins * sizeof(as_bin));

		// If it's a replace, clear the new bins array.
		if (record_level_replace) {
			as_bin_set_all_empty(rd);
		}
	}

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	index_metadata old_metadata;

	write_master_update_index_metadata(tr, increment_generation, &old_metadata, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bins to write.
	//

	cf_ll_buf_define(particles_llb, STACK_PARTICLES_SIZE);

	if ((result = write_master_bin_ops(tr, rd, &particles_llb, NULL, NULL,
			&rw->response_db, &n_new_bins, dirty_bins)) != 0) {
		cf_ll_buf_free(&particles_llb);
		write_master_index_metadata_unwind(&old_metadata, r);
		return result;
	}

	//------------------------------------------------------
	// Created the new bins to write.
	//

	// Adjust - the actual number of new bins.
	rd->n_bins = n_new_bins;

	if (n_new_bins == 0) {
		if (n_old_bins == 0) {
			cf_ll_buf_free(&particles_llb);
			write_master_index_metadata_unwind(&old_metadata, r);
			return AS_PROTO_RESULT_FAIL_NOTFOUND;
		}

		*is_delete = true;
	}

	// Pickle before writing - bins may disappear on as_storage_record_close().
	if (! pickle_all(rd, rw)) {
		cf_ll_buf_free(&particles_llb);
		write_master_index_metadata_unwind(&old_metadata, r);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	//------------------------------------------------------
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_storage_record_write() ", ns->name);
		cf_ll_buf_free(&particles_llb);
		write_master_index_metadata_unwind(&old_metadata, r);
		return -result;
	}

	//------------------------------------------------------
	// Success - adjust sindex, looking at old and new bins.
	//

	if (has_sindex &&
			write_master_sindex_update(ns, set_name, &tr->keyd, old_bins,
					n_old_bins, new_bins, n_new_bins)) {
		tr->flags |= AS_TRANSACTION_FLAG_SINDEX_TOUCHED;
	}

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (! as_index_is_flag_set(r, AS_INDEX_FLAG_KEY_STORED) && rd->key) {
		as_index_set_flags(r, AS_INDEX_FLAG_KEY_STORED);
	}

	cf_ll_buf_free(&particles_llb);

	return 0;
}


//==========================================================
// write_master() - apply record updates.
//

void
write_master_update_index_metadata(as_transaction* tr,
		bool increment_generation, index_metadata* old, as_record* r)
{
	old->void_time = r->void_time;
	old->last_update_time = r->last_update_time;
	old->generation = r->generation;

	update_metadata_in_index(tr, increment_generation, r);
}


int
write_master_bin_ops(as_transaction* tr, as_storage_rd* rd,
		cf_ll_buf* particles_llb, as_bin* cleanup_bins,
		uint32_t* p_n_cleanup_bins, cf_dyn_buf* db, uint32_t* p_n_final_bins,
		xdr_dirty_bins* dirty_bins)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;
	bool has_read_all_op = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;

	as_msg_op* ops[m->n_ops];
	as_bin response_bins[has_read_all_op ? rd->n_bins : m->n_ops];
	as_bin result_bins[m->n_ops];

	uint32_t n_response_bins = 0;
	uint32_t n_result_bins = 0;

	int result = write_master_bin_ops_loop(tr, rd, ops, response_bins,
			&n_response_bins, result_bins, &n_result_bins, particles_llb,
			cleanup_bins, p_n_cleanup_bins, dirty_bins);

	if (result != 0) {
		destroy_stack_bins(result_bins, n_result_bins);
		return result;
	}

	*p_n_final_bins = as_bin_inuse_count(rd);

	if (n_response_bins == 0) {
		// If 'ordered-ops' flag was not set, and there were no read ops or CDT
		// ops with results, there's no response to build and send later.
		return 0;
	}

	as_bin* bins[n_response_bins];

	for (uint32_t i = 0; i < n_response_bins; i++) {
		as_bin* b = &response_bins[i];

		bins[i] = as_bin_inuse(b) ? b : NULL;
	}

	uint32_t generation = r->generation;
	uint32_t void_time = r->void_time;

	// Deletes don't return metadata.
	if (*p_n_final_bins == 0) {
		generation = 0;
		void_time = 0;
	}

	size_t msg_sz = 0;
	uint8_t* msgp = (uint8_t*)as_msg_make_response_msg(AS_PROTO_RESULT_OK,
			generation, void_time, has_read_all_op ? NULL : ops, bins,
			(uint16_t)n_response_bins, ns, NULL, &msg_sz,
			as_transaction_trid(tr), NULL);

	destroy_stack_bins(result_bins, n_result_bins);

	if (! msgp)	{
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed make response msg ", ns->name);
		return AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Stash the message, to be sent later.
	db->buf = msgp;
	db->is_stack = false;
	db->alloc_sz = msg_sz;
	db->used_sz = msg_sz;

	return 0;
}


int
write_master_bin_ops_loop(as_transaction* tr, as_storage_rd* rd,
		as_msg_op** ops, as_bin* response_bins, uint32_t* p_n_response_bins,
		as_bin* result_bins, uint32_t* p_n_result_bins,
		cf_ll_buf* particles_llb, as_bin* cleanup_bins,
		uint32_t* p_n_cleanup_bins, xdr_dirty_bins* dirty_bins)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	int result;

	as_msg_op* op = NULL;
	int i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (OP_IS_TOUCH(op->op)) {
			continue;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			// AS_PARTICLE_TYPE_NULL means delete the bin.
			// TODO - should this even be allowed for single-bin?
			if (op->particle_type == AS_PARTICLE_TYPE_NULL) {
				int32_t j = as_bin_get_index_from_buf(rd, op->name, op->name_sz);

				if (j != -1) {
					if (ns->storage_data_in_memory) {
						// Double copy necessary for single-bin, but doing it
						// generally for code simplicity.
						as_bin cleanup_bin;
						as_bin_copy(ns, &cleanup_bin, &rd->bins[j]);

						append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
					}

					as_bin_set_empty_shift(rd, j);
					xdr_fill_dirty_bins(dirty_bins);
				}
			}
			// It's a regular bin write.
			else {
				as_bin* b = as_bin_get_or_create_from_buf(rd, op->name, op->name_sz, &result);

				if (! b) {
					return result;
				}

				if (ns->storage_data_in_memory) {
					as_bin cleanup_bin;
					as_bin_copy(ns, &cleanup_bin, b);

					if ((result = as_bin_particle_alloc_from_client(b, op)) < 0) {
						cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_particle_alloc_from_client() ", ns->name);
						return -result;
					}

					append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
				}
				else {
					if ((result = as_bin_particle_stack_from_client(b, particles_llb, op)) < 0) {
						cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_particle_stack_from_client() ", ns->name);
						return -result;
					}
				}

				xdr_add_dirty_bin(ns, dirty_bins, (const char*)op->name, op->name_sz);
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		// Modify an existing bin value.
		else if (OP_IS_MODIFY(op->op)) {
			as_bin* b = as_bin_get_or_create_from_buf(rd, op->name, op->name_sz, &result);

			if (! b) {
				return result;
			}

			if (ns->storage_data_in_memory) {
				as_bin cleanup_bin;
				as_bin_copy(ns, &cleanup_bin, b);

				if ((result = as_bin_particle_alloc_modify_from_client(b, op)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_particle_alloc_modify_from_client() ", ns->name);
					return -result;
				}

				append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
			}
			else {
				if ((result = as_bin_particle_stack_modify_from_client(b, particles_llb, op)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_particle_stack_modify_from_client() ", ns->name);
					return -result;
				}
			}

			xdr_add_dirty_bin(ns, dirty_bins, (const char*)op->name, op->name_sz);

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op_is_read_all(op, m)) {
			for (uint16_t i = 0; i < rd->n_bins; i++) {
				as_bin* b = &rd->bins[i];

				if (! as_bin_inuse(b)) {
					break;
				}

				// ops array will not be not used in this case.
				as_bin_copy(ns, &response_bins[(*p_n_response_bins)++], b);
			}
		}
		else if (op->op == AS_MSG_OP_READ) {
			as_bin* b = as_bin_get_from_buf(rd, op->name, op->name_sz);

			if ((result = write_master_bin_check(tr, b)) != 0) {
				return result;
			}

			if (b) {
				ops[*p_n_response_bins] = op;
				as_bin_copy(ns, &response_bins[(*p_n_response_bins)++], b);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			as_bin* b = as_bin_get_or_create_from_buf(rd, op->name, op->name_sz, &result);

			if (! b) {
				return result;
			}

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if (ns->storage_data_in_memory) {
				as_bin cleanup_bin;
				as_bin_copy(ns, &cleanup_bin, b);

				if ((result = as_bin_cdt_alloc_modify_from_client(b, op, &result_bin)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_cdt_alloc_modify_from_client() ", ns->name);
					return -result;
				}

				// Account for noop CDT operations. Modifying non-mutable
				// particle contents in-place is still disallowed.
				if (cleanup_bin.particle != b->particle) {
					append_bin_to_destroy(&cleanup_bin, cleanup_bins, p_n_cleanup_bins);
				}
			}
			else {
				if ((result = as_bin_cdt_stack_modify_from_client(b, particles_llb, op, &result_bin)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_cdt_alloc_modify_from_client() ", ns->name);
					return -result;
				}
			}

			if (respond_all_ops || as_bin_inuse(&result_bin)) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}

			if (! as_bin_inuse(b)) {
				// TODO - could do better than finding index from name.
				int32_t index = as_bin_get_index_from_buf(rd, op->name, op->name_sz);

				if (index >= 0) {
					as_bin_set_empty_shift(rd, (uint32_t)index);
					xdr_fill_dirty_bins(dirty_bins);
				}
			}
			else {
				xdr_add_dirty_bin(ns, dirty_bins, (const char*)op->name, op->name_sz);
			}
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			as_bin* b = as_bin_get_from_buf(rd, op->name, op->name_sz);

			if ((result = write_master_bin_check(tr, b)) != 0) {
				return result;
			}

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_cdt_read_from_client(b, op, &result_bin)) < 0) {
					cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: failed as_bin_cdt_read_from_client() ", ns->name);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else {
			cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: unknown bin op %u ", ns->name, op->op);
			return AS_PROTO_RESULT_FAIL_PARAMETER;
		}
	}

	return 0;
}


// For now, used only for read ops.
int
write_master_bin_check(as_transaction* tr, as_bin* bin)
{
	if (bin && as_bin_is_hidden(bin)) {
		// Note - if single-bin, this likely means the bin state is corrupt.
		cf_warning_digest(AS_RW, &tr->keyd, "{%s} write_master: cannot manipulate hidden bin directly ", tr->rsv.ns->name);
		return AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	return 0;
}


bool
write_master_sindex_update(as_namespace* ns, const char* set_name,
		cf_digest* keyd, as_bin* old_bins, uint32_t n_old_bins,
		as_bin* new_bins, uint32_t n_new_bins)
{
	int sbins_populated = 0;
	bool not_just_created[n_new_bins];

	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		not_just_created[i_new] = false;
	}

	// Maximum number of sindexes which can be changed in one transaction is
	// 2 * ns->sindex_cnt.

	SINDEX_GRLOCK();
	SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);
	as_sindex* si_arr[2 * ns->sindex_cnt];
	int si_arr_index = 0;

	// Reserve matching SIs.

	for (int i = 0; i < n_old_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				old_bins[i].id, &si_arr[si_arr_index]);
	}

	for (int i = 0; i < n_new_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				new_bins[i].id, &si_arr[si_arr_index]);
	}

	// For every old bin, find the corresponding new bin (if any) and adjust the
	// secondary index if the bin was modified. If no corresponding new bin is
	// found, it means the old bin was deleted - also adjust the secondary index
	// accordingly.

	for (int32_t i_old = 0; i_old < (int32_t)n_old_bins; i_old++) {
		as_bin* b_old = &old_bins[i_old];
		bool found = false;

		// Loop over new bins. Start at old bin index (if possible) and go down,
		// wrapping around to do the higher indexes last. This will find a match
		// (if any) very quickly - instantly, unless there were bins deleted.

		bool any_new = n_new_bins != 0;
		int32_t n_new_minus_1 = (int32_t)n_new_bins - 1;
		int32_t i_new = n_new_minus_1 < i_old ? n_new_minus_1 : i_old;

		while (any_new) {
			as_bin* b_new = &new_bins[i_new];

			if (b_old->id == b_new->id) {
				if (as_bin_get_particle_type(b_old) != as_bin_get_particle_type(b_new) ||
						b_old->particle != b_new->particle) {
					sbins_populated += as_sindex_sbins_populate(&sbins[sbins_populated], ns, set_name, b_old, b_new);
				}

				found = true;
				not_just_created[i_new] = true;
				break;
			}

			if (--i_new < 0 && (i_new = n_new_minus_1) <= i_old) {
				break;
			}

			if (i_new == i_old) {
				break;
			}
		}

		if (! found) {
			sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b_old,
					&sbins[sbins_populated], AS_SINDEX_OP_DELETE);
		}
	}

	// Now find the new bins that are just-created bins. We've marked the others
	// in the loop above, so any left are just-created.

	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		if (not_just_created[i_new]) {
			continue;
		}

		sbins_populated += as_sindex_sbins_from_bin(ns, set_name,
				&new_bins[i_new], &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
	}

	SINDEX_GRUNLOCK();

	if (sbins_populated != 0) {
		as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated, keyd);
		as_sindex_sbin_freeall(sbins, sbins_populated);
	}

	as_sindex_release_arr(si_arr, si_arr_index);

	return sbins_populated != 0;
}


//==========================================================
// write_master() - unwind on failure or cleanup.
//

void
write_master_index_metadata_unwind(index_metadata* old, as_record* r)
{
	r->void_time = old->void_time;
	r->last_update_time = old->last_update_time;
	r->generation = old->generation;
}


void
write_master_dim_single_bin_unwind(as_bin* old_bin, as_bin* new_bin,
		as_bin* cleanup_bins, uint32_t n_cleanup_bins)
{
	as_particle* p_old = as_bin_get_particle(old_bin);

	if (as_bin_is_external_particle(new_bin) && new_bin->particle != p_old) {
		as_bin_particle_destroy(new_bin, true);
	}

	for (uint32_t i_cleanup = 0; i_cleanup < n_cleanup_bins; i_cleanup++) {
		as_bin* b_cleanup = &cleanup_bins[i_cleanup];

		if (b_cleanup->particle != p_old) {
			as_bin_particle_destroy(b_cleanup, true);
		}
	}

	as_single_bin_copy(new_bin, old_bin);
}


void
write_master_dim_unwind(as_bin* old_bins, uint32_t n_old_bins, as_bin* new_bins,
		uint32_t n_new_bins, as_bin* cleanup_bins, uint32_t n_cleanup_bins)
{
	for (uint32_t i_new = 0; i_new < n_new_bins; i_new++) {
		as_bin* b_new = &new_bins[i_new];

		if (! as_bin_inuse(b_new)) {
			break;
		}

		// Embedded particles have no-op destructors - skip loop over old bins.
		if (as_bin_is_embedded_particle(b_new)) {
			continue;
		}

		as_particle* p_new = b_new->particle;
		uint32_t i_old;

		for (i_old = 0; i_old < n_old_bins; i_old++) {
			as_bin* b_old = &old_bins[i_old];

			if (b_new->id == b_old->id) {
				if (p_new != as_bin_get_particle(b_old)) {
					as_bin_particle_destroy(b_new, true);
				}

				break;
			}
		}

		if (i_old == n_old_bins) {
			as_bin_particle_destroy(b_new, true);
		}
	}

	for (uint32_t i_cleanup = 0; i_cleanup < n_cleanup_bins; i_cleanup++) {
		as_bin* b_cleanup = &cleanup_bins[i_cleanup];
		as_particle* p_cleanup = b_cleanup->particle;
		uint32_t i_old;

		for (i_old = 0; i_old < n_old_bins; i_old++) {
			as_bin* b_old = &old_bins[i_old];

			if (b_cleanup->id == b_old->id) {
				if (p_cleanup != as_bin_get_particle(b_old)) {
					as_bin_particle_destroy(b_cleanup, true);
				}

				break;
			}
		}

		if (i_old == n_old_bins) {
			as_bin_particle_destroy(b_cleanup, true);
		}
	}

	// The index element's as_bin_space pointer still points at old bins.
}
