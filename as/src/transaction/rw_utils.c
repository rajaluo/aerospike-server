/*
 * rw_utils.c
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

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_clock.h"

#include "fault.h"
#include "msg.h"

#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/secondary_index.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

void
send_rw_messages(rw_request* rw)
{
	for (int i = 0; i < rw->n_dest_nodes; i++) {
		if (rw->dest_complete[i]) {
			continue;
		}

		msg_incr_ref(rw->dest_msg);

		int rv = as_fabric_send(rw->dest_nodes[i], rw->dest_msg,
				AS_FABRIC_PRIORITY_MEDIUM);

		if (rv != AS_FABRIC_SUCCESS) {
			if (rv != AS_FABRIC_ERR_NO_NODE) {
				// Can't get AS_FABRIC_ERR_QUEUE_FULL for MEDIUM priority.
				cf_crash(AS_RW, "unexpected fabric send result %d", rv);
			}

			as_fabric_msg_put(rw->dest_msg);

			// Mark as complete although we won't have a response msg.
			rw->dest_complete[i] = true;
			rw->dup_result_code[i] = AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}
}


int
set_set_from_msg(as_record* r, as_namespace* ns, as_msg* m)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET);
	size_t name_len = (size_t)as_msg_field_get_value_sz(f);

	if (name_len == 0) {
		return 0;
	}

	// Given the name, find/assign the set-ID and write it in the as_index.
	return as_index_set_set_w_len(r, ns, (const char*)f->data, name_len, true);
}


// Caller must have checked that key is present in message.
bool
check_msg_key(as_msg* m, as_storage_rd* rd)
{
	as_msg_field* f = as_msg_field_get(m, AS_MSG_FIELD_TYPE_KEY);
	uint32_t key_size = as_msg_field_get_value_sz(f);
	uint8_t* key = f->data;

	if (key_size != rd->key_size || memcmp(key, rd->key, key_size) != 0) {
		cf_warning(AS_RW, "key mismatch - end of universe?");
		return false;
	}

	return true;
}


bool
get_msg_key(as_transaction* tr, as_storage_rd* rd)
{
	if (! as_transaction_has_key(tr)) {
		return true;
	}

	as_msg_field* f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_KEY);

	if (rd->ns->single_bin && rd->ns->storage_data_in_memory) {
		// For now we just ignore the key - should we fail out of write_local()?
		cf_warning(AS_RW, "can't store key if data-in-memory & single-bin");
		return false;
	}

	rd->key_size = as_msg_field_get_value_sz(f);
	rd->key = f->data;

	return true;
}


void
update_metadata_in_index(as_transaction* tr, bool increment_generation,
		as_record* r)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	uint64_t now = cf_clepoch_milliseconds();

	if (m->record_ttl == 0xFFFFffff) {
		// TTL = -1 - set record to "never expire".
		r->void_time = 0;
	}
	else if (m->record_ttl != 0) {
		// Assuming we checked m->record_ttl <= 10 years, so no overflow etc.
		r->void_time = (now / 1000) + m->record_ttl;
	}
	else if (ns->default_ttl != 0) {
		// TTL = 0 - set record void-time using default TTL value.
		r->void_time = (now / 1000) + ns->default_ttl;
	}
	else {
		// TTL = 0 - and default TTL is "never expire".
		r->void_time = 0;
	}

	if (as_ldt_record_is_sub(r)) {
		// Sub-records never expire by themselves.
		r->void_time = 0;
	}

	// Note - last-update-time is not allowed to go backwards!
	if (r->last_update_time < now) {
		r->last_update_time = now;
	}

	if (increment_generation) {
		r->generation++;

		// The generation might wrap - 0 is reserved as "uninitialized".
		if (r->generation == 0) {
			r->generation = 1;
		}
	}
}


bool
pickle_all(as_storage_rd* rd, rw_request* rw)
{
	if (as_record_pickle(rd->r, rd, &rw->pickled_buf, &rw->pickled_sz) != 0) {
		return false;
	}

	// TODO - we could avoid this copy (and maybe even not do this here at all)
	// if all callers malloc'd rd->rec_props.p_data upstream for hand-off...
	if (rd->rec_props.p_data) {
		rw->pickled_rec_props.size = rd->rec_props.size;
		rw->pickled_rec_props.p_data = cf_malloc(rd->rec_props.size);

		if (! rw->pickled_rec_props.p_data) {
			cf_free(rw->pickled_buf);
			return false;
		}

		memcpy(rw->pickled_rec_props.p_data, rd->rec_props.p_data,
				rd->rec_props.size);
	}

	return true;
}


// Remove record from secondary index. Called only for data-in-memory. If
// data-not-in-memory, existing record is not read, and secondary index entry is
// cleaned up by background sindex defrag thread.
void
delete_adjust_sindex(as_storage_rd* rd)
{
	as_namespace* ns = rd->ns;

	if (! as_sindex_ns_has_sindex(ns)) {
		return;
	}

	as_record* r = rd->r;

	rd->n_bins = as_bin_get_n_bins(r, rd);
	rd->bins = as_bin_get_all(r, rd, NULL);

	int status = AS_SINDEX_OK;
	const char* set_name = as_index_get_set_name(r, ns);

	SINDEX_GRLOCK();

	SINDEX_BINS_SETUP(sbins, ns->sindex_cnt);

	as_sindex* si_arr[ns->sindex_cnt];
	int si_arr_index = 0;
	int sbins_populated = 0;

	// Reserve matching sindexes.
	for (int i = 0; i < (int)rd->n_bins; i++) {
		si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name,
				rd->bins[i].id, &si_arr[si_arr_index]);
	}

	for (int i = 0; i < (int)rd->n_bins; i++) {
		sbins_populated += as_sindex_sbins_from_bin(ns, set_name, &rd->bins[i],
				&sbins[sbins_populated], AS_SINDEX_OP_DELETE);
	}

	SINDEX_GUNLOCK();

	if (sbins_populated) {
		status = as_sindex_update_by_sbin(ns, set_name, sbins, sbins_populated,
				&rd->keyd);
		as_sindex_sbin_freeall(sbins, sbins_populated);
	}

	as_sindex_release_arr(si_arr, si_arr_index);
}
