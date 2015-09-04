/* 
 * aggr.c
 *
 * Copyright (C) 2014 Aerospike, Inc.
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

#include "base/aggr.h"

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>


#include "aerospike/as_val.h"
#include "aerospike/mod_lua.h"
#include "citrusleaf/cf_ll.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/udf_arglist.h"
#include "base/udf_memtracker.h"
#include "base/udf_record.h"


#define AS_AGGR_ERR  -1
#define AS_AGGR_OK    0

/*
 * Aggregation Stream Object
 */
// **************************************************************************************************
typedef struct {
	// Iteration
	bool                    open; // rename it and structure
	cf_ll_iterator        * iter;
	as_index_keys_arr     * keys_arr;
	int                     keys_arr_offset;
	
	// Record 
	as_rec                * urec;
	as_partition_reservation * rsv;
	
	// Module Data
	as_aggr_call          * call;
	void                  * udata;
} aggr_record;

static as_partition_reservation *
ptn_reserve(aggr_record *arecord, as_partition_id pid, as_partition_reservation *rsv)
{
	as_aggr_call *call = arecord->call;
	if (call && call->aggr_hooks && call->aggr_hooks->ptn_reserve) {
		return call->aggr_hooks->ptn_reserve(arecord->udata, rsv->ns, pid, rsv);
	} 
	return NULL;
}

static void
ptn_release(aggr_record *arecord)
{
	as_aggr_call  *call = arecord->call;
	if (call && call->aggr_hooks && call->aggr_hooks->ptn_release) {
		call->aggr_hooks->ptn_release(arecord->udata, arecord->rsv);	
	}
}

static void
set_error(aggr_record *arecord, int err)
{
	as_aggr_call  *call = arecord->call;
	if (call && call->aggr_hooks && call->aggr_hooks->set_error) {
		call->aggr_hooks->set_error(arecord->udata, err);
	}
}

static bool
pre_check(aggr_record *arecord, void *skey)
{
	as_aggr_call  *call = arecord->call;
	if (call && call->aggr_hooks && call->aggr_hooks->pre_check) {
		return call->aggr_hooks->pre_check(arecord->udata, as_rec_source(arecord->urec), skey);
	} 
	return true; // if not defined pre_check succeeds
}

static int
aopen(aggr_record *arecord, cf_digest digest) 
{
	udf_record   * urecord  = as_rec_source(arecord->urec);
	as_index_ref   * r_ref  = urecord->r_ref;
	as_transaction * tr     = urecord->tr;

	int pid                = as_partition_getid(digest);
	urecord->keyd = digest; 

	AS_PARTITION_RESERVATION_INIT(tr->rsv);	
	arecord->rsv        = ptn_reserve(arecord, pid, &tr->rsv);
	if (!arecord->rsv) {
		cf_debug(AS_AGGR, "Reservation not done for partition %d", pid);
		return -1; 
	}
	
	// NB: Partial Initialization due to heaviness. Not everything needed
	// TODO: Make such initialization Commodity
	tr->rsv.state       = arecord->rsv->state;
	tr->rsv.pid         = arecord->rsv->pid;
	tr->rsv.p           = arecord->rsv->p;
	tr->rsv.tree        = arecord->rsv->tree;
	tr->rsv.cluster_key = arecord->rsv->cluster_key;
	tr->rsv.sub_tree    = arecord->rsv->sub_tree;
	tr->keyd            = urecord->keyd;

	r_ref->skip_lock    = false;
	if (udf_record_open(urecord) == 0) { 
		arecord->open   = true;
		return 0;
	}
	ptn_release(arecord);
	return -1;
}

void
aclose(aggr_record *arecord)
{
	// Bypassing doing the direct destroy because we need to
	// avoid reducing the ref count. This rec (query_record
	// implementation of as_rec) is ref counted when passed from
	// here to Lua. If Lua access it even after moving to next
	// element in the stream it does it at its own risk. Record
	// may have changed under the hood.
	if (arecord->open) {
		udf_record_close(as_rec_source(arecord->urec));
		ptn_release(arecord);
		arecord->open = false;
	}
	return;
}

void
acleanup(aggr_record *arecord) 
{
	if (arecord->iter) {
		cf_ll_releaseIterator(arecord->iter);
		arecord->iter = NULL;
	}
	aclose(arecord);
}

// **************************************************************************************************

/*
 * Aggregation Input Stream
 */
// **************************************************************************************************
cf_digest *
get_next(aggr_record *arecord)
{
	if (!arecord->keys_arr) {
		cf_ll_element * ele       = cf_ll_getNext(arecord->iter);
		if (!ele) {
			arecord->keys_arr = NULL;
			cf_detail(AS_AGGR, "No more digests found in agg stream");	
		}
		else {
			arecord->keys_arr = ((as_index_keys_ll_element*)ele)->keys_arr;
		}
		arecord->keys_arr_offset  = 0;
	} 
	as_index_keys_arr  * keys_arr  = arecord->keys_arr;

	if (!keys_arr) {
		cf_debug(AS_AGGR, "No digests found in agg stream");
		return NULL;
	}

	if (keys_arr->num == arecord->keys_arr_offset) {
		cf_ll_element * ele  = cf_ll_getNext(arecord->iter);
		if (!ele) {
			cf_detail(AS_AGGR, "No More Nodes for this Lua Call");
			return NULL;
		}
		keys_arr                 = ((as_index_keys_ll_element*)ele)->keys_arr;
		arecord->keys_arr_offset = 0;
		arecord->keys_arr        = keys_arr;
		cf_detail(AS_AGGR, "Moving to next node of digest list");
	} else {
		arecord->keys_arr_offset++;
	}

	return &arecord->keys_arr->pindex_digs[arecord->keys_arr_offset];
}

// only operates on the record as_val in the stream points to
// and updates the references ... this function has to acquire
// partition reservation and also the object lock. So if the UDF
// does something stupid the object lock is gonna get held for
// a while ... there has to be timeout mechanism in here I think
static as_val *
istream_read(const as_stream *s) 
{
	aggr_record *arecord = as_stream_source(s);

	aclose(arecord);

	// Iterate through stream to get next digest and
	// populate record with it
	while (!arecord->open) {
		
		if (get_next(arecord)) { 
			return NULL;
		}

		if (!aopen(arecord, arecord->keys_arr->pindex_digs[arecord->keys_arr_offset])) {
			if (!pre_check(arecord, &arecord->keys_arr->sindex_keys[arecord->keys_arr_offset])) {
				aclose(arecord);
			}
		}
	}
	as_val_reserve(arecord->urec);
	return (as_val *)arecord->urec;
}

const as_stream_hooks istream_hooks = {
		.destroy	= NULL,
		.read		= istream_read,
		.write		= NULL
};
// **************************************************************************************************



/*
 * Aggregation Input Stream
 */
// **************************************************************************************************
as_stream_status
ostream_write(const as_stream *s, as_val *val)
{
	aggr_record *arecord = (aggr_record *)as_stream_source(s);
	return arecord->call->aggr_hooks->ostream_write(arecord->udata, val);
}

const as_stream_hooks ostream_hooks = {
		.destroy	= NULL,
		.read		= NULL,
		.write		= ostream_write
};
// **************************************************************************************************


/*
 * Aggregation AS_AEROSPIKE interface for LUA
 */
// **************************************************************************************************
static int
as_aggr_aerospike_log(const as_aerospike * a, const char * file, const int line, const int lvl, const char * msg)
{
	cf_fault_event(AS_AGGR, lvl, file, NULL, line, (char *) msg);
	return 0;
}

static const as_aerospike_hooks as_aggr_aerospike_hooks = {
	.open_subrec      = NULL,
	.close_subrec     = NULL,
	.update_subrec    = NULL,
	.create_subrec    = NULL,
	.rec_update       = NULL,
	.rec_remove       = NULL,
	.rec_exists       = NULL,
	.log              = as_aggr_aerospike_log,
	.get_current_time = NULL,
	.destroy          = NULL
};
// **************************************************************************************************



int 
as_aggr_process(as_namespace *ns, as_aggr_call * ag_call, cf_ll * ap_recl, void * udata, as_result * ap_res)
{
	as_index_ref    r_ref;
	r_ref.skip_lock   = false;
	as_storage_rd   rd;
	bzero(&rd, sizeof(as_storage_rd));
	as_transaction  tr;


	udf_record urecord;
	udf_record_init(&urecord, false);
	urecord.tr      = &tr;
	tr.rsv.ns       = ns;
	urecord.r_ref   = &r_ref;
	urecord.rd      = &rd;
	as_rec   * urec = as_rec_new(&urecord, &udf_record_hooks);

	aggr_record arecord = {
		.iter            = cf_ll_getIterator(ap_recl, true /*forward*/),
		.urec            = urec,
		.keys_arr        = NULL,
		.keys_arr_offset = 0,
		.call            = ag_call,
		.udata           = udata,
		.open            = false,
		.rsv             = &tr.rsv
	};

	if (!arecord.iter) {
		cf_warning (AS_AGGR, "Could not set up iterator .. possibly out of memory .. Aborting Query !!");
		return AS_AGGR_ERR;
	}

	as_aerospike as;
	as_aerospike_init(&as, NULL, &as_aggr_aerospike_hooks);

	// Input Stream
	as_stream istream;
	as_stream_init(&istream, &arecord, &istream_hooks);

	// Output stream
	as_stream ostream;
	as_stream_init(&ostream, &arecord, &ostream_hooks);

	// Argument list
	as_list arglist;
	as_list_init(&arglist, ag_call->def.arglist, &udf_arglist_hooks);

	as_udf_context ctx = {
		.as         = &as,
		.timer      = NULL,
		.memtracker = NULL
	};
	int ret = as_module_apply_stream(&mod_lua, &ctx, ag_call->def.filename, ag_call->def.function, &istream, &arglist, &ostream, ap_res);

	udf_memtracker_cleanup();

	as_list_destroy(&arglist);

	acleanup(&arecord);
	return ret;
}
