/*
 * truncate.c
 *
 * Copyright (C) 2017 Aerospike, Inc.
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

#include "base/truncate.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/system_metadata.h"

// FIXME - new fault context?


//==========================================================
// Typedefs & constants.
//

typedef struct truncate_reduce_cb_info_s {
	as_namespace* ns;
	as_index_tree* tree;
	int64_t n_deleted;
} truncate_reduce_cb_info;

static const uint32_t NUM_TRUNCATE_THREADS = 4;

// Truncate system metadata module name.
const char AS_TRUNCATE_MODULE[] = "truncate";
#define TRUNCATE_MODULE ((char*)AS_TRUNCATE_MODULE)
// TODO - change smd API to take const char* module names?

// Includes 1 for delimiter and 1 for null-terminator.
#define TRUNCATE_KEY_SIZE (AS_ID_NAMESPACE_SZ + AS_SET_NAME_MAX_SIZE)

// System metadata key format token.
#define TOK_DELIMITER ('|')


//==========================================================
// Globals.
//

static bool g_truncate_smd_loaded = false;
static shash* g_truncate_filter_hash = NULL;


//==========================================================
// Forward declarations & inlines.
//

as_set* as_namespace_get_set_by_name(as_namespace* ns, const char* set_name);
as_set* as_namespace_get_set_by_id(as_namespace* ns, uint16_t set_id);
int startup_set_hash_reduce_cb(void* key, void* data, void* udata);
bool filter_hash_put(const as_smd_item_t* item);
void filter_hash_delete(const as_smd_item_t* item);

bool truncate_smd_conflict_cb(char* module, as_smd_item_t* existing_item, as_smd_item_t* new_item, void* udata);
int truncate_smd_accept_cb(char* module, as_smd_item_list_t* items, void* udata, uint32_t accept_opt);
int truncate_smd_can_accept_cb(char* module, as_smd_item_t *item, void *udata);

void action_startup(as_namespace* ns, const char* set_name, uint64_t lut);
void action_truncate(as_namespace* ns, const char* set_name, uint64_t lut);
void action_undo(as_namespace* ns, const char* set_name);
void truncate_all(as_namespace* ns);
void* run_truncate(void* arg);
void truncate_finish(as_namespace* ns);
void truncate_reduce_cb(as_index_ref* r_ref, void* udata);

// TODO - make this a generic utility in namespace.c?
static inline as_set*
as_namespace_get_record_set(as_namespace* ns, const as_record* r)
{
	return as_namespace_get_set_by_id(ns, as_index_get_set_id(r));
}

static inline uint64_t
lut_from_smd(const as_smd_item_t* item)
{
	return strtoul(item->value, NULL, 16);
}

// TODO - promote to util when shash is cleaned up. (See also SMD.)
static inline uint32_t
fno_hash_fn(void* value, uint32_t size)
{
	uint32_t hash = 2166136261;

	while (size--) {
		hash ^= *(uint8_t*)value++;
		hash *= 16777619;
	}

	return hash;
}

// TODO - make generic string hash and promote to util?
static inline uint32_t
truncate_hash_fn(void* key)
{
	return fno_hash_fn(key, (uint32_t)strlen((const char*)key));
}


//==========================================================
// Public API.
//

void
as_truncate_init(as_namespace* ns)
{
	// Create the shash used at startup. (Will be destroyed after startup.)
	if (shash_create(&ns->truncate.startup_set_hash, truncate_hash_fn,
			AS_SET_NAME_MAX_SIZE, sizeof(uint64_t), 1024, 0) != SHASH_OK) {
		cf_crash(AS_TRUNCATE, "truncate init - failed startup-set-hash create");
	}

	ns->truncate.state = TRUNCATE_IDLE;
	pthread_mutex_init(&ns->truncate.state_lock, 0);

	// Lazily create the global filter shash used on the SMD principal.
	if (! g_truncate_filter_hash &&
			shash_create(&g_truncate_filter_hash, truncate_hash_fn,
					TRUNCATE_KEY_SIZE, sizeof(uint64_t),
					1024 * g_config.n_namespaces, 0) != SHASH_OK) {
		cf_crash(AS_TRUNCATE, "truncate init - failed filter-hash create");
	}

	// Register the system metadata custom callbacks.
	if (as_smd_create_module(TRUNCATE_MODULE,
			NULL, NULL,
			truncate_smd_conflict_cb, NULL,
			truncate_smd_accept_cb, NULL,
			truncate_smd_can_accept_cb, NULL) != 0) {
		cf_crash(AS_TRUNCATE, "truncate init - failed smd create module");
	}
}


void
as_truncate_done_startup(as_namespace* ns)
{
	shash_reduce(ns->truncate.startup_set_hash, startup_set_hash_reduce_cb, ns);
	shash_destroy(ns->truncate.startup_set_hash);
}


// SMD key is "ns-name|set-name" or "ns-name".
// SMD value is last-update-time as hex string.
bool
as_truncate_cmd(const char* ns_name, const char* set_name, const char* lut_str)
{
	char smd_key[TRUNCATE_KEY_SIZE];

	strcpy(smd_key, ns_name);

	if (set_name) {
		char* p_write = smd_key + strlen(ns_name);

		*p_write++ = TOK_DELIMITER;
		strcpy(p_write, set_name);
	}

	uint64_t lut;

	if (lut_str) {
		uint64_t utc_nanosec = strtoul(lut_str, NULL, 0);

		// Last update time as human-readable UTC seconds.
		// TODO - make generic utility?
		char utc_sec[64] = { 0 };
		time_t utc_time = utc_nanosec / 1000000000;
		struct tm utc_tm;

		if (cf_fault_is_using_local_time()) {
			localtime_r(&utc_time, &utc_tm);
			strftime(utc_sec, sizeof(utc_sec), "%b %d %Y %T GMT%z", &utc_tm);
		}
		else {
			gmtime_r(&utc_time, &utc_tm);
			strftime(utc_sec, sizeof(utc_sec), "%b %d %Y %T %Z", &utc_tm);
		}

		lut = cf_clepoch_ms_from_utc_ns(utc_nanosec);

		if (lut == 0) {
			cf_warning(AS_TRUNCATE, "command lut %s (%s) would truncate to 0",
					lut_str, utc_sec);
			return false;
		}

		// FIXME - enforce future bound sanity check ???

		cf_info(AS_TRUNCATE, "{%s} got command to truncate to %s (%lx)",
				smd_key, utc_sec, lut);
	}
	else {
		// Use a last-update-time threshold of now.
		lut = cf_clepoch_milliseconds();

		cf_info(AS_TRUNCATE, "{%s} got command to truncate to now (%lx)",
				smd_key, lut);
	}

	char smd_value[16 + 1]; // uint64_t takes at most 16 hex characters

	sprintf(smd_value, "%lx", lut);

	// Broadcast the truncate command to all nodes (including this one).
	as_smd_set_metadata(TRUNCATE_MODULE, smd_key, smd_value);

	return true;
}


// SMD key is "ns-name|set-name" or "ns-name".
void
as_truncate_undo_cmd(const char* ns_name, const char* set_name)
{
	char smd_key[TRUNCATE_KEY_SIZE];

	strcpy(smd_key, ns_name);

	if (set_name) {
		char* p_write = smd_key + strlen(ns_name);

		*p_write++ = TOK_DELIMITER;
		strcpy(p_write, set_name);
	}

	cf_info(AS_TRUNCATE, "{%s} got command to undo truncate", smd_key);

	// Broadcast the truncate-undo command to all nodes (including this one).
	as_smd_delete_metadata(TRUNCATE_MODULE, smd_key);
}


bool
as_truncate_now_is_truncated(struct as_namespace_s* ns, uint16_t set_id)
{
	uint64_t now = cf_clepoch_milliseconds();

	if (now < ns->truncate.lut) {
		return true;
	}

	as_set* p_set = as_namespace_get_set_by_id(ns, set_id);

	return p_set ? now < p_set->truncate_lut : false;
}


bool
as_truncate_record_is_truncated(const as_record* r, as_namespace* ns)
{
	if (r->last_update_time < ns->truncate.lut) {
		return true;
	}

	as_set* p_set = as_namespace_get_record_set(ns, r);

	return p_set ? r->last_update_time < p_set->truncate_lut : false;
}


//==========================================================
// Local helpers - generic.
//

// TODO - make this a generic utility in namespace.c?
as_set*
as_namespace_get_set_by_name(as_namespace* ns, const char* set_name)
{
	uint32_t idx;

	if (cf_vmapx_get_index(ns->p_sets_vmap, set_name, &idx) != CF_VMAPX_OK) {
		return NULL;
	}

	as_set* p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, idx, (void**)&p_set) !=
			CF_VMAPX_OK) {
		// Should be impossible - just verified idx.
		cf_crash(AS_TRUNCATE, "unexpected vmap error");
	}

	return p_set;
}


// TODO - make this a generic utility in namespace.c?
as_set*
as_namespace_get_set_by_id(as_namespace* ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return NULL;
	}

	as_set* p_set;

	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_id - 1, (void**)&p_set) !=
			CF_VMAPX_OK) {
		// Should be impossible.
		cf_warning(AS_TRUNCATE, "unexpected - record with set-id not in vmap");
		return NULL;
	}

	return p_set;
}


int
startup_set_hash_reduce_cb(void* key, void* data, void* udata)
{
	as_namespace* ns = (as_namespace*)udata;
	const char* set_name = (const char*)key;
	uint64_t set_lut = *(uint64_t*)data;

	as_set* p_set = as_namespace_get_set_by_name(ns, set_name);

	if (! p_set) {
		cf_detail(AS_TRUNCATE, "{%s|%s} set tombstone found for empty set",
				ns->name, set_name);
		return SHASH_OK;
	}

	// Transfer the last-update-time from the hash to the vmap.
	p_set->truncate_lut = set_lut;

	return SHASH_OK;
}


bool
filter_hash_put(const as_smd_item_t* item)
{
	uint64_t new_lut = lut_from_smd(item);
	uint64_t ex_lut;
	char hkey[TRUNCATE_KEY_SIZE] = { 0 }; // pad for consistent shash key

	strcpy(hkey, item->key);

	if (shash_get(g_truncate_filter_hash, hkey, &ex_lut) != SHASH_OK ||
			new_lut > ex_lut) {
		if (shash_put(g_truncate_filter_hash, hkey, &new_lut) != SHASH_OK) {
			cf_warning(AS_TRUNCATE, "{%s} failed filter-hash put", item->key);
		}

		return true;
	}

	// This is normal on principal, from truncate_smd_accept_cb().
	cf_detail(AS_TRUNCATE, "{%s} ignoring truncate lut %lx <= %lx", item->key,
			new_lut, ex_lut);

	return false;
}


void
filter_hash_delete(const as_smd_item_t* item)
{
	char hkey[TRUNCATE_KEY_SIZE] = { 0 }; // pad for consistent shash key

	strcpy(hkey, item->key);

	if (shash_delete(g_truncate_filter_hash, hkey) != SHASH_OK) {
		cf_warning(AS_TRUNCATE, "{%s} failed filter-hash delete", item->key);
	}
}


//==========================================================
// Local helpers - SMD callbacks.
//

bool
truncate_smd_conflict_cb(char* module, as_smd_item_t* existing_item,
		as_smd_item_t* new_item, void* udata)
{
	return lut_from_smd(existing_item) >= lut_from_smd(new_item);
}


int
truncate_smd_accept_cb(char* module, as_smd_item_list_t* items, void* udata,
		uint32_t accept_opt)
{
	if ((accept_opt & AS_SMD_ACCEPT_OPT_CREATE) != 0) {
		g_truncate_smd_loaded = true;
		return 0;
	}

	for (int i = 0; i < (int)items->num_items; i++) {
		as_smd_item_t* item = items->item[i];

		if (item->action == AS_SMD_ACTION_SET) {
			// Ignore result - SMD principal's hash will already have this item.
			filter_hash_put(item);
		}
		else if (item->action == AS_SMD_ACTION_DELETE) {
			filter_hash_delete(item);
		}
		else {
			cf_warning(AS_TRUNCATE, "smd accept cb - unknown action");
			continue;
		}

		const char* ns_name = item->key;
		const char* tok = strchr(ns_name, TOK_DELIMITER);

		uint32_t ns_len = tok ? (uint32_t)(tok - ns_name) : strlen(ns_name);
		as_namespace* ns = as_namespace_get_bybuf((uint8_t*)ns_name, ns_len);

		if (! ns) {
			cf_detail(AS_TRUNCATE, "skipping invalid ns");
			continue;
		}

		const char* set_name = tok ? tok + 1 : NULL;

		if (item->action == AS_SMD_ACTION_SET) {
			uint64_t lut = lut_from_smd(item);

			if (g_truncate_smd_loaded) {
				action_truncate(ns, set_name, lut);
			}
			else {
				action_startup(ns, set_name, lut);
			}
		}
		else {
			action_undo(ns, set_name);
		}
	}

	return 0;
}


int
truncate_smd_can_accept_cb(char* module, as_smd_item_t* item, void* udata)
{
	if (item->action == AS_SMD_ACTION_SET) {
		return filter_hash_put(item) ? 0 : -1;
	}
	else if (item->action == AS_SMD_ACTION_DELETE) {
		return 0;
	}
	else {
		cf_warning(AS_TRUNCATE, "smd can accept cb - unknown action");
		return -1;
	}
}


//==========================================================
// Local helpers - SMD callbacks' helpers.
//

void
action_startup(as_namespace* ns, const char* set_name, uint64_t lut)
{
	if (! set_name) {
		ns->truncate.lut = lut;
		return;
	}

	char hkey[AS_SET_NAME_MAX_SIZE] = { 0 }; // pad for consistent shash key

	strcpy(hkey, set_name);

	if (shash_put_unique(ns->truncate.startup_set_hash, hkey, &lut) !=
			SHASH_OK) {
		cf_crash(AS_TRUNCATE, "{%s|%s} failed startup-hash put", ns->name,
				set_name);
	}
}


void
action_truncate(as_namespace* ns, const char* set_name, uint64_t lut)
{
	if (set_name) {
		as_set* p_set = as_namespace_get_set_by_name(ns, set_name);

		if (! p_set) {
			cf_detail(AS_TRUNCATE, "{%s|%s} truncate for nonexistent set",
					ns->name, set_name);
			return;
		}

		if (lut <= p_set->truncate_lut) {
			cf_detail(AS_TRUNCATE, "{%s|%s} truncate lut %lx <= vmap lut %lx",
					ns->name, set_name, lut, p_set->truncate_lut);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s|%s} truncating to %lx", ns->name, set_name,
				lut);

		p_set->truncate_lut = lut;
	}
	else {
		if (lut <= ns->truncate.lut) {
			cf_detail(AS_TRUNCATE, "{%s} truncate lut %lx <= ns lut %lx",
					ns->name, lut, ns->truncate.lut);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s} truncating to %lx", ns->name, lut);

		ns->truncate.lut = lut;
	}

	// Truncate to new last-update-time.

	pthread_mutex_lock(&ns->truncate.state_lock);

	switch (ns->truncate.state) {
	case TRUNCATE_IDLE:
		cf_info(AS_TRUNCATE, "{%s} starting truncate", ns->name);
		truncate_all(ns);
		break;
	case TRUNCATE_RUNNING:
		cf_info(AS_TRUNCATE, "{%s} flagging truncate to restart", ns->name);
		ns->truncate.state = TRUNCATE_RESTART;
		break;
	case TRUNCATE_RESTART:
		cf_info(AS_TRUNCATE, "{%s} truncate already will restart", ns->name);
		break;
	default:
		cf_crash(AS_TRUNCATE, "bad truncate state %d", ns->truncate.state);
		break;
	}

	pthread_mutex_unlock(&ns->truncate.state_lock);
}


void
action_undo(as_namespace* ns, const char* set_name)
{
	if (set_name) {
		as_set* p_set = as_namespace_get_set_by_name(ns, set_name);

		if (! p_set) {
			cf_detail(AS_TRUNCATE, "{%s|%s} undo truncate for nonexistent set",
					ns->name, set_name);
			return;
		}

		cf_info(AS_TRUNCATE, "{%s|%s} undoing truncate to %lx", ns->name,
				set_name, p_set->truncate_lut);

		p_set->truncate_lut = 0;
	}
	else {
		cf_info(AS_TRUNCATE, "{%s} undoing truncate to %lx", ns->name,
				ns->truncate.lut);

		ns->truncate.lut = 0;
	}
}


// Called under truncate lock.
void
truncate_all(as_namespace* ns)
{
	// TODO - skipping sindex deletion shortcut - can't do that if we want to
	// keep writing through set truncates. Is this ok?

	ns->truncate.state = TRUNCATE_RUNNING;
	cf_atomic32_set(&ns->truncate.n_threads_running, NUM_TRUNCATE_THREADS);
	cf_atomic32_set(&ns->truncate.pid, -1);

	cf_atomic64_set(&ns->truncate.n_records_this_run, 0);

	pthread_t thread;
	pthread_attr_t attrs;

	pthread_attr_init(&attrs);
	pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_DETACHED);

	for (uint32_t i = 0; i < NUM_TRUNCATE_THREADS; i++) {
		if (pthread_create(&thread, &attrs, run_truncate, (void*)ns) != 0) {
			cf_crash(AS_TRUNCATE, "failed to create truncate thread");
			// TODO - be forgiving? Is there any point?
		}
	}
}


void*
run_truncate(void* arg)
{
	as_namespace* ns = (as_namespace*)arg;
	uint32_t pid;

	while ((pid = (uint32_t)cf_atomic32_incr(&ns->truncate.pid)) <
			AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve_migrate(ns, pid, &rsv, NULL);

		truncate_reduce_cb_info cb_info = { .ns = ns, .tree = rsv.tree };

		as_index_reduce(rsv.tree, truncate_reduce_cb, (void*)&cb_info);
		as_partition_release(&rsv);

		cf_atomic64_add(&ns->truncate.n_records_this_run, cb_info.n_deleted);
	}

	truncate_finish(ns);

	return NULL;
}


void
truncate_finish(as_namespace* ns)
{
	if (cf_atomic32_decr(&ns->truncate.n_threads_running) == 0) {
		pthread_mutex_lock(&ns->truncate.state_lock);

		ns->truncate.n_records += ns->truncate.n_records_this_run;

		cf_info(AS_TRUNCATE, "{%s} truncated records (%lu,%lu)", ns->name,
				ns->truncate.n_records_this_run, ns->truncate.n_records);

		switch (ns->truncate.state) {
		case TRUNCATE_RUNNING:
			cf_info(AS_TRUNCATE, "{%s} done truncate", ns->name);
			ns->truncate.state = TRUNCATE_IDLE;
			break;
		case TRUNCATE_RESTART:
			cf_info(AS_TRUNCATE, "{%s} restarting truncate", ns->name);
			truncate_all(ns);
			break;
		case TRUNCATE_IDLE:
		default:
			cf_crash(AS_TRUNCATE, "bad truncate state %d", ns->truncate.state);
			break;
		}

		pthread_mutex_unlock(&ns->truncate.state_lock);
	}
}


void
truncate_reduce_cb(as_index_ref* r_ref, void* udata)
{
	as_record* r = r_ref->r;
	truncate_reduce_cb_info* cb_info = (truncate_reduce_cb_info*)udata;
	as_namespace* ns = cb_info->ns;

	if (r->last_update_time < ns->truncate.lut) {
		cb_info->n_deleted++;
		as_index_delete(cb_info->tree, &r->key);
		as_record_done(r_ref, ns);
		return;
	}

	as_set* p_set = as_namespace_get_record_set(ns, r);

	// Delete records not updated since their set's threshold last-update-time.
	if (p_set && r->last_update_time < p_set->truncate_lut) {
		cb_info->n_deleted++;
		as_index_delete(cb_info->tree, &r->key);
	}

	as_record_done(r_ref, ns);
}
