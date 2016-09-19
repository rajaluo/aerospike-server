/*
 * partition.c
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include "fabric/partition.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_b64.h"

#include "fault.h"
#include "util.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "storage/storage.h"
#include "fabric/partition_balance.h"


//==========================================================
// Constants and typedefs.
//

const as_partition_vinfo NULL_VINFO = { 0 };


//==========================================================
// Globals.
//

static cf_atomic32 g_partition_check_counter = 0;


//==========================================================
// Forward declarations.
//

cf_node find_best_node(const as_partition* p, const as_namespace* ns, bool is_read);
int find_in_replica_list(const as_partition* p, cf_node node);
void partition_health_check(const as_partition* p, const as_namespace* ns, int self_n);
int partition_reserve_read_write(as_namespace* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, bool is_read, uint64_t* cluster_key);
void partition_reserve_lockfree(as_namespace* ns, uint32_t pid, as_partition_reservation* rsv);
cf_node partition_getreplica_prole(as_namespace* ns, uint32_t pid);
char partition_getstate_str(int state);
int partition_get_replica_self_lockfree(const as_namespace* ns, uint32_t pid);


//==========================================================
// Public API.
//

void
as_partition_init(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	// Note - as_partition has been zeroed since it's a member of as_namespace.
	// Set non-zero members.

	pthread_mutex_init(&p->lock, NULL);

	p->id = pid;
	p->state = AS_PARTITION_STATE_ABSENT;

	if (ns->cold_start) {
		p->vp = as_index_tree_create(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				ns->tree_roots ? &ns->tree_roots[pid] : NULL);

		p->sub_vp = as_index_tree_create(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				ns->sub_tree_roots ? &ns->sub_tree_roots[pid] : NULL);
	}
	else {
		p->vp = as_index_tree_resume(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				&ns->tree_roots[pid]);

		p->sub_vp = as_index_tree_resume(ns->arena,
				(as_index_value_destructor)&as_record_destroy, ns,
				&ns->sub_tree_roots[pid]);
	}
}


// Return number of partitions found in storage.
int
as_partition_get_state_from_storage(as_namespace* ns, bool* partition_states)
{
	memset(partition_states, 0, sizeof(bool) * AS_PARTITIONS);

	int n_found = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_vinfo vinfo;

		// Find if the value has been set in storage.
		as_storage_info_get(ns, pid, &vinfo);

		if (! as_partition_is_null(&vinfo)) {
			partition_states[pid] = true;
			n_found++;
		}
	}

	return n_found;
}


// Get a list of all nodes (excluding self) that are replicas for a specified
// partition: place the list in *nv and return the number of nodes found.
uint32_t
as_partition_get_other_replicas(as_partition* p, cf_node* nv)
{
	uint32_t n_other_replicas = 0;

	pthread_mutex_lock(&p->lock);

	for (uint32_t repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
		// Don't ever include yourself.
		if (p->replicas[repl_ix] == g_config.self_node) {
			continue;
		}

		// Copy the node ID into the user-supplied vector.
		nv[n_other_replicas++] = p->replicas[repl_ix];
	}

	pthread_mutex_unlock(&p->lock);

	return n_other_replicas;
}


cf_node
as_partition_writable_node(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	cf_node best_node = find_best_node(p, ns, false);

	pthread_mutex_unlock(&p->lock);

	return best_node;
}


// If this node is an eventual master, return the acting master, else return 0.
cf_node
as_partition_proxyee_redirect(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];
	cf_node self = g_config.self_node;

	pthread_mutex_lock(&p->lock);

	bool is_final_master = find_in_replica_list(p, self) == 0;
	bool is_desync = p->state == AS_PARTITION_STATE_DESYNC;
	cf_node acting_master = p->origin;

	pthread_mutex_unlock(&p->lock);

	return is_final_master && is_desync ? acting_master : (cf_node)0;
}


// TODO - deprecate in "six months".
void
as_partition_get_replicas_prole_str(cf_dyn_buf* db)
{
	uint8_t prole_bitmap[CLIENT_BITMAP_BYTES];
	char b64_bitmap[CLIENT_B64MAP_BYTES];

	size_t db_sz = db->used_sz;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		memset(prole_bitmap, 0, sizeof(uint8_t) * CLIENT_BITMAP_BYTES);
		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			if (g_config.self_node == partition_getreplica_prole(ns, pid) ) {
				prole_bitmap[pid >> 3] |= (0x80 >> (pid & 7));
			}
		}

		cf_b64_encode(prole_bitmap, CLIENT_BITMAP_BYTES, b64_bitmap);
		cf_dyn_buf_append_buf(db, (uint8_t*)b64_bitmap, CLIENT_B64MAP_BYTES);
		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


void
as_partition_get_replicas_master_str(cf_dyn_buf* db)
{
	size_t db_sz = db->used_sz;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');
		cf_dyn_buf_append_buf(db, (uint8_t*)ns->replica_maps[0].b64map,
				sizeof(ns->replica_maps[0].b64map));
		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


void
as_partition_get_replicas_all_str(cf_dyn_buf* db)
{
	size_t db_sz = db->used_sz;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		cf_dyn_buf_append_string(db, ns->name);
		cf_dyn_buf_append_char(db, ':');

		uint32_t repl_factor = ns->replication_factor;

		cf_dyn_buf_append_uint32(db, repl_factor);

		for (uint32_t repl_ix = 0; repl_ix < repl_factor; repl_ix++) {
			cf_dyn_buf_append_char(db, ',');
			cf_dyn_buf_append_buf(db,
					(uint8_t*)&ns->replica_maps[repl_ix].b64map,
					sizeof(ns->replica_maps[repl_ix].b64map));
		}

		cf_dyn_buf_append_char(db, ';');
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db);
	}
}


void
as_partition_get_master_prole_stats(as_namespace* ns, repl_stats* p_stats)
{
	p_stats->n_master_objects = 0;
	p_stats->n_prole_objects = 0;
	p_stats->n_master_sub_objects = 0;
	p_stats->n_prole_sub_objects = 0;
	p_stats->n_master_tombstones = 0;
	p_stats->n_prole_tombstones = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition* p = &ns->partitions[pid];

		pthread_mutex_lock(&p->lock);

		int self_n = find_in_replica_list(p, g_config.self_node); // -1 if not
		bool am_master = (self_n == 0 && p->state == AS_PARTITION_STATE_SYNC) ||
				p->target != (cf_node)0;

		if (am_master) {
			int64_t n_tombstones = (int64_t)p->n_tombstones;
			int64_t n_objects = (int64_t)p->vp->elements - n_tombstones;

			p_stats->n_master_objects += n_objects > 0 ?
					(uint64_t)n_objects : 0;
			p_stats->n_master_sub_objects += p->sub_vp->elements;
			p_stats->n_master_tombstones += (uint64_t)n_tombstones;
		}
		else if (self_n > 0 && p->origin == 0) {
			int64_t n_tombstones = (int64_t)p->n_tombstones;
			int64_t n_objects = (int64_t)p->vp->elements - n_tombstones;

			p_stats->n_prole_objects += n_objects > 0 ? (uint64_t)n_objects : 0;
			p_stats->n_prole_sub_objects += p->sub_vp->elements;
			p_stats->n_prole_tombstones += (uint64_t)n_tombstones;
		}

		pthread_mutex_unlock(&p->lock);
	}
}


int
as_partition_reserve_write(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* node, uint64_t* cluster_key)
{
	return partition_reserve_read_write(ns, pid, rsv, node, false, cluster_key);
}


int
as_partition_reserve_read(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* node, uint64_t* cluster_key)
{
	return partition_reserve_read_write(ns, pid, rsv, node, true, cluster_key);
}


void
as_partition_reserve_migrate(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* node)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	partition_reserve_lockfree(ns, pid, rsv);

	pthread_mutex_unlock(&p->lock);

	if (node) {
		*node = g_config.self_node;
	}
}


int
as_partition_reserve_migrate_timeout(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* node, int timeout_ms)
{
	as_partition* p = &ns->partitions[pid];

	struct timespec tp;
	cf_set_wait_timespec(timeout_ms, &tp);

	if (0 != pthread_mutex_timedlock(&p->lock, &tp)) {
		return -1;
	}

	partition_reserve_lockfree(ns, pid, rsv);

	pthread_mutex_unlock(&p->lock);

	if (node) {
		*node = g_config.self_node;
	}

	return 0;
}


// Reserves all query-able partitions.
// Returns the number of partitions reserved.
int
as_partition_prereserve_query(as_namespace* ns, bool can_partition_query[],
		as_partition_reservation rsv[])
{
	int reserved = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		if (as_partition_reserve_query(ns, pid, &rsv[pid])) {
			can_partition_query[pid] = false;
		}
		else {
			can_partition_query[pid] = true;
			reserved++;
		}
	}

	return reserved;
}


// Reserve a partition for query.
// Return value 0 means the reservation was taken, -1 means not.
int
as_partition_reserve_query(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	return as_partition_reserve_write(ns, pid, rsv, NULL, NULL);
}


// Obtain a partition reservation for XDR reads. Succeeds, if we are sync or
// zombie for the partition.
int
as_partition_reserve_xdr_read(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	int res;

	if (p->state == AS_PARTITION_STATE_SYNC ||
			p->state == AS_PARTITION_STATE_ZOMBIE) {
		partition_reserve_lockfree(ns, pid, rsv);
		res = 0;
	}
	else {
		res = -1;
	}

	pthread_mutex_unlock(&p->lock);

	return res;
}


void
as_partition_reservation_copy(as_partition_reservation* dst,
		as_partition_reservation* src)
{
	dst->ns = src->ns;
	dst->p = src->p;
	dst->tree = src->tree;
	dst->sub_tree = src->sub_tree;
	dst->cluster_key = src->cluster_key;
	dst->state = src->state;
	dst->n_dupl = src->n_dupl;

	if (dst->n_dupl != 0) {
		memcpy(dst->dupl_nodes, src->dupl_nodes, sizeof(cf_node) * dst->n_dupl);
	}
}


void
as_partition_release(as_partition_reservation* rsv)
{
	as_index_tree_release(rsv->tree);
	as_index_tree_release(rsv->sub_tree);
}


void
as_partition_getinfo_str(cf_dyn_buf* db)
{
	size_t db_sz = db->used_sz;

	cf_dyn_buf_append_string(db, "namespace:partition:state:n_dupl:replica:"
			"origin:target:emigrates:immigrates:records:sub_records:tombstones:"
			"ldt_version:version;");

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			pthread_mutex_lock(&p->lock);

			char state_c = partition_getstate_str(p->state);

			// Find myself in the replica list.
			uint32_t repl_ix;

			for (repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
				if (p->replicas[repl_ix] == g_config.self_node) {
					break;
				}
			}

			cf_dyn_buf_append_string(db, ns->name);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_int(db, pid);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_char(db, state_c);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->n_dupl);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, repl_ix);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->origin);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->target);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->pending_emigrations);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64_x(db, p->pending_immigrations);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->vp->elements);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint32(db, p->sub_vp->elements);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, p->n_tombstones);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, p->current_outgoing_ldt_version);
			cf_dyn_buf_append_char(db, ':');
			cf_dyn_buf_append_uint64(db, p->version_info.iid);
			cf_dyn_buf_append_char(db, '-');
			cf_dyn_buf_append_uint64(db, p->version_info.vtp[0]);
			cf_dyn_buf_append_char(db, '-');
			cf_dyn_buf_append_uint64(db, p->version_info.vtp[8]);
			cf_dyn_buf_append_char(db, ';');

			pthread_mutex_unlock(&p->lock);
		}
	}

	if (db_sz != db->used_sz) {
		cf_dyn_buf_chomp(db); // take back the final ';'
	}
}


//==========================================================
// Public API - client view replica maps.
//

void
client_replica_maps_create(as_namespace* ns)
{
	uint32_t size = sizeof(client_replica_map) * ns->cfg_replication_factor;

	ns->replica_maps = cf_malloc(size);
	memset(ns->replica_maps, 0, size);

	for (uint32_t repl_ix = 0; repl_ix < ns->cfg_replication_factor;
			repl_ix++) {
		client_replica_map* repl_map = &ns->replica_maps[repl_ix];

		pthread_mutex_init(&repl_map->write_lock, NULL);

		cf_b64_encode((uint8_t*)repl_map->bitmap,
				(uint32_t)sizeof(repl_map->bitmap), (char*)repl_map->b64map);
	}
}


bool
client_replica_maps_update(as_namespace* ns, uint32_t pid)
{
	uint32_t byte_i = pid >> 3;
	uint32_t byte_chunk = (byte_i / 3);
	uint32_t chunk_bitmap_offset = byte_chunk * 3;
	uint32_t chunk_b64map_offset = byte_chunk << 2;

	uint32_t bytes_from_end = CLIENT_BITMAP_BYTES - chunk_bitmap_offset;
	uint32_t input_size = bytes_from_end > 3 ? 3 : bytes_from_end;

	int replica = partition_get_replica_self_lockfree(ns, pid); // -1 if not
	uint8_t set_mask = 0x80 >> (pid & 0x7);
	bool changed = false;

	for (int repl_ix = 0; repl_ix < (int)ns->cfg_replication_factor;
			repl_ix++) {
		client_replica_map* repl_map = &ns->replica_maps[repl_ix];

		volatile uint8_t* mbyte = repl_map->bitmap + byte_i;
		bool owned = replica == repl_ix;
		bool is_set = (*mbyte & set_mask) != 0;
		bool needs_update = (owned && ! is_set) || (! owned && is_set);

		if (! needs_update) {
			continue;
		}

		volatile uint8_t* bitmap_chunk = repl_map->bitmap + chunk_bitmap_offset;
		volatile char* b64map_chunk = repl_map->b64map + chunk_b64map_offset;

		pthread_mutex_lock(&repl_map->write_lock);

		*mbyte ^= set_mask;
		cf_b64_encode((uint8_t*)bitmap_chunk, input_size, (char*)b64map_chunk);

		pthread_mutex_unlock(&repl_map->write_lock);

		changed = true;
	}

	return changed;
}


bool
client_replica_maps_is_partition_queryable(const as_namespace* ns, uint32_t pid)
{
	uint32_t byte_i = pid >> 3;

	const client_replica_map* repl_map = ns->replica_maps;
	const volatile uint8_t* mbyte = repl_map->bitmap + byte_i;

	uint8_t set_mask = 0x80 >> (pid & 0x7);

	return (*mbyte & set_mask) != 0;
}


//==========================================================
// Local helpers.
//

// Find best node to handle read/write. Called within partition lock.
cf_node
find_best_node(const as_partition* p, const as_namespace* ns, bool is_read)
{
	// Find location of self in replica list, returns -1 if not found.
	int self_n = find_in_replica_list(p, g_config.self_node);

	// Do health check occasionally (expensive to do for every read/write).
	if ((cf_atomic32_incr(&g_partition_check_counter) & 0x0FFF) == 0) {
		partition_health_check(p, ns, self_n);
	}

	// Find an appropriate copy of this partition.
	//
	// Return this node if:
	// - node is final working master
	// - node is acting master
	// Return origin node (acting master) if:
	// - node is eventual (final non-working) master
	// Return this node if:
	// - it's a read, node is replica, and has no origin
	// Otherwise, return final master.

	bool is_sync = p->state == AS_PARTITION_STATE_SYNC;
	bool is_desync = p->state == AS_PARTITION_STATE_DESYNC;
	bool is_final_master = self_n == 0;
	bool is_prole = self_n > 0 && self_n < p->n_replicas;
	bool acting_master = p->target != 0;

	cf_node best_node = (cf_node)0;

	if ((is_final_master && is_sync) || acting_master) {
		best_node = g_config.self_node;
	}
	else if (is_final_master && is_desync) {
		best_node = p->origin;
	}
	else if (is_read && is_prole && p->origin == (cf_node)0) {
		best_node = g_config.self_node;
	}
	else {
		best_node = p->replicas[0];
	}

	if (best_node == (cf_node)0 && as_partition_balance_is_init_resolved()) {
		cf_warning(AS_PARTITION, "{%s:%u} could not find sync copy, my index %d master %lu replica %lu origin %lu",
				ns->name, p->id, self_n, p->replicas[0], p->replicas[1],
				p->origin);
	}

	return best_node;
}


int
find_in_replica_list(const as_partition* p, cf_node node)
{
	for (uint32_t repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
		if (p->replicas[repl_ix] == node) {
			return repl_ix;
		}
	}

	return -1;
}


void
partition_health_check(const as_partition* p, const as_namespace* ns,
		int self_n)
{
	uint32_t pid = p->id;
	const as_partition_vinfo* pvinfo = &ns->partitions[pid].version_info;

	bool is_sync = p->state == AS_PARTITION_STATE_SYNC;
	bool is_desync = p->state == AS_PARTITION_STATE_DESYNC;
	bool is_zombie = p->state == AS_PARTITION_STATE_ZOMBIE;
	bool is_master = self_n == 0;
	bool is_replica = self_n > 0 && self_n < p->n_replicas;
	bool is_primary = as_partition_vinfo_same(pvinfo, &p->primary_version_info);
	bool migrating_to_master = p->target != (cf_node)0;

	// State consistency checks.
	if (migrating_to_master) {
		if (p->target != p->replicas[0]) {
			cf_warning(AS_PARTITION, "{%s:%u} partition state error on write reservation - target of migration not master node",
					ns->name, pid);
		}

		if (! ((is_zombie && is_primary) ||
				(is_replica && is_sync && is_primary))) {
			cf_warning(AS_PARTITION, "{%s:%u} partition state error on write reservation - illegal state in node migrating to master",
					ns->name, pid);
		}
	}

	if (((is_replica && is_desync) || (is_replica && is_sync && ! is_primary))
			&& p->origin != p->replicas[0]) {
		cf_warning(AS_PARTITION, "{%s:%u} partition state error on write reservation - origin does not match master",
				ns->name, pid);
	}
	else if (is_replica && is_sync && is_primary && ! migrating_to_master
			&& p->origin && p->origin != p->replicas[0]) {
		cf_warning(AS_PARTITION, "{%s:%u} partition state error on write reservation - replica sync node's origin does not match master",
				ns->name, pid);
	}
	else if (is_master && is_desync && p->origin == (cf_node)0) {
		cf_warning(AS_PARTITION, "{%s:%u} partition state error on write reservation - origin node is null for non-sync master",
				ns->name, pid);
	}

	for (uint32_t n = 0; n < p->n_replicas; n++) {
		if (p->replicas[n] == (cf_node)0
				&& as_partition_balance_is_init_resolved()) {
			cf_warning(AS_PARTITION, "{%s:%u} detected state error - replica list contains null node at position %u",
					ns->name, pid, n);
		}
	}

	for (uint32_t n = p->n_replicas; n < AS_CLUSTER_SZ; n++) {
		if (p->replicas[n] != (cf_node)0) {
			cf_warning(AS_PARTITION, "{%s:%u} detected state error - replica list contains non null node %lu at position %u",
					ns->name, pid, p->replicas[n], n);
		}
	}
}


int
partition_reserve_read_write(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* node, bool is_read,
		uint64_t* cluster_key)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	cf_node best_node = find_best_node(p, ns, is_read);

	if (node) {
		*node = best_node;
	}

	if (cluster_key) {
		*cluster_key = p->cluster_key;
	}

	// If this node is not the appropriate one, return.
	if (best_node != g_config.self_node) {
		pthread_mutex_unlock(&p->lock);
		return -1;
	}

	if (p->state != AS_PARTITION_STATE_SYNC &&
			p->state != AS_PARTITION_STATE_ZOMBIE) {
		cf_crash(AS_PARTITION, "{%s:%u} %s reserve - state %u unexpected",
				ns->name, pid, is_read ? "read" : "write", p->state);
	}

	cf_rc_reserve(p->vp);
	cf_rc_reserve(p->sub_vp);

	rsv->ns = ns;
	rsv->p = p;
	rsv->tree = p->vp;
	rsv->sub_tree = p->sub_vp;
	rsv->cluster_key = p->cluster_key;
	rsv->state = p->state;

	rsv->n_dupl = p->n_dupl;

	if (rsv->n_dupl != 0) {
		memcpy(rsv->dupl_nodes, p->dupl_nodes, sizeof(cf_node) * rsv->n_dupl);
	}

	pthread_mutex_unlock(&p->lock);

	return 0;
}


void
partition_reserve_lockfree(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv)
{
	as_partition* p = &ns->partitions[pid];

	cf_rc_reserve(p->vp);
	cf_rc_reserve(p->sub_vp);

	rsv->ns = ns;
	rsv->p = p;
	rsv->tree = p->vp;
	rsv->sub_tree = p->sub_vp;
	rsv->cluster_key = p->cluster_key;
	rsv->state = p->state;

	rsv->n_dupl = p->n_dupl;

	if (rsv->n_dupl != 0) {
		memcpy(rsv->dupl_nodes, p->dupl_nodes, sizeof(cf_node) * rsv->n_dupl);
	}
}


// TODO - deprecate in "six months".
cf_node
partition_getreplica_prole(as_namespace* ns, uint32_t pid)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	// Check is this is a master node.
	cf_node best_node = find_best_node(p, ns, false);

	if (best_node == g_config.self_node) {
		// It's a master, return 0.
		best_node = (cf_node)0;
	}
	else {
		// Not a master, see if it's a prole.
		best_node = find_best_node(p, ns, true);
	}

	pthread_mutex_unlock(&p->lock);

	return best_node;
}


// Definition for the partition-info data:
// name:part_id:STATE:replica_count(int):origin:target:migrate_tx:migrate_rx:sz
char
partition_getstate_str(int state)
{
	switch (state) {
	case AS_PARTITION_STATE_UNDEF:
		return 'U';
	case AS_PARTITION_STATE_SYNC:
		return 'S';
	case AS_PARTITION_STATE_DESYNC:
		return 'D';
	case AS_PARTITION_STATE_ZOMBIE:
		return 'Z';
	case AS_PARTITION_STATE_ABSENT:
		return 'A';
	default:
		return '?';
	}
}


int
partition_get_replica_self_lockfree(const as_namespace* ns, uint32_t pid)
{
	const as_partition* p = &ns->partitions[pid];

	int self_n = find_in_replica_list(p, g_config.self_node); // -1 if not
	bool am_master = (self_n == 0 && p->state == AS_PARTITION_STATE_SYNC) ||
			p->target != (cf_node)0;

	if (am_master) {
		return 0;
	}

	if (self_n > 0 && p->origin == 0 &&
			// Check self_n < n_repl only because n_repl could be out-of-sync
			// with (less than) partition's replica list count.
			self_n < (int)ns->replication_factor) {
		return self_n;
	}

	return -1; // not a replica
}
