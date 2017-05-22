/*
 * partition_balance.c
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

#include "fabric/partition_balance.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_hash_math.h"
#include "citrusleaf/cf_queue.h"

#include "fault.h"
#include "node.h"

#include "base/cluster_config.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/exchange.h"
#include "fabric/migrate.h"
#include "fabric/partition.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

// The instantaneous maximum number of cluster participants, represented as a
// positive and negative mask.
#define AS_CLUSTER_SZ_MASKP ((uint64_t)(1 - (AS_CLUSTER_SZ + 1)))
#define AS_CLUSTER_SZ_MASKN ((uint64_t)(AS_CLUSTER_SZ - 1))

typedef struct inter_hash_s {
	uint64_t hashed_node;
	uint64_t hashed_pid;
} inter_hash;

const as_partition_version ZERO_VERSION = { 0 };


//==========================================================
// Globals.
//

cf_atomic32 g_partition_generation = 0;

// Using int for 4-byte size, but maintaining bool semantics.
// TODO - ok as non-volatile, but should selectively load/store in the future.
// Better - use g_partition_generation and selectively load to compare non-0.
static int g_init_balance_done = false;

static cf_atomic32 g_migrate_num_incoming = 0;

// Using int for 4-byte size, but maintaining bool semantics.
static volatile int g_allow_migrations = true;
static volatile int g_multi_node = false; // XXX JUMP - remove in "six months"

static uint64_t g_hashed_pids[AS_PARTITIONS];

// Shortcuts to values set by as_exchange, for use in partition balance only.
static uint32_t g_cluster_size = 0;
static cf_node* g_succession = NULL;


//==========================================================
// Forward declarations.
//

// Only partition_balance hooks into exchange.
extern cf_node* as_exchange_succession();

// Helpers - generic.
void set_partition_version_in_storage(as_namespace* ns, uint32_t pid, const as_partition_version* version, bool flush);
void partition_migrate_record_fill(partition_migrate_record* pmr, cf_node dest, as_namespace* ns, uint32_t pid, uint64_t cluster_key, emig_type type, uint32_t tx_flags);
void drop_trees(as_partition* p, as_namespace* ns);

// Helpers - balance partitions.
void partition_cluster_topology_info();
void fill_global_tables(cf_node* full_node_seq_table, uint32_t* full_sl_ix_table);
void balance_namespace(cf_node* full_node_seq_table, uint32_t* full_sl_ix_table, as_namespace* ns, cf_queue* mq);
void apply_single_replica_limit(as_namespace* ns);
uint32_t rack_count(const as_namespace* ns);
void fill_translation(int translation[], const as_namespace* ns);
void fill_namespace_rows(const cf_node* full_node_seq, const uint32_t* full_sl_ix, cf_node* ns_node_seq, uint32_t* ns_sl_ix, const as_namespace* ns, const int translation[]);
void rack_aware_adjust_rows(cf_node* ns_node_seq, uint32_t* ns_sl_ix, const as_namespace* ns, uint32_t n_racks);
bool is_rack_distinct_before_n(const uint32_t* ns_sl_ix, const as_namespace* ns, uint32_t rack_id, uint32_t n);
uint32_t find_self(const cf_node* ns_node_seq, const as_namespace* ns);
int find_working_master(const as_partition* p, const uint32_t* ns_sl_ix, const as_namespace* ns);
uint32_t find_duplicates(const as_partition* p, const cf_node* ns_node_seq, const uint32_t* ns_sl_ix, const as_namespace* ns, uint32_t working_master_n, cf_node dupls[]);
void fill_witnesses(as_partition* p, const cf_node* ns_node_seq, const uint32_t* ns_sl_ix, as_namespace* ns);
uint32_t fill_immigrators(as_partition* p, const uint32_t* ns_sl_ix, as_namespace* ns, uint32_t working_master_n, uint32_t n_dupl);
void advance_version(as_partition* p, const uint32_t* ns_sl_ix, as_namespace* ns, uint32_t self_n,	uint32_t working_master_n, uint32_t n_dupl, const cf_node dupls[]);
uint32_t fill_family_versions(const as_partition* p, const uint32_t* ns_sl_ix, const as_namespace* ns, uint32_t working_master_n, uint32_t n_dupl, const cf_node dupls[], as_partition_version family_versions[]);
bool has_replica_parent(const as_partition* p, const uint32_t* ns_sl_ix, const as_namespace* ns, const as_partition_version* subset_version, uint32_t subset_n);
uint32_t find_family(const as_partition_version* self_version, uint32_t n_families, const as_partition_version family_versions[]);
void queue_namespace_migrations(as_partition* p, as_namespace* ns, uint32_t self_n, cf_node working_master, uint32_t n_dupl, cf_node dupls[], cf_queue* mq);

// Helpers - migration-related.
bool partition_immigration_is_valid(const as_partition* p, cf_node source_node, const as_namespace* ns, const char* tag);

// Old migration-related public API.
void old_as_partition_emigrate_done(as_migrate_state s, struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, uint32_t tx_flags);
as_migrate_result old_as_partition_immigrate_start(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, uint32_t start_type, cf_node source_node);
as_migrate_result old_as_partition_immigrate_done(struct as_namespace_s* ns, uint32_t pid, uint64_t orig_cluster_key, cf_node source_node);

// Old helpers - generic.
void set_partition_vinfo_in_storage(as_namespace* ns, uint32_t pid, const as_partition_vinfo* vinfo, bool flush);
void generate_new_partition_vinfo(as_partition_vinfo* new_vinfo);
void set_partition_sync_lockfree(as_partition* p, as_namespace* ns, bool flush);
void set_partition_desync_lockfree(as_partition* p, as_namespace* ns, bool flush);
void set_partition_absent_lockfree(as_partition* p, as_namespace* ns, bool flush);

// Old helpers - balance partitions.
void old_balance_namespace(cf_node* full_node_seq_table, uint32_t* full_sl_ix_table, as_namespace* ns, cf_queue* mq);
void old_rack_aware_adjust_rows(cf_node* ns_node_seq, uint32_t* ns_sl_ix, const as_namespace* ns);
bool old_is_group_distinct_before_n(const cf_node* ns_node_seq, cc_node_t group_id, uint32_t n);
int set_primary_version(as_partition* p, const cf_node* ns_node_seq, const uint32_t* ns_vinfo_index, as_namespace* ns, bool has_version[], int* self_n);
void handle_lost_partition(as_partition* p, const cf_node* ns_node_seq, as_namespace* ns, bool has_version[]);
uint32_t old_find_duplicates(const as_partition* p, const cf_node* ns_node_seq, const uint32_t* ns_vinfo_index, const as_namespace* ns, cf_node dupl_nodes[]);
bool should_advance_version(const as_partition* p, uint32_t old_repl_factor, as_namespace* ns);
void old_advance_version(as_partition* p, const cf_node* ns_node_seq, const uint32_t* ns_vinfo_index, as_namespace* ns, int first_versioned_n, const as_partition_vinfo* new_vinfo);
void old_queue_namespace_migrations(as_partition* p, const cf_node* ns_node_seq, as_namespace* ns, int self_n, int first_versioned_n, const bool has_version[], uint32_t n_dupl, const cf_node dupl_nodes[], cf_queue* mq, int* ns_delayed_emigrations);


//==========================================================
// Inlines & macros.
//

// XXX JUMP - remove in "six months".
static inline bool
old_is_rack_aware()
{
	return g_config.cluster_mode != CL_MODE_NO_TOPOLOGY && g_cluster_size > 1;
}

static inline bool
is_self_final_master(const as_partition* p)
{
	return p->replicas[0] == g_config.self_node;
}

static inline bool
is_self_replica(const as_partition* p)
{
	return contains_node(p->replicas, p->n_replicas, g_config.self_node);
}

static inline bool
is_family_same(const as_partition_version* v1, const as_partition_version* v2)
{
	return v1->ckey == v2->ckey && v1->family == v2->family &&
			v1->family != VERSION_FAMILY_UNIQUE;
}

static inline bool
contains_self(const cf_node* nodes, uint32_t n_nodes)
{
	return contains_node(nodes, n_nodes, g_config.self_node);
}

static inline uint32_t
remove_node(cf_node* nodes, uint32_t n_nodes, cf_node node)
{
	int n = index_of_node(nodes, n_nodes, node);

	if (n != -1) {
		nodes[n] = nodes[--n_nodes];
	}

	return n_nodes;
}

// A comparison_fn_t used with qsort().
static inline int
compare_hashed_nodes(const void* pa, const void* pb)
{
	const uint64_t* a = (const uint64_t*)pa;
	const uint64_t* b = (const uint64_t*)pb;

	return *a > *b ? -1 : (*a == *b ? 0 : 1);
}

// A comparison_fn_t used with qsort().
static inline int
compare_rack_ids(const void* pa, const void* pb)
{
	const uint32_t* a = (const uint32_t*)pa;
	const uint32_t* b = (const uint32_t*)pb;

	return *a > *b ? -1 : (*a == *b ? 0 : 1);
}

// Define macros for accessing the full node-seq and sl-ix arrays.
#define FULL_NODE_SEQ(x, y) full_node_seq_table[(x * g_cluster_size) + y]
#define FULL_SL_IX(x, y) full_sl_ix_table[(x * g_cluster_size) + y]

// Get the partition version that was input by exchange.
#define INPUT_VERSION(_n) (&ns->cluster_versions[ns_sl_ix[_n]][p->id])

// Get the rack-id that was input by exchange.
#define RACK_ID(_n) (ns->rack_ids[ns_sl_ix[_n]])


//==========================================================
// Public API - regulate migrations.
//

void
as_partition_balance_allow_migrations()
{
	if (as_new_clustering()) {
		cf_detail(AS_PARTITION, "allow migrations");
	}
	else {
		cf_info(AS_PARTITION, "ALLOW MIGRATIONS");
	}

	g_allow_migrations = true;
}


void
as_partition_balance_disallow_migrations()
{
	if (as_new_clustering()) {
		cf_detail(AS_PARTITION, "disallow migrations");
	}
	else {
		cf_info(AS_PARTITION, "DISALLOW MIGRATIONS");
	}

	g_allow_migrations = false;
}


bool
as_partition_balance_are_migrations_allowed()
{
	return g_allow_migrations;
}


void
as_partition_balance_synchronize_migrations()
{
	// Acquire and release each partition lock to ensure threads acquiring a
	// partition lock after this will be forced to check the latest cluster key.
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			pthread_mutex_lock(&p->lock);
			pthread_mutex_unlock(&p->lock);
		}
	}

	// Prior-round migrations won't decrement g_migrate_num_incoming due to
	// cluster key check.
	cf_atomic32_set(&g_migrate_num_incoming, 0);
}


//==========================================================
// Public API - balance partitions.
//

void
as_partition_balance_init()
{
	// Cache hashed pids for all future rebalances.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		g_hashed_pids[pid] = cf_hash_fnv64((const uint8_t*)&pid,
				sizeof(uint32_t));
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		uint32_t n_stored = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			if (ns->storage_type != AS_STORAGE_ENGINE_SSD) {
				continue;
			}

			as_partition_vinfo vinfo;

			// TODO - change storage function to take new version after jump.
			as_storage_info_get(ns, pid, &vinfo);

			if (as_new_clustering()) {
				as_partition_version version = *(as_partition_version*)&vinfo;

				if (as_partition_version_is_null(&version)) {
					// Stores the length, even when the version is zeroed.
					set_partition_version_in_storage(ns, pid, &ZERO_VERSION,
							false);
				}
				else {
					as_partition* p = &ns->partitions[pid];

					p->n_replicas = 1;
					p->replicas[0] = g_config.self_node;

					version.master = 0;
					version.subset = 1;

					p->version = version;

					ns->cluster_versions[0][pid] = version;

					n_stored++;
				}
			}
			else {
				if (as_partition_is_null(&vinfo)) {
					// Stores the vinfo length, even when the vinfo is zeroed.
					set_partition_vinfo_in_storage(ns, pid, &NULL_VINFO, false);
				}
				else {
					as_partition* p = &ns->partitions[pid];

					p->n_replicas = 1;
					p->replicas[0] = g_config.self_node;

					p->primary_version_info = vinfo;
					p->version_info = vinfo;
					p->state = AS_PARTITION_STATE_SYNC;

					ns->cluster_vinfo[0][pid] = vinfo;

					n_stored++;
				}
			}
		}

		if (n_stored < AS_PARTITIONS) {
			as_storage_info_flush(ns);
		}

		cf_info(AS_PARTITION, "{%s} %u partitions: found %u absent, %u stored",
				ns->name, AS_PARTITIONS, AS_PARTITIONS - n_stored, n_stored);
	}
}


// Has the node resolved as operating either in a multi-node cluster or as a
// single-node cluster?
bool
as_partition_balance_is_init_resolved()
{
	return g_init_balance_done;
}


void
as_partition_balance_revert_to_orphan()
{
	g_init_balance_done = false;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		client_replica_maps_clear(ns);

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			pthread_mutex_lock(&p->lock);

			if (! as_partition_version_is_null(&p->version)) {
				p->version.master = 0;
				p->version.subset = 1;
			}

			pthread_mutex_unlock(&p->lock);
		}
	}

	cf_atomic32_incr(&g_partition_generation);
}


void
as_partition_balance()
{
	// Temporary paranoia.
	static uint64_t last_cluster_key = 0;

	if (last_cluster_key == as_exchange_cluster_key()) {
		cf_warning(AS_PARTITION, "as_partition_balance: cluster key %lx same as last time",
				last_cluster_key);
		return;
	}

	last_cluster_key = as_exchange_cluster_key();
	// End - temporary paranoia.

	// These shortcuts must only be used under the scope of this function.
	g_cluster_size = as_exchange_cluster_size();
	g_succession = as_exchange_succession();

	// Prepare rack aware info.
	if (! as_new_clustering()) {
		partition_cluster_topology_info();
	}

	cf_node* full_node_seq_table =
			cf_malloc(AS_PARTITIONS * g_cluster_size * sizeof(cf_node));

	cf_assert(full_node_seq_table, AS_PARTITION, "as_partition_balance: couldn't allocate node sequence table");

	uint32_t* full_sl_ix_table =
			cf_malloc(AS_PARTITIONS * g_cluster_size * sizeof(uint32_t));

	cf_assert(full_sl_ix_table, AS_PARTITION, "as_partition_balance: couldn't allocate succession index table");

	// Each partition separately shuffles the node succession list to generate
	// its own node sequence.
	fill_global_tables(full_node_seq_table, full_sl_ix_table);

	cf_queue mq;

	cf_queue_init(&mq, sizeof(partition_migrate_record),
			g_config.n_namespaces * AS_PARTITIONS, false);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		if (as_new_clustering()) {
			balance_namespace(full_node_seq_table, full_sl_ix_table,
					g_config.namespaces[ns_ix], &mq);
		}
		else {
			old_balance_namespace(full_node_seq_table, full_sl_ix_table,
					g_config.namespaces[ns_ix], &mq);
		}
	}

	// All partitions now have replicas assigned, ok to allow transactions.
	g_init_balance_done = true;
	cf_atomic32_incr(&g_partition_generation);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_storage_info_flush(g_config.namespaces[ns_ix]);
	}

	as_partition_balance_allow_migrations();

	partition_migrate_record pmr;

	while (cf_queue_pop(&mq, &pmr, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		as_migrate_emigrate(&pmr);
	}

	cf_queue_destroy(&mq);

	cf_free(full_node_seq_table);
	cf_free(full_sl_ix_table);
}


uint64_t
as_partition_balance_remaining_migrations()
{
	uint64_t remaining_migrations = 0;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		remaining_migrations += ns->migrate_tx_partitions_remaining;
		remaining_migrations += ns->migrate_rx_partitions_remaining;
	}

	return remaining_migrations;
}


//==========================================================
// Public API - migration-related as_partition methods.
//

// Currently used only for enterprise build.
bool
as_partition_pending_migrations(as_partition* p)
{
	pthread_mutex_lock(&p->lock);

	bool pending = p->pending_immigrations + p->pending_emigrations > 0;

	pthread_mutex_unlock(&p->lock);

	return pending;
}


void
as_partition_emigrate_done(as_migrate_state s, as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t tx_flags)
{
	if (! as_new_clustering()) {
		old_as_partition_emigrate_done(s, ns, pid, orig_cluster_key, tx_flags);
		return;
	}

	// TODO - better handled outside?
	if (s != AS_MIGRATE_STATE_DONE) {
		if (s == AS_MIGRATE_STATE_ERROR) {
			if (orig_cluster_key == as_exchange_cluster_key()) {
				cf_warning(AS_PARTITION, "{%s:%u} emigrate_done - error result but cluster key is current",
						ns->name, pid);
			}
		}
		else {
			cf_warning(AS_PARTITION, " {%s:%u} emigrate_done - unexpected result %d",
					ns->name, pid, (int)s);
		}

		return;
	}

	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return;
	}

	if (p->pending_emigrations == 0) {
		cf_warning(AS_PARTITION, "{%s:%u} emigrate_done - no pending emigrations",
				ns->name, pid);
		pthread_mutex_unlock(&p->lock);
		return;
	}

	p->pending_emigrations--;

	int64_t migrates_tx_remaining =
			cf_atomic_int_decr(&ns->migrate_tx_partitions_remaining);

	if (migrates_tx_remaining < 0){
		cf_warning(AS_PARTITION, "{%s:%u} (%d,%ld) emigrate_done - counter went negative",
				ns->name, pid, p->pending_emigrations, migrates_tx_remaining);
	}

	p->current_outgoing_ldt_version = 0;

	if (! is_self_final_master(p)) {
		if ((tx_flags & TX_FLAGS_ACTING_MASTER) != 0) {
			p->target = (cf_node)0;
			p->n_dupl = 0;
			p->version.master = 0;
		}

		p->version.ckey = p->final_version.ckey;
		p->version.family = 0;

		if (p->pending_immigrations != 0 || ! is_self_replica(p)) {
			p->version.subset = 1;
		}
		// else - must already be a parent.

		set_partition_version_in_storage(ns, p->id, &p->version, true);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	cf_queue mq;
	partition_migrate_record pmr;
	int w_ix = -1;

	if (is_self_final_master(p) &&
			p->pending_emigrations == 0 && p->pending_immigrations == 0) {
		cf_queue_init(&mq, sizeof(partition_migrate_record), p->n_witnesses,
				false);

		for (w_ix = 0; w_ix < (int)p->n_witnesses; w_ix++) {
			partition_migrate_record_fill(&pmr, p->witnesses[w_ix], ns, pid,
					orig_cluster_key, EMIG_TYPE_SIGNAL_ALL_DONE, TX_FLAGS_NONE);
			cf_queue_push(&mq, &pmr);
		}
	}

	pthread_mutex_unlock(&p->lock);

	if (w_ix >= 0) {
		while (cf_queue_pop(&mq, &pmr, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
			as_migrate_emigrate(&pmr);
		}

		cf_queue_destroy(&mq);
	}
}


as_migrate_result
as_partition_immigrate_start(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t start_type, cf_node source_node)
{
	if (! as_new_clustering()) {
		return old_as_partition_immigrate_start(ns, pid, orig_cluster_key,
				start_type, source_node);
	}

	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	uint32_t num_incoming = (uint32_t)cf_atomic32_incr(&g_migrate_num_incoming);

	if (num_incoming > g_config.migrate_max_num_incoming) {
		cf_atomic32_decr(&g_migrate_num_incoming);
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	if (! partition_immigration_is_valid(p, source_node, ns, "start")) {
		cf_atomic32_decr(&g_migrate_num_incoming);
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	if (! is_self_final_master(p) &&
			// Become subset of final version if not already such.
			! (p->version.ckey == p->final_version.ckey &&
					p->version.family == 0 && p->version.subset == 1)) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = 0;
		p->version.master = 0; // racing emigrate done if we were acting master
		p->version.subset = 1;
		// Leave evade flag as-is.

		set_partition_version_in_storage(ns, p->id, &p->version, true);
	}

	pthread_mutex_unlock(&p->lock);

	return AS_MIGRATE_OK;
}


as_migrate_result
as_partition_immigrate_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, cf_node source_node)
{
	if (! as_new_clustering()) {
		return old_as_partition_immigrate_done(ns, pid, orig_cluster_key,
				source_node);
	}

	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	cf_atomic32_decr(&g_migrate_num_incoming);

	if (! partition_immigration_is_valid(p, source_node, ns, "done")) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	p->pending_immigrations--;

	int64_t migrates_rx_remaining =
			cf_atomic_int_decr(&ns->migrate_rx_partitions_remaining);

	// Sanity-check only.
	if (migrates_rx_remaining < 0) {
		cf_warning(AS_PARTITION, "{%s:%u} (%d,%ld) immigrate_done - counter went negative",
				ns->name, pid, p->pending_immigrations, migrates_rx_remaining);
	}

	if (p->pending_immigrations == 0 &&
			! as_partition_version_same(&p->version, &p->final_version)) {
		p->version = p->final_version;
		set_partition_version_in_storage(ns, p->id, &p->version, true);
	}

	if (! is_self_final_master(p)) {
		p->origin = (cf_node)0;

		if (client_replica_maps_update(ns, pid)) {
			cf_atomic32_incr(&g_partition_generation);
		}

		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_OK;
	}

	// Final master finished an immigration, adjust duplicates.

	if (p->origin == source_node) {
		p->origin = (cf_node)0;

		if (! as_partition_version_same(&p->version, &p->final_version)) {
			p->version = p->final_version;
			set_partition_version_in_storage(ns, p->id, &p->version, true);
		}
	}
	else {
		p->n_dupl = remove_node(p->dupls, p->n_dupl, source_node);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	if (p->pending_immigrations != 0) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_OK;
	}

	// Final master finished all immigration.

	cf_queue mq;
	partition_migrate_record pmr;

	if (p->pending_emigrations != 0) {
		cf_queue_init(&mq, sizeof(partition_migrate_record), p->n_replicas - 1,
				false);

		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->immigrators[repl_ix]) {
				partition_migrate_record_fill(&pmr, p->replicas[repl_ix], ns,
						pid, orig_cluster_key, EMIG_TYPE_TRANSFER,
						TX_FLAGS_NONE);
				cf_queue_push(&mq, &pmr);
			}
		}
	}
	else {
		cf_queue_init(&mq, sizeof(partition_migrate_record), p->n_witnesses,
				false);

		for (uint32_t w_ix = 0; w_ix < p->n_witnesses; w_ix++) {
			partition_migrate_record_fill(&pmr, p->witnesses[w_ix], ns, pid,
					orig_cluster_key, EMIG_TYPE_SIGNAL_ALL_DONE, TX_FLAGS_NONE);
			cf_queue_push(&mq, &pmr);
		}
	}

	pthread_mutex_unlock(&p->lock);

	while (cf_queue_pop(&mq, &pmr, 0) == CF_QUEUE_OK) {
		as_migrate_emigrate(&pmr);
	}

	cf_queue_destroy(&mq);

	return AS_MIGRATE_OK;
}


as_migrate_result
as_partition_migrations_all_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	if (p->pending_emigrations != 0) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	// Not a replica - drop partition.
	if (! is_self_replica(p)) {
		p->version = ZERO_VERSION;
		set_partition_version_in_storage(ns, p->id, &p->version, true);
		drop_trees(p, ns);
	}

	pthread_mutex_unlock(&p->lock);

	return AS_MIGRATE_OK;
}


//==========================================================
// Local helpers - generic.
//

void
set_partition_version_in_storage(as_namespace* ns, uint32_t pid,
		const as_partition_version* version, bool flush)
{
	as_partition_vinfo vinfo = { .iid = *(uint64_t*)version };

	// TODO - change storage function to take new version after jump.
	as_storage_info_set(ns, pid, &vinfo);

	if (flush) {
		as_storage_info_flush(ns);
	}
}


void
partition_migrate_record_fill(partition_migrate_record* pmr, cf_node dest,
		as_namespace* ns, uint32_t pid, uint64_t cluster_key, emig_type type,
		uint32_t tx_flags)
{
	pmr->dest = dest;
	pmr->ns = ns;
	pmr->pid = pid;
	pmr->type = type;
	pmr->tx_flags = tx_flags;
	pmr->cluster_key = cluster_key;
}


void
drop_trees(as_partition* p, as_namespace* ns)
{
	as_index_tree* temp = p->vp;

	p->vp = as_index_tree_create(&ns->tree_shared, ns->arena);
	as_index_tree_release(temp);

	if (ns->ldt_enabled) {
		as_index_tree* sub_temp = p->sub_vp;

		p->sub_vp = as_index_tree_create(&ns->tree_shared, ns->arena);
		as_index_tree_release(sub_temp);
	}

	// TODO - consider p->n_tombstones?
	cf_atomic64_set(&p->max_void_time, 0);
}


//==========================================================
// Local helpers - balance partitions.
//

void
partition_cluster_topology_info()
{
	uint32_t distinct_groups = 0;
	cluster_config_t cc;

	cc_cluster_config_defaults(&cc);

	for (uint32_t cur_n = 0; cur_n < g_cluster_size; cur_n++) {
		cc_group_t cur_group = cc_compute_group_id(g_succession[cur_n]);

		cc_add_fullnode_group_entry(&cc, g_succession[cur_n]);

		uint32_t prev_n;

		for (prev_n = 0; prev_n < cur_n; prev_n++) {
			if (cc_compute_group_id(g_succession[prev_n]) == cur_group) {
				break;
			}
		}

		if (prev_n == cur_n) {
			distinct_groups++;
		}
	}

	cc.cluster_state = cc_get_cluster_state(&cc);
	g_config.cluster.cluster_state = cc.cluster_state;
	g_config.cluster.group_count = cc.group_count;

	cc_show_cluster_state(&cc);
}


// fill_global_tables()
//
//  Succession list - all nodes in cluster
//  +---------------+
//  | A | B | C | D |
//  +---------------+
//
//  Succession list index - used as version table index (version_ix)
//  +---------------+
//  | 0 | 1 | 2 | 3 |
//  +---------------+
//
// Every partition shuffles the succession list independently, e.g. for pid 0:
// Hash the node names with the pid:
//  H(A,0) = Y, H(B,0) = X, H(C,0) = W, H(D,0) = Z
// Store version_ix in last byte of hash results so it doesn't affect sort:
//  +-----------------------+
//  | Y_0 | X_1 | W_2 | Z_3 |
//  +-----------------------+
// This sorts to:
//  +-----------------------+
//  | W_2 | X_1 | Y_0 | Z_3 |
//  +-----------------------+
// Replace original node names, and keep version_ix order, resulting in:
//  +---------------+    +---------------+
//  | C | B | A | D |    | 2 | 1 | 0 | 3 |
//  +---------------+    +---------------+
//
//  Node sequence table      Version info index table
//   pid                      pid
//  +===+---------------+    +===+---------------+
//  | 0 | C | B | A | D |    | 0 | 2 | 1 | 0 | 3 |
//  +===+---------------+    +===+---------------+
//  | 1 | A | D | C | B |    | 1 | 0 | 3 | 2 | 1 |
//  +===+---------------+    +===+---------------+
//  | 2 | D | C | B | A |    | 2 | 3 | 2 | 1 | 0 |
//  +===+---------------+    +===+---------------+
//  | 3 | B | A | D | C |    | 3 | 1 | 0 | 3 | 2 |
//  +===+---------------+    +===+---------------+
//  | 4 | D | B | C | A |    | 4 | 3 | 1 | 2 | 0 |
//  +===+---------------+    +===+---------------+
//  ... to pid 4095.
//
// We keep the partition version index table so we can refer back to namespaces'
// version tables, where nodes are in the original succession list order.
void
fill_global_tables(cf_node* full_node_seq_table, uint32_t* full_sl_ix_table)
{
	uint64_t hashed_nodes[g_cluster_size];

	for (uint32_t n = 0; n < g_cluster_size; n++) {
		hashed_nodes[n] = cf_hash_fnv64((const uint8_t*)&g_succession[n],
				sizeof(cf_node));
	}

	// Build the node sequence table.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		inter_hash h;

		h.hashed_pid = g_hashed_pids[pid];

		for (uint32_t n = 0; n < g_cluster_size; n++) {
			h.hashed_node = hashed_nodes[n];

			cf_node* node_p = &FULL_NODE_SEQ(pid, n);

			*node_p = cf_hash_jen64((const uint8_t*)&h, sizeof(h));

			// Overlay index onto last byte.
			*node_p &= AS_CLUSTER_SZ_MASKP;
			*node_p += n;
		}

		// Sort the hashed node values.
		qsort(&full_node_seq_table[pid * g_cluster_size], g_cluster_size,
				sizeof(cf_node), compare_hashed_nodes);

		// Overwrite the sorted hash values with the original node IDs.
		for (uint32_t n = 0; n < g_cluster_size; n++) {
			cf_node* node_p = &FULL_NODE_SEQ(pid, n);
			uint32_t version_ix = (uint32_t)(*node_p & AS_CLUSTER_SZ_MASKN);

			*node_p = g_succession[version_ix];

			// Saved to refer back to the partition version table.
			FULL_SL_IX(pid, n) = version_ix;
		}
	}
}


void
balance_namespace(cf_node* full_node_seq_table, uint32_t* full_sl_ix_table,
		as_namespace* ns, cf_queue* mq)
{
	// Figure out effective replication factor in the face of node failures.
	apply_single_replica_limit(ns);

	uint32_t n_racks = rack_count(ns);

	// If a namespace is not on all nodes or is rack aware, it can't use the
	// global node sequence and index tables.
	bool ns_not_equal_global =
			ns->cluster_size != g_cluster_size || n_racks != 1;

	// The translation array is used to convert global table rows to namespace
	// rows, if  necessary.
	int translation[ns_not_equal_global ? g_cluster_size : 0];

	if (ns_not_equal_global) {
		cf_info(AS_PARTITION, "{%s} is on %u of %u nodes", ns->name,
				ns->cluster_size, g_cluster_size);

		fill_translation(translation, ns);
	}

	int ns_pending_immigrations = 0;
	int ns_pending_emigrations = 0;
	int ns_pending_signals = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition* p = &ns->partitions[pid];

		cf_node* full_node_seq = &FULL_NODE_SEQ(pid, 0);
		uint32_t* full_sl_ix = &FULL_SL_IX(pid, 0);

		// Usually a namespace can simply use the global tables...
		cf_node* ns_node_seq = full_node_seq;
		uint32_t* ns_sl_ix = full_sl_ix;

		cf_node stack_node_seq[ns_not_equal_global ? ns->cluster_size : 0];
		uint32_t stack_version_ix[ns_not_equal_global ? ns->cluster_size : 0];

		// ... but sometimes a namespace is different.
		if (ns_not_equal_global) {
			ns_node_seq = stack_node_seq;
			ns_sl_ix = stack_version_ix;

			fill_namespace_rows(full_node_seq, full_sl_ix, ns_node_seq,
					ns_sl_ix, ns, translation);

			if (n_racks != 1) {
				rack_aware_adjust_rows(ns_node_seq, ns_sl_ix, ns, n_racks);
			}
		}

		pthread_mutex_lock(&p->lock);

		p->n_replicas = ns->replication_factor;
		memset(p->replicas, 0, sizeof(p->replicas));
		memcpy(p->replicas, ns_node_seq, p->n_replicas * sizeof(cf_node));

		p->cluster_key = as_exchange_cluster_key();

		p->acting_master_involved = false;

		p->pending_emigrations = 0;
		p->pending_immigrations = 0;
		memset(p->immigrators, 0, sizeof(p->immigrators));

		p->origin = (cf_node)0;
		p->target = (cf_node)0;

		p->n_dupl = 0;
		memset(p->dupls, 0, sizeof(p->dupls));

		p->n_witnesses = 0;
		memset(p->witnesses, 0, sizeof(p->witnesses));

		p->current_outgoing_ldt_version = 0;

		uint32_t self_n = find_self(ns_node_seq, ns);

		as_partition_version final_version = { .ckey = p->cluster_key };

		p->final_version = final_version;
		p->final_version.master = self_n == 0 ? 1 : 0;

		int working_master_n = find_working_master(p, ns_sl_ix, ns);

		uint32_t n_dupl = 0;
		cf_node dupls[ns->cluster_size];

		memset(dupls, 0, sizeof(dupls));

		// TEMPORARY debugging.
		uint32_t debug_n_immigrators = 0;
		as_partition_version debug_orig = ZERO_VERSION;

		if (working_master_n == -1) {
			// No existing versions - assign fresh version to replicas.
			working_master_n = 0;

			if (self_n < p->n_replicas) {
				p->version = p->final_version;
			}
		}
		else {
			p->acting_master_involved = working_master_n != 0;

			n_dupl = find_duplicates(p, ns_node_seq, ns_sl_ix, ns,
					(uint32_t)working_master_n, dupls);

			uint32_t n_immigrators = fill_immigrators(p, ns_sl_ix, ns,
					(uint32_t)working_master_n, n_dupl);

			// TEMPORARY debugging.
			debug_n_immigrators = n_immigrators;
			debug_orig = p->version;

			if (n_immigrators != 0) {
				// Migrations required - advance versions for next rebalance,
				// queue migrations for this rebalance.

				advance_version(p, ns_sl_ix, ns, self_n,
						(uint32_t)working_master_n, n_dupl, dupls);

				queue_namespace_migrations(p, ns, self_n,
						ns_node_seq[working_master_n], n_dupl, dupls, mq);

				if (self_n == 0) {
					fill_witnesses(p, ns_node_seq, ns_sl_ix, ns);
					ns_pending_signals += p->n_witnesses;
				}
			}
			else {
				// No migrations required - refresh replicas' versions (only
				// truly necessary if replication factor decreased) and drop
				// superfluous non-replica partitions immediately.

				if (self_n < p->n_replicas) {
					p->version = p->final_version;
				}
				else {
					p->version = ZERO_VERSION;
					set_partition_version_in_storage(ns, p->id, &p->version,
							false);
					drop_trees(p, ns);
				}
			}
		}

		if (! as_partition_version_is_null(&p->version)) {
			set_partition_version_in_storage(ns, p->id, &p->version, false);
		}

		ns_pending_immigrations += p->pending_immigrations;
		ns_pending_emigrations += p->pending_emigrations;

		// TEMPORARY debugging.
		if (pid < 20) {
			cf_debug(AS_PARTITION, "ck%012lX %02u (%d %d) %s -> %s - self_n %u wm_n %d repls %u dupls %u immigrators %u",
					p->cluster_key, pid, p->pending_emigrations,
					p->pending_immigrations, VERSION_AS_STRING(&debug_orig),
					VERSION_AS_STRING(&p->version), self_n, working_master_n,
					p->n_replicas, n_dupl, debug_n_immigrators);
		}

		client_replica_maps_update(ns, pid);

		pthread_mutex_unlock(&p->lock);
	}

	cf_info(AS_PARTITION, "{%s} re-balanced, expected migrations - (%d tx, %d rx, %d sig)",
			ns->name, ns_pending_emigrations, ns_pending_immigrations,
			ns_pending_signals);

	ns->migrate_tx_partitions_initial = (uint64_t)ns_pending_emigrations;
	ns->migrate_tx_partitions_remaining = (uint64_t)ns_pending_emigrations;

	ns->migrate_rx_partitions_initial = (uint64_t)ns_pending_immigrations;
	ns->migrate_rx_partitions_remaining = (uint64_t)ns_pending_immigrations;

	ns->migrate_signals_remaining = ns_pending_signals;
}


void
apply_single_replica_limit(as_namespace* ns)
{
	// Replication factor can't be bigger than observed cluster.
	uint32_t repl_factor = ns->cluster_size < ns->cfg_replication_factor ?
			ns->cluster_size : ns->cfg_replication_factor;

	// Reduce the replication factor to 1 if the cluster size is less than or
	// equal to the specified limit.
	ns->replication_factor =
			ns->cluster_size <= g_config.paxos_single_replica_limit ?
					1 : repl_factor;

	cf_info(AS_PARTITION, "{%s} replication factor is %u", ns->name,
			ns->replication_factor);
}


uint32_t
rack_count(const as_namespace* ns)
{
	uint32_t ids[ns->cluster_size];

	memcpy(ids, ns->rack_ids, sizeof(ids));
	qsort(ids, ns->cluster_size, sizeof(uint32_t), compare_rack_ids);

	if (ids[0] == ids[ns->cluster_size - 1]) {
		return 1; // common path - not rack-aware
	}

	uint32_t n_racks = 1;
	uint32_t cur_id = ids[0];

	for (uint32_t i = 1; i < ns->cluster_size; i++) {
		if (ids[i] != cur_id) {
			cur_id = ids[i];
			n_racks++;
		}
	}

	return n_racks;
}


void
fill_translation(int translation[], const as_namespace* ns)
{
	int ns_n = 0;

	for (uint32_t full_n = 0; full_n < g_cluster_size; full_n++) {
		translation[full_n] = g_succession[full_n] == ns->succession[ns_n] ?
				ns_n++ : -1;
	}
}


void
fill_namespace_rows(const cf_node* full_node_seq, const uint32_t* full_sl_ix,
		cf_node* ns_node_seq, uint32_t* ns_sl_ix, const as_namespace* ns,
		const int translation[])
{
	if (ns->cluster_size == g_cluster_size) {
		// Rack-aware but namespace is on all nodes - just copy.
		memcpy(ns_node_seq, full_node_seq, g_cluster_size * sizeof(cf_node));
		memcpy(ns_sl_ix, full_sl_ix, g_cluster_size * sizeof(int));

		return;
	}

	// Fill namespace sequences from global table rows using translation array.
	uint32_t n = 0;

	for (uint32_t full_n = 0; full_n < g_cluster_size; full_n++) {
		int ns_n = translation[full_sl_ix[full_n]];

		if (ns_n != -1) {
			ns_node_seq[n] = ns->succession[ns_n];
			ns_sl_ix[n] = (uint32_t)ns_n;
			n++;
		}
	}
}


// rack_aware_adjust_rows()
//
// When "rack aware", nodes are in "racks".
//
//  Nodes and racks in the cluster
//  +---------------+
//  | Rack1 | Rack2 |
//  +---------------+
//  | A | B | C | D |
//  +---------------+
//
// Proles for a partition can't be in the same rack as the master, e.g. for
// replication factor 2:
//
//  Node sequence table      Succession index table
//   pid                      pid
//  +===+-------+-------+    +===+-------+-------+
//  | 0 | C | B | A | D |    | 0 | 2 | 1 | 0 | 3 |
//  +===+-------+-------+    +===+-------+-------+
//  | 1 | A | D | C | B |    | 1 | 0 | 3 | 2 | 1 |
//  +===+-------+-------+    +===+-------+-------+
//  | 2 | D |<C>| B | A |    | 2 | 3 |<2>| 1 | 0 | <= adjustment needed
//  +===+-------+-------+    +===+-------+-------+
//  | 3 | B |<A>| D | C |    | 3 | 1 |<0>| 3 | 2 | <= adjustment needed
//  +===+-------+-------+    +===+-------+-------+
//  | 4 | D | B | C | A |    | 4 | 3 | 1 | 2 | 0 |
//  +===+-------+-------+    +===+-------+-------+
//  ... to pid 4095.
//
// To adjust a table row, we swap the prole with the first non-replica.
void
rack_aware_adjust_rows(cf_node* ns_node_seq, uint32_t* ns_sl_ix,
		const as_namespace* ns, uint32_t n_racks)
{
	uint32_t n_needed = n_racks < ns->replication_factor ?
			n_racks : ns->replication_factor;

	uint32_t next_n = n_needed; // next candidate index to swap with

	for (uint32_t cur_n = 1; cur_n < n_needed; cur_n++) {
		uint32_t cur_rack_id = RACK_ID(cur_n);

		// If cur_rack_id is unique for nodes < cur_i, continue to next node.
		if (is_rack_distinct_before_n(ns_sl_ix, ns, cur_rack_id, cur_n)) {
			continue;
		}

		// Find group after cur_i that's unique for rack-ids before cur_i.
		uint32_t swap_n = cur_n; // if swap cannot be found then no change

		while (next_n < ns->cluster_size) {
			uint32_t next_rack_id = RACK_ID(next_n);

			if (is_rack_distinct_before_n(ns_sl_ix, ns, next_rack_id, cur_n)) {
				swap_n = next_n;
				next_n++;
				break;
			}

			next_n++;
		}

		if (swap_n == cur_n) {
			// No other distinct rack-ids found - shouldn't be possible.
			// We should reach n_needed first.
			cf_crash(AS_PARTITION, "can't find a diff cur:%u swap:%u repl:%u clsz:%u",
					cur_n, swap_n, ns->replication_factor, ns->cluster_size);
		}

		// Now swap cur_n with swap_n.

		// Swap node.
		cf_node temp_node = ns_node_seq[swap_n];

		ns_node_seq[swap_n] = ns_node_seq[cur_n];
		ns_node_seq[cur_n] = temp_node;

		// Swap succession list index.
		uint32_t temp_ix = ns_sl_ix[swap_n];

		ns_sl_ix[swap_n] = ns_sl_ix[cur_n];
		ns_sl_ix[cur_n] = temp_ix;
	}
}


// Returns true if rack_id is unique within nodes list indices less than n.
bool
is_rack_distinct_before_n(const uint32_t* ns_sl_ix, const as_namespace* ns,
		uint32_t rack_id, uint32_t n)
{
	for (uint32_t cur_n = 0; cur_n < n; cur_n++) {
		uint32_t cur_rack_id = RACK_ID(cur_n);

		if (cur_rack_id == rack_id) {
			return false;
		}
	}

	return true;
}


uint32_t
find_self(const cf_node* ns_node_seq, const as_namespace* ns)
{
	int n = index_of_node(ns_node_seq, ns->cluster_size, g_config.self_node);

	cf_assert(n != -1, AS_PARTITION, "{%s} self node not in succession list",
			ns->name);

	return (uint32_t)n;
}


// Preference: Vm > V > Ve > Vs > Vse > absent.
int
find_working_master(const as_partition* p, const uint32_t* ns_sl_ix,
		const as_namespace* ns)
{
	int best_n = -1;
	int best_score = -1;

	for (int n = 0; n < (int)ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		// Skip zero versions.
		if (as_partition_version_is_null(version)) {
			continue;
		}

		// If previous working master exists, use it. (There can be more than
		// one after split brains. Also, the flag is only to prevent superfluous
		// master swaps on rebalance when rack-aware.)
		if (version->master == 1) {
			return n;
		}
		// else - keep going but remember the best so far.

		// V = 3 > Ve = 2 > Vs = 1 > Vse = 0.
		int score = (version->evade == 1 ? 0 : 1) +
				(version->subset == 1 ? 0 : 2);

		if (score > best_score) {
			best_score = score;
			best_n = n;
		}
	}

	return best_n;
}


uint32_t
find_duplicates(const as_partition* p, const cf_node* ns_node_seq,
		const uint32_t* ns_sl_ix, const as_namespace* ns,
		uint32_t working_master_n, cf_node dupls[])
{
	uint32_t n_dupl = 0;
	as_partition_version parent_dupl_versions[ns->cluster_size];

	memset(parent_dupl_versions, 0, sizeof(parent_dupl_versions));

	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		// Skip 0 versions, and postpone subsets to next pass.
		if (as_partition_version_is_null(version) || version->subset == 1) {
			continue;
		}

		// Every unique version is a duplicate.
		if (version->family == VERSION_FAMILY_UNIQUE) {
			dupls[n_dupl++] = ns_node_seq[n];
			continue;
		}

		// Add parent versions as duplicates, unless they are already in.

		uint32_t d;

		for (d = 0; d < n_dupl; d++) {
			if (is_family_same(&parent_dupl_versions[d], version)) {
				break;
			}
		}

		if (d == n_dupl) {
			// Not in dupls.
			parent_dupl_versions[n_dupl] = *version;
			dupls[n_dupl++] = ns_node_seq[n];
		}
	}

	// Second pass to deal with subsets.
	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		if (version->subset == 0) {
			continue;
		}

		uint32_t d;

		for (d = 0; d < n_dupl; d++) {
			if (is_family_same(&parent_dupl_versions[d], version)) {
				break;
			}
		}

		if (d == n_dupl) {
			// Not in dupls.
			// Leave 0 in parent_dupl_versions array.
			dupls[n_dupl++] = ns_node_seq[n];
		}
	}

	// Remove working master from 'variants' to leave duplicates.
	return remove_node(dupls, n_dupl, ns_node_seq[working_master_n]);
}


void
fill_witnesses(as_partition* p, const cf_node* ns_node_seq,
		const uint32_t* ns_sl_ix, as_namespace* ns)
{
	for (uint32_t n = 1; n < ns->cluster_size; n++) {
		const as_partition_version* version = INPUT_VERSION(n);

		if (n < p->n_replicas || ! as_partition_version_is_null(version)) {
			p->witnesses[p->n_witnesses++] = ns_node_seq[n];
		}
	}
}


uint32_t
fill_immigrators(as_partition* p, const uint32_t* ns_sl_ix, as_namespace* ns,
		uint32_t working_master_n, uint32_t n_dupl)
{
	uint32_t n_immigrators = 0;

	for (uint32_t repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
		const as_partition_version* version = INPUT_VERSION(repl_ix);

		if (n_dupl != 0 || (repl_ix != working_master_n &&
				(as_partition_version_is_null(version) ||
						version->subset == 1))) {
			p->immigrators[repl_ix] = true;
			n_immigrators++;
		}
	}

	return n_immigrators;
}


void
advance_version(as_partition* p, const uint32_t* ns_sl_ix, as_namespace* ns,
		uint32_t self_n, uint32_t working_master_n, uint32_t n_dupl,
		const cf_node dupls[])
{
	// Fill family versions.

	uint32_t max_n_families = p->n_replicas + 1;

	if (max_n_families > AS_PARTITION_N_FAMILIES) {
		max_n_families = AS_PARTITION_N_FAMILIES;
	}

	as_partition_version family_versions[max_n_families];
	uint32_t n_families = fill_family_versions(p, ns_sl_ix, ns,
			working_master_n, n_dupl, dupls, family_versions);

	// Advance working master.
	if (self_n == working_master_n) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = (self_n == 0 || n_dupl == 0) ? 0 : 1;
		p->version.master = 1;
		p->version.subset = 0;
		p->version.evade = 0;

		return;
	}

	p->version.master = 0;

	// Advance eventual master.
	if (self_n == 0) {
		bool self_is_versionless = as_partition_version_is_null(&p->version);
		bool was_subset = p->version.subset == 1;

		p->version.ckey = p->final_version.ckey;
		p->version.family = 0;
		p->version.subset = n_dupl == 0 ? 1 : 0;

		if (self_is_versionless || (was_subset && p->version.subset == 0)) {
			p->version.evade = 1;
		}
		// else - don't change evade flag.

		return;
	}

	// Advance non-masters ...

	uint32_t family = find_family(&p->version, n_families, family_versions);

	// ... proles ...
	if (self_n < p->n_replicas) {
		bool self_is_versionless = as_partition_version_is_null(&p->version);

		p->version.ckey = p->final_version.ckey;
		p->version.family = family;

		if (self_is_versionless) {
			p->version.family = 0;
			p->version.subset = 1;
			p->version.evade = 1;
		}
		else if (n_dupl != 0 && p->version.family == 0) {
			p->version.subset = 1;
		}
		// else - don't change either subset or evade flag.

		return;
	}

	// ... or non-replicas.
	if (family != VERSION_FAMILY_UNIQUE &&
			family_versions[family].subset == 0) {
		p->version.ckey = p->final_version.ckey;
		p->version.family = family;
		p->version.subset = 1;
	}
	// else - leave version as-is.
}


uint32_t
fill_family_versions(const as_partition* p, const uint32_t* ns_sl_ix,
		const as_namespace* ns, uint32_t working_master_n, uint32_t n_dupl,
		const cf_node dupls[], as_partition_version family_versions[])
{
	uint32_t n_families = 1;
	const as_partition_version* final_master_version = INPUT_VERSION(0);

	family_versions[0] = *final_master_version;

	if (working_master_n != 0) {
		const as_partition_version* working_master_version =
				INPUT_VERSION(working_master_n);

		if (n_dupl == 0) {
			family_versions[0] = *working_master_version;
		}
		else {
			family_versions[0] = p->final_version; // not matchable
			family_versions[1] = *working_master_version;
			n_families = 2;
		}
	}

	for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
		if (repl_ix == working_master_n) {
			continue;
		}

		const as_partition_version* version = INPUT_VERSION(repl_ix);

		if (contains_node(dupls, n_dupl, p->replicas[repl_ix])) {
			family_versions[n_families++] = *version;
		}
		else if (version->subset == 1 &&
				! has_replica_parent(p, ns_sl_ix, ns, version, repl_ix)) {
			family_versions[n_families++] = *version;
		}
	}

	return n_families;
}


bool
has_replica_parent(const as_partition* p, const uint32_t* ns_sl_ix,
		const as_namespace* ns, const as_partition_version* subset_version,
		uint32_t subset_n)
{
	for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
		if (repl_ix == subset_n) {
			continue;
		}

		const as_partition_version* version = INPUT_VERSION(repl_ix);

		if (version->subset == 0 && is_family_same(version, subset_version)) {
			return true;
		}
	}

	return false;
}


uint32_t
find_family(const as_partition_version* self_version, uint32_t n_families,
		const as_partition_version family_versions[])
{
	for (uint32_t n = 0; n < n_families; n++) {
		if (is_family_same(self_version, &family_versions[n])) {
			return n;
		}
	}

	return VERSION_FAMILY_UNIQUE;
}


void
queue_namespace_migrations(as_partition* p, as_namespace* ns, uint32_t self_n,
		cf_node working_master, uint32_t n_dupl, cf_node dupls[], cf_queue* mq)
{
	partition_migrate_record pmr;

	if (self_n == 0) {
		// <><><><><><>  Final Master  <><><><><><>

		if (g_config.self_node == working_master) {
			p->pending_immigrations = (int)n_dupl;
		}
		else {
			// Remove self from duplicates.
			n_dupl = remove_node(dupls, n_dupl, g_config.self_node);

			p->origin = working_master;
			p->pending_immigrations = (int)n_dupl + 1;
		}

		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupls, dupls, n_dupl * sizeof(cf_node));
		}

		if (p->pending_immigrations != 0) {
			for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
				if (p->immigrators[repl_ix]) {
					p->pending_emigrations++;
				}
			}

			// Emigrate later, after all immigration is complete.
			return;
		}

		// Emigrate now, no immigrations to wait for.
		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->immigrators[repl_ix]) {
				p->pending_emigrations++;
				partition_migrate_record_fill(&pmr, p->replicas[repl_ix],
						ns, p->id, p->cluster_key, EMIG_TYPE_TRANSFER,
						TX_FLAGS_NONE);
				cf_queue_push(mq, &pmr);
			}
		}

		return;
	}
	// else - <><><><><><>  Not Final Master  <><><><><><>

	if (g_config.self_node == working_master) {
		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupls, dupls, n_dupl * sizeof(cf_node));
		}

		p->target = p->replicas[0];
		p->pending_emigrations = 1;
		partition_migrate_record_fill(&pmr, p->target, ns, p->id,
				p->cluster_key, EMIG_TYPE_TRANSFER, TX_FLAGS_ACTING_MASTER);
		cf_queue_push(mq, &pmr);
	}
	else if (contains_self(dupls, n_dupl)) {
		p->pending_emigrations = 1;
		partition_migrate_record_fill(&pmr, p->replicas[0], ns, p->id,
				p->cluster_key, EMIG_TYPE_TRANSFER, TX_FLAGS_NONE);
		cf_queue_push(mq, &pmr);
	}

	if (self_n < p->n_replicas && p->immigrators[self_n]) {
		p->origin = p->replicas[0];
		p->pending_immigrations = 1;
	}
}


//==========================================================
// Local helpers - migration-related as_partition methods.
//

// Sanity checks for immigrations commands.
bool
partition_immigration_is_valid(const as_partition* p, cf_node source_node,
		const as_namespace* ns, const char* tag)
{
	char* failure_reason = NULL;

	if (p->pending_immigrations == 0) {
		failure_reason = "no immigrations expected";
	}
	else if (is_self_final_master(p)) {
		if (p->origin != source_node &&
				! contains_node(p->dupls, p->n_dupl, source_node)) {
			failure_reason = "source not final master's origin or duplicate";
		}
	}
	else if (p->origin != (cf_node)0 && p->origin != source_node) {
		failure_reason = "source not prole's origin";
	}

	if (failure_reason) {
		cf_warning(AS_PARTITION, "{%s:%u} immigrate_%s - origin %lx source %lx pending-immigrations %d - %s",
				ns->name, p->id, tag, p->origin, source_node,
				p->pending_immigrations, failure_reason);

		return false;
	}

	return true;
}



//==============================================================================
// XXX JUMP - remove in "six months".
//

//==========================================================
// Public API - balance partitions.
//

// TODO - may want a non-jump version of this for manual version refreshing.
void
as_partition_balance_jump_versions()
{
	as_partition_version new_version = { .ckey = as_exchange_cluster_key() };

	// Piggy-back rack-aware conversion.
	uint32_t rack_id = old_is_rack_aware() ?
			(uint32_t)cc_compute_group_id(g_config.self_node): 0;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			pthread_mutex_lock(&p->lock);

			p->final_version = new_version;
			p->final_version.master = is_self_final_master(p) ? 1 : 0;

			// Cross-over from old vinfo world to new version world.
			if (! as_partition_is_null(&p->version_info)) {
				p->version = p->final_version;
				set_partition_version_in_storage(ns, p->id, &p->version, false);
			}

			p->state = AS_PARTITION_STATE_UNDEF;

			pthread_mutex_unlock(&p->lock);
		}

		as_storage_info_flush(ns);

		ns->rack_id = rack_id;
	}
}


// If we do not encounter other nodes at startup, all initially ABSENT
// partitions are assigned a new version and converted to SYNC.
void
as_partition_balance_init_single_node_cluster()
{
	as_partition_vinfo new_vinfo;

	generate_new_partition_vinfo(&new_vinfo);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->replication_factor = 1;

		uint32_t n_promoted = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			// For defrag, which is allowed to operate while we're doing this.
			pthread_mutex_lock(&p->lock);

			p->cluster_key = as_exchange_cluster_key();
			p->witnesses[0] = g_config.self_node;

			if (as_partition_is_null(&p->version_info)) {
				p->n_replicas = 1;
				p->replicas[0] = g_config.self_node;

				p->primary_version_info = new_vinfo;
				p->version_info = new_vinfo;
				p->state = AS_PARTITION_STATE_SYNC;

				ns->cluster_vinfo[0][pid] = new_vinfo;

				set_partition_vinfo_in_storage(ns, pid, &new_vinfo, false);

				n_promoted++;
			}

			client_replica_maps_update(ns, pid);

			pthread_mutex_unlock(&p->lock);
		}

		if (n_promoted != 0) {
			as_storage_info_flush(ns);
		}

		cf_info(AS_PARTITION, "{%s} %u absent partitions promoted to master",
				ns->name, n_promoted);
	}

	// Ok to allow transactions.
	g_init_balance_done = true;
	cf_atomic32_incr(&g_partition_generation);
}


// If this node encounters other nodes at startup, prevent it from switching to
// a single-node cluster - any initially ABSENT partitions participate in
// clustering as ABSENT.
void
as_partition_balance_init_multi_node_cluster()
{
	g_multi_node = true;
}


// Has this node encountered other nodes?
bool
as_partition_balance_is_multi_node_cluster()
{
	return g_multi_node;
}


//==========================================================
// Public API - migration-related as_partition methods.
//

void
old_as_partition_emigrate_done(as_migrate_state s, as_namespace* ns,
		uint32_t pid, uint64_t orig_cluster_key, uint32_t tx_flags)
{
	// TODO - better handled outside?
	if (s != AS_MIGRATE_STATE_DONE) {
		if (s == AS_MIGRATE_STATE_ERROR) {
			if (orig_cluster_key == as_exchange_cluster_key()) {
				cf_warning(AS_PARTITION, "{%s:%d} emigrate done: failed with error and cluster key is current",
						ns->name, pid);
			}
		}
		else if (s == AS_MIGRATE_STATE_EAGAIN) {
			cf_warning(AS_PARTITION, " {%s:%d} emigrate done: migrate failed",
					ns->name, pid);
		}
		else {
			cf_warning(AS_PARTITION, " {%s:%d} emigrate done: unknown notification %d",
					ns->name, pid, (int)s);
		}

		return;
	}

	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return;
	}

	bool acting_master = (tx_flags & TX_FLAGS_ACTING_MASTER) != 0;
	bool migration_request = (tx_flags & TX_FLAGS_REQUEST) != 0;

	// Flush writes on the acting master now that it has completed filling the
	// real master with data.
	if (acting_master) {
		p->target = 0;
		p->n_dupl = 0;
		memset(p->dupls, 0, sizeof(p->dupls));
	}

	if (p->pending_emigrations == 0) {
		cf_warning(AS_PARTITION, "{%s:%d} concurrency event - paxos reconfiguration occurred during migrate_tx?",
				ns->name, pid);
		pthread_mutex_unlock(&p->lock);
		return;
	}

	p->pending_emigrations--;

	if (p->pending_emigrations != 0 && g_config.self_node != p->replicas[0]) {
		cf_warning(AS_PARTITION, "{%s:%d} emigrate done: not final master - should have done exactly 1 emigration, but %d remain",
				ns->name, pid, p->pending_emigrations);
		pthread_mutex_unlock(&p->lock);
		return;
	}

	if (! migration_request) {
		int64_t migrates_tx_remaining = cf_atomic_int_decr(
				&ns->migrate_tx_partitions_remaining);

		if (migrates_tx_remaining < 0){
			cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld) tx partitions schedule exceeded, possibly a race with prior migration",
					ns->name, pid, p->pending_emigrations,
					migrates_tx_remaining);
		}
	}

	p->current_outgoing_ldt_version = 0;

	if (p->state == AS_PARTITION_STATE_ZOMBIE) {
		set_partition_absent_lockfree(p, ns, true);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	pthread_mutex_unlock(&p->lock);
}


as_migrate_result
old_as_partition_immigrate_start(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t start_type, cf_node source_node)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	uint32_t num_incoming = (uint32_t)cf_atomic32_incr(&g_migrate_num_incoming);

	if (num_incoming > g_config.migrate_max_num_incoming) {
		cf_atomic32_decr(&g_migrate_num_incoming);
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	as_migrate_result rv = AS_MIGRATE_FAIL;
	cf_queue mq;

	cf_queue_init(&mq, sizeof(partition_migrate_record),
			ns->cfg_replication_factor, false);

	switch (p->state) {
	case AS_PARTITION_STATE_DESYNC:
		rv = AS_MIGRATE_OK;
		break;
	case AS_PARTITION_STATE_SYNC:
	case AS_PARTITION_STATE_ZOMBIE:
		if (! as_new_clustering()) {
			if (g_config.self_node != p->replicas[0]) {
				if (start_type == 0) {
					// Start message from old node. Note that this still has a
					// bug for replication factor > 2, where subsequent normal
					// starts masquerade as duplicate request-type starts.
					start_type = MIG_TYPE_START_IS_NORMAL;

					if (p->has_master_wait) {
						start_type = MIG_TYPE_START_IS_REQUEST;
					}
				}
			}
		}

		// FIXME - Should we send a new command for request instead of sending a
		// migration start?
		if (start_type == MIG_TYPE_START_IS_REQUEST) {
			if (source_node != p->replicas[0]) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_start aborted - waiting node received migrate request from non-master",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			p->pending_emigrations++;

			partition_migrate_record r;
			partition_migrate_record_fill(&r, p->replicas[0], ns, pid,
					orig_cluster_key, EMIG_TYPE_TRANSFER, TX_FLAGS_NONE);
			cf_queue_push(&mq, &r);

			rv = AS_MIGRATE_ALREADY_DONE;
			break;
		}

		if (p->state == AS_PARTITION_STATE_ZOMBIE) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_start - in zombie state %d, fail",
					ns->name, pid, p->state);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		if (g_config.self_node != p->replicas[0]) {
			bool is_replica = false;

			for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
				if (g_config.self_node == p->replicas[repl_ix]) {
					is_replica = true;
					break; // out of for loop
				}
			}

			if (! is_replica) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_start aborted - non replica node received migrate request",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			if (source_node != p->replicas[0]) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_start aborted - sync replica node received migrate request from non-master",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			if (p->origin != p->replicas[0]) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_start aborted - sync replica node receiving migrate request has origin set to non-master",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			// The only place we switch to DESYNC outside of re-balance.
			p->state = AS_PARTITION_STATE_DESYNC;
		}
		else {
			if (p->origin != (cf_node)0) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_start aborted - sync master has origin set to non-null",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			bool dupl_node_found = false;

			for (uint32_t dupl_ix = 0; dupl_ix < p->n_dupl; dupl_ix++) {
				if (source_node == p->dupls[dupl_ix]) {
					dupl_node_found = true;
					break;
				}
			}

			if (! dupl_node_found) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_start aborted - sync master receiving migrate from node not in duplicate list",
						ns->name, pid);
				rv = AS_MIGRATE_FAIL;
				break;
			}
		}

		rv = AS_MIGRATE_OK;
		break;
	default:
		cf_warning(AS_PARTITION, "{%s:%d} immigrate_start - in state %d, fail",
				ns->name, pid, p->state);
		rv = AS_MIGRATE_FAIL;
		break;
	}

	if (rv != AS_MIGRATE_OK) {
		// Migration has been rejected, incoming migration not expected.
		cf_atomic32_decr(&g_migrate_num_incoming);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	pthread_mutex_unlock(&p->lock);

	partition_migrate_record pmr;

	while (CF_QUEUE_OK == cf_queue_pop(&mq, &pmr, 0)) {
		as_migrate_emigrate(&pmr);
	}

	cf_queue_destroy(&mq);

	return rv;
}


as_migrate_result
old_as_partition_immigrate_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, cf_node source_node)
{
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (! g_allow_migrations || orig_cluster_key != as_exchange_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	if (p->pending_immigrations == 0) {
		cf_warning(AS_PARTITION, "{%s:%d} immigrate_done concurrency event - paxos reconfiguration occurred during migrate_done?",
				ns->name, pid);
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	as_migrate_result rv = AS_MIGRATE_FAIL;
	as_partition_state orig_p_state = p->state;
	cf_queue mq;

	cf_queue_init(&mq, sizeof(partition_migrate_record),
			ns->cfg_replication_factor, false);

	switch (orig_p_state) {
	case AS_PARTITION_STATE_DESYNC:
		if (p->origin != source_node) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - state error for desync partition",
					ns->name, pid);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		p->pending_immigrations--;

		int64_t migrates_rx_remaining = cf_atomic_int_decr(
				&ns->migrate_rx_partitions_remaining);

		if (migrates_rx_remaining < 0) {
			cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld) immigrate_done - partitions schedule exceeded, possibly a race with prior migration",
					ns->name, pid, p->pending_immigrations,
					migrates_rx_remaining);
		}

		p->origin = 0;

		set_partition_sync_lockfree(p, ns, true);

		// If this is not the final master, we are done.
		if (g_config.self_node != p->replicas[0]) {
			if (p->pending_immigrations != 0) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - rx %d is non zero",
						ns->name, pid, p->pending_immigrations);
				rv = AS_MIGRATE_FAIL;
				break;
			}

			rv = AS_MIGRATE_OK;
			break;
		}
		// else - a DESYNC master has just become SYNC.

		if (p->pending_emigrations != 0) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - tx %d value is non-zero for master that just turned sync after migrate",
					ns->name, pid, p->pending_emigrations);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		// Send migrate messages to request migrations, if needed.
		for (uint32_t dupl_ix = 0; dupl_ix < p->n_dupl; dupl_ix++) {
			p->pending_emigrations++;

			partition_migrate_record r;
			partition_migrate_record_fill(&r, p->dupls[dupl_ix],
					ns, pid, orig_cluster_key, EMIG_TYPE_TRANSFER,
					TX_FLAGS_REQUEST);

			cf_queue_push(&mq, &r);
		}

		if (p->pending_emigrations != p->pending_immigrations) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - rx %d and tx %d values mismatch",
					ns->name, pid, p->pending_immigrations, p->pending_emigrations);
			rv = AS_MIGRATE_FAIL;
			break;
		}

	// No break - the state is sync now.
	case AS_PARTITION_STATE_SYNC:
		if (g_config.self_node != p->replicas[0]) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - state error for sync partition",
					ns->name, pid);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		if (p->n_dupl != 0) {
			bool found = false;
			uint32_t dupl_ix;

			for (dupl_ix = 0; dupl_ix < p->n_dupl; dupl_ix++) {
				if (p->dupls[dupl_ix] == source_node) {
					found = true;
					break;
				}
			}

			if (found) {
				uint32_t last_dupl_ix = p->n_dupl - 1;

				if (dupl_ix == last_dupl_ix) { // delete last entry
					p->dupls[dupl_ix] = (cf_node)0;
				}
				else { // copy last entry into deleted entry
					p->dupls[dupl_ix] = p->dupls[last_dupl_ix];
					p->dupls[last_dupl_ix] = (cf_node)0;
				}

				p->n_dupl--;
				p->pending_immigrations--;

				int64_t migrates_rx_remaining = cf_atomic_int_decr(
						&ns->migrate_rx_partitions_remaining);

				if (migrates_rx_remaining < 0) {
					cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld) immigrate_done - partitions schedule exceeded, possibly a race with prior migration",
							ns->name, pid, p->pending_immigrations,
							migrates_rx_remaining);
				}
			}
			else {
				// We get here when DESYNC (empty) master becomes SYNC and there
				// were duplicates. The first sync node is not a member of the
				// dupl_nodes array.

				if (orig_p_state != AS_PARTITION_STATE_DESYNC) {
					cf_warning(AS_PARTITION, "{%s:%d} immigrate_done - source node %"PRIx64" not found in desync state",
							ns->name, pid, source_node);
					rv = AS_MIGRATE_FAIL;
					break;
				}
			}
		}
		// else - no duplicates - we get here e.g. if master & prole(s) are
		// DESYNC and there is a single partition version coming from a zombie
		// to here - the DESYNC master. Might now do migration(s) to prole(s) -
		// don't break!

		if (p->pending_immigrations > 0) {
			rv = AS_MIGRATE_OK;
			break;
		}
		// else - received all expected, send anything pending as needed.

		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->immigrators[repl_ix]) {
				p->immigrators[repl_ix] = false;
				p->pending_emigrations++;

				partition_migrate_record r;
				partition_migrate_record_fill(&r, p->replicas[repl_ix],
						ns, pid, orig_cluster_key, EMIG_TYPE_TRANSFER,
						TX_FLAGS_NONE);

				cf_queue_push(&mq, &r);
			}
		}

		rv = AS_MIGRATE_OK;
		break;
	default:
		cf_warning(AS_PARTITION, "{%s:%u} immigrate_done received with bad state partition: %u ",
				ns->name, pid, p->state);
		rv = AS_MIGRATE_FAIL;
		break;
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	pthread_mutex_unlock(&p->lock);

	partition_migrate_record pmr;

	while (0 == cf_queue_pop(&mq, &pmr, 0)) {
		as_migrate_emigrate(&pmr);
	}

	cf_queue_destroy(&mq);

	// For receiver-side migration flow control.
	cf_atomic32_decr(&g_migrate_num_incoming);

	return rv;
}


//==========================================================
// Local helpers - generic.
//

void
set_partition_vinfo_in_storage(as_namespace* ns, uint32_t pid,
		const as_partition_vinfo* vinfo, bool flush)
{
	as_storage_info_set(ns, pid, vinfo);

	if (flush) {
		as_storage_info_flush(ns);
	}
}


void
generate_new_partition_vinfo(as_partition_vinfo* new_vinfo)
{
	*new_vinfo = NULL_VINFO;
	new_vinfo->iid = as_exchange_cluster_key();
	new_vinfo->vtp[0] = 1;
}


void
set_partition_sync_lockfree(as_partition* p, as_namespace* ns, bool flush)
{
	p->state = AS_PARTITION_STATE_SYNC;
	p->version_info = p->primary_version_info;
	set_partition_vinfo_in_storage(ns, p->id, &p->version_info, flush);
}


void
set_partition_desync_lockfree(as_partition* p, as_namespace* ns, bool flush)
{
	p->state = AS_PARTITION_STATE_DESYNC;
	p->version_info = NULL_VINFO;
	set_partition_vinfo_in_storage(ns, p->id, &NULL_VINFO, flush);

	drop_trees(p, ns);
}


void
set_partition_absent_lockfree(as_partition* p, as_namespace* ns, bool flush)
{
	p->state = AS_PARTITION_STATE_ABSENT;
	p->version_info = NULL_VINFO;
	set_partition_vinfo_in_storage(ns, p->id, &NULL_VINFO, flush);

	drop_trees(p, ns);

	p->current_outgoing_ldt_version = 0;
}


//==========================================================
// Local helpers - balance partitions.
//

void
old_balance_namespace(cf_node* full_node_seq_table, uint32_t* full_sl_ix_table,
		as_namespace* ns, cf_queue* mq)
{
	// Generate the new partition version based on the cluster key and use this
	// for any newly initialized partition.
	as_partition_vinfo new_vinfo;

	generate_new_partition_vinfo(&new_vinfo);

	// Figure out effective replication factor in the face of node failures.
	apply_single_replica_limit(ns);

	int ns_pending_immigrations = 0;
	int ns_pending_emigrations = 0;
	int ns_delayed_emigrations = 0;
	uint32_t ns_fresh_partitions = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition* p = &ns->partitions[pid];

		cf_node* full_node_seq = &FULL_NODE_SEQ(pid, 0);
		uint32_t* full_vinfo_index = &FULL_SL_IX(pid, 0);

		// Usually a namespace can simply use the global tables...
		cf_node* ns_node_seq = full_node_seq;
		uint32_t* ns_vinfo_index = full_vinfo_index;

		if (old_is_rack_aware()) {
			old_rack_aware_adjust_rows(ns_node_seq, ns_vinfo_index, ns);
		}

		pthread_mutex_lock(&p->lock);

		uint32_t old_repl_factor = p->n_replicas;

		p->n_replicas = ns->replication_factor;
		memset(p->replicas, 0, sizeof(p->replicas));
		memcpy(p->replicas, ns_node_seq, p->n_replicas * sizeof(cf_node));

		p->cluster_key = as_exchange_cluster_key();

		// Interrupted migrations - start over.
		if (p->state == AS_PARTITION_STATE_DESYNC &&
				! as_partition_is_null(&p->version_info)) {
			p->state = AS_PARTITION_STATE_SYNC;
		}

		p->has_master_wait = false;
		p->pending_emigrations = 0;
		p->pending_immigrations = 0;
		memset(p->immigrators, 0,
				sizeof(p->immigrators));

		p->origin = (cf_node)0;
		p->target = (cf_node)0;

		p->n_dupl = 0;
		memset(p->dupls, 0, sizeof(p->dupls));

		p->current_outgoing_ldt_version = 0;

		uint32_t n_dupl = 0;
		cf_node dupl_nodes[ns->cluster_size]; // nodes with duplicate versions

		memset(&dupl_nodes, 0, sizeof(dupl_nodes));

		bool has_version[ns->cluster_size];
		int self_n = -1;
		int first_versioned_n = set_primary_version(p, ns_node_seq,
				ns_vinfo_index, ns, has_version, &self_n);

		if (first_versioned_n == -1) {
			first_versioned_n = 0;
			p->primary_version_info = new_vinfo;

			handle_lost_partition(p, ns_node_seq, ns, has_version);
			ns_fresh_partitions++;
		}
		else {
			n_dupl = old_find_duplicates(p, ns_node_seq, ns_vinfo_index, ns,
					dupl_nodes);

			if (should_advance_version(p, old_repl_factor, ns)) {
				old_advance_version(p, ns_node_seq, ns_vinfo_index, ns,
						first_versioned_n, &new_vinfo);
			}
		}

		old_queue_namespace_migrations(p, ns_node_seq, ns, self_n,
				first_versioned_n, has_version, n_dupl, dupl_nodes, mq,
				&ns_delayed_emigrations);

		// Copy the new node sequence over the old node sequence.
		memset(p->witnesses, 0, sizeof(p->witnesses));
		memcpy(p->witnesses, ns_node_seq, sizeof(cf_node) * ns->cluster_size);

		ns_pending_immigrations += p->pending_immigrations;
		ns_pending_emigrations += p->pending_emigrations;

		client_replica_maps_update(ns, pid);

		pthread_mutex_unlock(&p->lock);
	}

	int ns_all_pending_emigrations = ns_pending_emigrations +
			ns_delayed_emigrations;

	cf_info(AS_PARTITION, "{%s} re-balanced, expected migrations - (%d tx, %d rx)",
			ns->name, ns_all_pending_emigrations, ns_pending_immigrations);

	if (ns_fresh_partitions != 0) {
		cf_info(AS_PARTITION, "{%s} fresh-partitions %u", ns->name,
				ns_fresh_partitions);
	}

	ns->migrate_tx_partitions_initial = (uint64_t)ns_all_pending_emigrations;
	ns->migrate_tx_partitions_remaining = (uint64_t)ns_all_pending_emigrations;

	ns->migrate_rx_partitions_initial = (uint64_t)ns_pending_immigrations;
	ns->migrate_rx_partitions_remaining = (uint64_t)ns_pending_immigrations;
}


void
old_rack_aware_adjust_rows(cf_node* ns_node_seq, uint32_t* ns_sl_ix,
		const as_namespace* ns)
{
	uint32_t n_groups = g_config.cluster.group_count;
	uint32_t n_needed = n_groups < ns->replication_factor ?
			n_groups : ns->replication_factor;

	uint32_t next_n = n_needed; // next candidate index to swap with

	for (uint32_t cur_n = 1; cur_n < n_needed; cur_n++) {
		cf_node cur_node = ns_node_seq[cur_n];
		cc_group_t cur_group_id = cc_compute_group_id(cur_node);

		if (cur_node == (cf_node)0) {
			cf_crash(AS_PARTITION, "null node found within cluster_size");
		}

		// If cur_group is unique for nodes < cur_i, continue to next node.
		if (old_is_group_distinct_before_n(ns_node_seq, cur_group_id, cur_n)) {
			continue;
		}

		// Find group after cur_i that's unique for groups before cur_i.
		uint32_t swap_n = cur_n; // if swap cannot be found then no change

		while (next_n < ns->cluster_size) {
			cf_node next_node = ns_node_seq[next_n];
			cc_group_t next_group_id = cc_compute_group_id(next_node);

			if (next_node == (cf_node)0) {
				cf_crash(AS_PARTITION, "null node found within cluster_size");
			}

			if (old_is_group_distinct_before_n(ns_node_seq, next_group_id,
					cur_n)) {
				swap_n = next_n;
				next_n++;
				break;
			}

			next_n++;
		}

		if (swap_n == cur_n) {
			// No other distinct groups found - shouldn't be possible.
			// We should reach n_needed first.
			cf_crash(AS_PARTITION, "can't find a diff cur:%u swap:%u repl:%u clsz:%u",
					cur_n, swap_n, ns->replication_factor, ns->cluster_size);
		}

		// Now swap cur_n with swap_n.

		// Swap node.
		cf_node temp_node = ns_node_seq[swap_n];

		ns_node_seq[swap_n] = ns_node_seq[cur_n];
		ns_node_seq[cur_n] = temp_node;

		// Swap succession list index.
		uint32_t temp_ix = ns_sl_ix[swap_n];

		ns_sl_ix[swap_n] = ns_sl_ix[cur_n];
		ns_sl_ix[cur_n] = temp_ix;
	}
}


// Returns true if group_id is unique within nodes list indices less than n.
bool
old_is_group_distinct_before_n(const cf_node* ns_node_seq, cc_node_t group_id,
		uint32_t n)
{
	for (uint32_t cur_n = 0; cur_n < n; cur_n++) {
		cc_node_t cur_group_id = cc_compute_group_id(ns_node_seq[cur_n]);

		if (cur_group_id == group_id) {
			return false;
		}
	}

	return true;
}


int
set_primary_version(as_partition* p, const cf_node* ns_node_seq,
		const uint32_t* ns_vinfo_index, as_namespace* ns, bool has_version[],
		int* self_n)
{
	int first_versioned_n = -1;

	for (int n = 0; n < (int)ns->cluster_size; n++) {
		if (ns_node_seq[n] == g_config.self_node) {
			*self_n = n;
		}

		uint32_t vi_ix = ns_vinfo_index[n];
		as_partition_vinfo* vinfo = &ns->cluster_vinfo[vi_ix][p->id];

		if (as_partition_is_null(vinfo)) {
			has_version[n] = false;
		}
		else {
			has_version[n] = true;

			if (first_versioned_n == -1) {
				first_versioned_n = n;
				p->primary_version_info = *vinfo;
			}
		}
	}

	return first_versioned_n;
}


void
handle_lost_partition(as_partition* p, const cf_node* ns_node_seq,
		as_namespace* ns, bool has_version[])
{
	for (uint32_t repl_ix = 0; repl_ix < p->n_replicas; repl_ix++) {
		// Each replica initializes its partition version to the same new value.
		if (ns_node_seq[repl_ix] == g_config.self_node) {
			drop_trees(p, ns);
			set_partition_sync_lockfree(p, ns, false);
		}

		has_version[repl_ix] = true;
	}
}


uint32_t
old_find_duplicates(const as_partition* p, const cf_node* ns_node_seq,
		const uint32_t* ns_vinfo_index, const as_namespace* ns,
		cf_node dupl_nodes[])
{
	uint32_t n_dupl = 0;
	as_partition_vinfo dupl_pvinfo[ns->cluster_size];

	memset(dupl_pvinfo, 0, sizeof(dupl_pvinfo));

	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		uint32_t vi_ix = ns_vinfo_index[n];
		const as_partition_vinfo* vinfo = &ns->cluster_vinfo[vi_ix][p->id];

		// If this partition version is unique, add to duplicates list.

		if (as_partition_is_null(vinfo) ||
				as_partition_vinfo_same(&p->primary_version_info, vinfo)) {
			continue;
		}

		uint32_t dupl_ix;

		for (dupl_ix = 0; dupl_ix < n_dupl; dupl_ix++) {
			if (as_partition_vinfo_same(&dupl_pvinfo[dupl_ix], vinfo)) {
				break;
			}
		}

		if (dupl_ix == n_dupl) {
			// Unique - didn't match primary version or any duplicate.
			dupl_nodes[n_dupl] = ns_node_seq[n];
			dupl_pvinfo[n_dupl] = *vinfo;
			n_dupl++;
		}
	}

	return n_dupl;
}


// For this partition, check if any replicas in the old node sequence are
// missing from the new succession list.
bool
should_advance_version(const as_partition* p, uint32_t old_repl_factor,
		as_namespace* ns)
{
	uint32_t cluster_size = ns->cluster_size;
	cf_node* succession = ns->succession;

	for (uint32_t repl_ix = 0; repl_ix < old_repl_factor; repl_ix++) {
		if (p->witnesses[repl_ix] == 0) {
			return false;
		}

		uint32_t n;

		for (n = 0; n < cluster_size; n++) {
			if (p->witnesses[repl_ix] == succession[n]) {
				break;
			}
		}

		if (n == cluster_size) {
			// Old node missing from new succession list.
			return true;
		}
	}

	return false;
}


void
old_advance_version(as_partition* p, const cf_node* ns_node_seq,
		const uint32_t* ns_vinfo_index, as_namespace* ns, int first_versioned_n,
		const as_partition_vinfo* new_vinfo)
{
	// Find the first versioned node in the old node sequence.
	cf_node first_versioned_node = ns_node_seq[first_versioned_n];
	int n;

	for (n = 0; n < AS_CLUSTER_SZ; n++) {
		if (p->witnesses[n] == (cf_node)0) {
			return;
		}

		if (p->witnesses[n] == first_versioned_node) {
			// n is first versioned node's index in old node sequence.
			break;
		}
	}

	// First versioned node not in old node sequence - leave version as is.
	if (n == AS_CLUSTER_SZ) {
		return;
	}

	uint32_t vi_ix = ns_vinfo_index[first_versioned_n];

	as_partition_vinfo adv_vinfo = ns->cluster_vinfo[vi_ix][p->id];
	int vtp_ix;

	for (vtp_ix = 0; vtp_ix < AS_PARTITION_MAX_VERSION; vtp_ix++) {
		if (adv_vinfo.vtp[vtp_ix] == 0) {
			adv_vinfo.vtp[vtp_ix] = (uint8_t)n + 1;
			break;
		}
	}

	// If we run out of space generate a completely new version.
	if (vtp_ix == AS_PARTITION_MAX_VERSION) {
		adv_vinfo = *new_vinfo;
	}

	if (as_partition_vinfo_same(&p->version_info,
			&ns->cluster_vinfo[vi_ix][p->id])) {
		set_partition_vinfo_in_storage(ns, p->id, &adv_vinfo, false);
		p->version_info = adv_vinfo;
	}

	p->primary_version_info = adv_vinfo;
}


void
old_queue_namespace_migrations(as_partition* p, const cf_node* ns_node_seq,
		as_namespace* ns, int self_n, int first_versioned_n,
		const bool has_version[], uint32_t n_dupl, const cf_node dupl_nodes[],
		cf_queue* mq, int* ns_delayed_emigrations)
{
	partition_migrate_record pmr;

	if (self_n == 0) {
		// <><><><><><>  Final Master  <><><><><><>

		if (has_version[0]) {
			// Final master is also (immediately) acting master.
			p->state = AS_PARTITION_STATE_SYNC;
		}
		else {
			// Final master is not yet acting master, it's eventual master.
			// Partition will be sent from origin, which is acting master.
			p->pending_immigrations++;
			p->origin = ns_node_seq[first_versioned_n];
			set_partition_desync_lockfree(p, ns, false);
		}

		// Final master expects migrations from each (unique) duplicate.
		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupls, dupl_nodes, sizeof(cf_node) * n_dupl);
			p->pending_immigrations += (int)n_dupl;
		}

		// If no expected immigrations, schedule emigrations to versionless
		// replicas right away.
		if (p->pending_immigrations == 0) {
			for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
				if (! has_version[repl_ix]) {
					partition_migrate_record_fill(&pmr, ns_node_seq[repl_ix],
							ns, p->id, p->cluster_key, EMIG_TYPE_TRANSFER,
							TX_FLAGS_NONE);
					cf_queue_push(mq, &pmr);
					p->pending_emigrations++;
				}
			}

			return;
		}

		// Expecting immigrations - schedule delayed emigrations of merged
		// partition to all replicas (if there are duplicates) or versionless
		// replicas (if there are no duplicates).
		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->n_dupl != 0 || ! has_version[repl_ix]) {
				p->immigrators[repl_ix] = true;
				(*ns_delayed_emigrations)++;
			}
		}

		return;
	}
	// else - <><><><><><>  Not Final Master  <><><><><><>

	// If this node has no version, nothing to emigrate.
	if (! has_version[self_n]) {
		if ((uint32_t)self_n < p->n_replicas) {
			// This node is a replica - expect immigration from final master.
			p->origin = ns_node_seq[0];
			set_partition_desync_lockfree(p, ns, false);
			p->pending_immigrations++;
		}
		else {
			// Not a replica, becomes absent.
			set_partition_absent_lockfree(p, ns, false);
		}

		return;
	}

	bool has_delayed_emigration = false;

	// If this node is acting master, schedule emigration to final master
	// immediately ...
	if (self_n == first_versioned_n) {
		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupls, dupl_nodes, sizeof(cf_node) * n_dupl);
		}

		p->target = ns_node_seq[0]; // only acting master sets p->target

		partition_migrate_record_fill(&pmr, ns_node_seq[0], ns, p->id,
				p->cluster_key, EMIG_TYPE_TRANSFER, TX_FLAGS_ACTING_MASTER);
		cf_queue_push(mq, &pmr);
		p->pending_emigrations++;
	}
	else if (contains_self(dupl_nodes, n_dupl)) {
		// ... If this node is a duplicate with a versioned final master,
		// schedule emigration to final master immediately ...
		if (has_version[0]) {
			partition_migrate_record_fill(&pmr, ns_node_seq[0], ns, p->id,
					p->cluster_key, EMIG_TYPE_TRANSFER, TX_FLAGS_NONE);
			cf_queue_push(mq, &pmr);
			p->pending_emigrations++;
		}
		// ... If this node is a duplicate with an eventual master, schedule
		// delayed emigration - eventual master signals when it gets a version.
		else {
			p->has_master_wait = true;
			has_delayed_emigration = true;
			(*ns_delayed_emigrations)++;
		}
	}

	// If this node is a replica and there are duplicates, expect immigration
	// from final master.
	if ((uint32_t)self_n < p->n_replicas) {
		if (n_dupl != 0) {
			p->origin = ns_node_seq[0];
			p->pending_immigrations++;
		}

		p->state = AS_PARTITION_STATE_SYNC;

		return;
	}

	// This node is not a replica. Partition will enter zombie state if it has
	// pending emigrations. Otherwise, we discard the partition.
	if (p->pending_emigrations || has_delayed_emigration) {
		p->state = AS_PARTITION_STATE_ZOMBIE;
	}
	else {
		set_partition_absent_lockfree(p, ns, false);
	}
}
