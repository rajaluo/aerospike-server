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
#include "citrusleaf/cf_queue.h"

#include "fault.h"
#include "util.h"

#include "base/cluster_config.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/migrate.h"
#include "fabric/partition.h"
#include "fabric/paxos.h"
#include "storage/storage.h"


//==========================================================
// Constants and typedefs.
//

// The instantaneous maximum number of cluster participants, represented as a
// positive and negative mask.
#define AS_CLUSTER_SZ_MASKP ((uint64_t)(1 - (AS_CLUSTER_SZ + 1)))
#define AS_CLUSTER_SZ_MASKN ((uint64_t)(AS_CLUSTER_SZ - 1))

#define BALANCE_INIT_UNRESOLVED 0
#define BALANCE_INIT_RESOLVED   1

// Define the macros for accessing the HV and hv_slindex arrays.
#define NODE_SEQ(x, y) node_seq_table[(x * g_paxos->cluster_size) + y]
#define SL_IX(x, y) succession_index_table[(x * g_paxos->cluster_size) + y]

typedef struct inter_hash_s {
	uint64_t hashed_pid;
	uint64_t hashed_node;
} inter_hash;


//==========================================================
// Globals.
//

cf_atomic32 g_partition_generation = 0;

static cf_atomic32 g_migrate_num_incoming = 0;

// Using int for 4-byte size, but maintaining bool semantics.
static volatile int g_allow_migrations = true;
static volatile int g_multi_node = false;

static volatile int g_balance_init = BALANCE_INIT_UNRESOLVED;
static uint64_t g_hashed_pids[AS_PARTITIONS];


//==========================================================
// Forward declarations.
//

void set_partition_version_in_storage(as_namespace* ns, uint32_t pid, const as_partition_vinfo* vinfo, bool flush);
void generate_new_partition_version(as_partition_vinfo* new_vinfo);
void apply_single_replica_limit(int new_cluster_size);
void partition_cluster_topology_info();
void fill_node_sequence_table(cf_node* node_seq_table, int* succession_index_table);
void adjust_node_sequence_table(cf_node* node_seq_table, int* succession_index_table, uint32_t repl_factor);
bool is_group_distinct_before_n(uint32_t pid, const cf_node* node_seq_table, const int* succession_index_table, cc_node_t group_id, uint32_t n);
int set_primary_version(as_partition* p, const cf_node* node_seq_table, const int* succession_index_table, as_namespace* ns, bool has_version[], int* self_n);
void handle_lost_partition(as_partition* p, const cf_node* node_seq_table, as_namespace* ns, bool has_version[]);
uint32_t find_duplicates(const as_partition* p, const cf_node* node_seq_table, const int* succession_index_table, const as_namespace* ns, cf_node dupl_nodes[]);
bool should_advance_version(const as_partition* p, uint32_t old_repl_factor);
void advance_version(as_partition* p, const cf_node* node_seq_table, const int* succession_index_table, as_namespace* ns, int first_versioned_n);
void queue_namespace_migrations(as_partition* p, const cf_node* node_seq_table,
		as_namespace* ns, int self_n, int first_versioned_n,
		const bool has_version[], uint32_t n_dupl, const cf_node dupl_nodes[],
		cf_queue* mq, int* ns_delayed_emigrations);
void partition_migrate_record_fill(partition_migrate_record* pmr, cf_node dest, as_namespace* ns, uint32_t pid, uint64_t cluster_key, uint32_t tx_flags);

void set_partition_sync_lockfree(as_partition* p, as_namespace* ns, bool flush);
void set_partition_desync_lockfree(as_partition* p, as_namespace* ns, bool flush);
void set_partition_absent_lockfree(as_partition* p, as_namespace* ns, bool flush);
void drop_trees(as_partition* p, as_namespace* ns);


//==========================================================
// Public API - regulate migrations.
//

void
as_partition_balance_allow_migrations()
{
	cf_info(AS_PARTITION, "ALLOW MIGRATIONS");
	g_allow_migrations = true;
}


void
as_partition_balance_disallow_migrations()
{
	cf_info(AS_PARTITION, "DISALLOW MIGRATIONS");
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
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

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

// Initially, every partition is either ABSENT, or a version was read from
// storage and it is SYNC.
void
as_partition_balance_init()
{
	// Cache hashed pids for all future rebalances.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		g_hashed_pids[pid] = cf_hash_fnv(&pid, sizeof(int));
	}

	g_paxos->cluster_size = 1;
	as_paxos_set_cluster_integrity(g_paxos, true);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		ns->replication_factor = 1;

		uint32_t n_stored = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			p->n_replicas = 1;
			p->replicas[0] = g_config.self_node;

			if (! as_partition_is_null(&p->version_info)) {
				p->primary_version_info = p->version_info;
				n_stored++;
			}
			else {
				// Stores the vinfo length, even when the vinfo is zeroed.
				set_partition_version_in_storage(ns, pid, &NULL_VINFO, false);
			}

			p->old_sl[0] = g_config.self_node;
			p->cluster_key = as_paxos_get_cluster_key();
		}

		if (n_stored < AS_PARTITIONS) {
			as_storage_info_flush(ns);
		}

		cf_info(AS_PARTITION, "{%s} %u partitions: found %u absent, %u stored",
				ns->name, AS_PARTITIONS, AS_PARTITIONS - n_stored, n_stored);
	}
}


// If we do not encounter other nodes at startup, all initially ABSENT
// partitions are assigned a new version and converted to SYNC.
void
as_partition_balance_init_single_node_cluster()
{
	as_partition_vinfo new_vinfo;

	generate_new_partition_version(&new_vinfo);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		uint32_t n_promoted = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			// For defrag, which is allowed to operate while we're doing this.
			pthread_mutex_lock(&p->lock);

			if (as_partition_is_null(&p->version_info)) {
				p->state = AS_PARTITION_STATE_SYNC;

				p->version_info = new_vinfo;
				p->primary_version_info = new_vinfo;
				ns->cluster_vinfo[0][pid] = new_vinfo;
				set_partition_version_in_storage(ns, pid, &new_vinfo, false);

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
	g_balance_init = BALANCE_INIT_RESOLVED;

	cf_atomic32_incr(&g_partition_generation);
}


// If this node encounters other nodes at startup, prevent it from switching to
// a single-node cluster - any initially ABSENT partitions participate in paxos
// and balancing as ABSENT.
void
as_partition_balance_init_multi_node_cluster()
{
	g_multi_node = true;
}


// Has the node resolved as operating either in a multi-node cluster or as a
// single-node cluster?
bool
as_partition_balance_is_init_resolved()
{
	return g_balance_init == BALANCE_INIT_RESOLVED;
}


// Has this node encountered other nodes?
bool
as_partition_balance_is_multi_node_cluster()
{
	return g_multi_node;
}


void
as_partition_balance()
{
	// Shortcut pointers.
	cf_node* succession = g_paxos->succession;

	// TODO: START move to paxos.
	if (! succession) {
		cf_crash(AS_PARTITION, "imbalance: succession list is uninitialized: couldn't start migrate");
	}

	uint32_t cluster_size = 0;

	while (cluster_size < AS_CLUSTER_SZ) {
		if (succession[cluster_size] == (cf_node)0) {
			break;
		}

		cluster_size++;
	}

	g_paxos->cluster_size = cluster_size;
	cf_info(AS_PARTITION, "CLUSTER SIZE = %zu", g_paxos->cluster_size);

	as_paxos_set_cluster_integrity(g_paxos, true);
	// TODO: END move to paxos.

	// Figure out effective replication factor in the face of node failures.
	apply_single_replica_limit(cluster_size);

	// Print rack aware info.
	partition_cluster_topology_info();

	cf_node* node_seq_table =
			cf_malloc(AS_PARTITIONS * cluster_size * sizeof(cf_node));

	if (! node_seq_table) {
		cf_crash(AS_PARTITION, "as_partition_balance: couldn't allocate node sequence table");
	}

	int* succession_index_table =
			cf_malloc(AS_PARTITIONS * cluster_size * sizeof(int));

	if (! succession_index_table) {
		cf_crash(AS_PARTITION, "as_partition_balance: couldn't allocate succession index table");
	}

	// Each partition separately shuffles the node succession list to generate
	// its own node sequence.
	fill_node_sequence_table(node_seq_table, succession_index_table);

	// Generate the new partition version based on the cluster key and use this
	// for any newly initialized partition.
	as_partition_vinfo new_vinfo;

	generate_new_partition_version(&new_vinfo);

	cf_queue mq;

	cf_queue_init(&mq, sizeof(partition_migrate_record),
			g_config.n_namespaces * AS_PARTITIONS, false);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// TODO - this is currently broken if namespaces have different
		// replication factors. Fix when we separate namespace rebalances.
		adjust_node_sequence_table(node_seq_table, succession_index_table,
				ns->replication_factor);

		int ns_pending_immigrations = 0;
		int ns_pending_emigrations = 0;
		int ns_delayed_emigrations = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			pthread_mutex_lock(&p->lock);

			uint32_t old_repl_factor = p->n_replicas;

			p->n_replicas = ns->replication_factor;
			memset(p->replicas, 0, sizeof(p->replicas));
			memcpy(p->replicas, &NODE_SEQ(pid, 0),
					p->n_replicas * sizeof(cf_node));

			p->cluster_key = as_paxos_get_cluster_key();

			// Interrupted migrations - start over.
			if (p->state == AS_PARTITION_STATE_DESYNC &&
					! as_partition_is_null(&p->version_info)) {
				p->state = AS_PARTITION_STATE_SYNC;
			}

			p->has_master_wait = false;
			p->pending_emigrations = 0;
			p->pending_immigrations = 0;
			memset(p->replicas_delayed_emigrate, 0,
					sizeof(p->replicas_delayed_emigrate));

			p->origin = (cf_node)0;
			p->target = (cf_node)0;

			p->n_dupl = 0;
			memset(p->dupl_nodes, 0, sizeof(p->dupl_nodes));

			p->current_outgoing_ldt_version = 0;

			uint32_t n_dupl = 0;
			cf_node dupl_nodes[cluster_size]; // nodes with duplicate versions

			memset(&dupl_nodes, 0, sizeof(dupl_nodes));

			bool has_version[cluster_size];
			int self_n = -1;
			int first_versioned_n = set_primary_version(p, node_seq_table,
					succession_index_table, ns, has_version, &self_n);

			if (first_versioned_n == -1) {
				first_versioned_n = 0;
				p->primary_version_info = new_vinfo;

				handle_lost_partition(p, node_seq_table, ns, has_version);

				// May advance the new partition version below - wasteful, but
				// leaving it for backward compatibility.)
			}
			else {
				n_dupl = find_duplicates(p, node_seq_table,
						succession_index_table, ns, dupl_nodes);

				if (should_advance_version(p, old_repl_factor)) {
					advance_version(p, node_seq_table, succession_index_table,
							ns, first_versioned_n);
				}
			}

			queue_namespace_migrations(p, node_seq_table, ns, self_n,
					first_versioned_n, has_version, n_dupl, dupl_nodes, &mq,
					&ns_delayed_emigrations);

			// Copy the new succession list over the old succession list.
			memset(p->old_sl, 0, sizeof(p->old_sl));
			memcpy(p->old_sl, &NODE_SEQ(pid, 0),
					sizeof(cf_node) * cluster_size);

			ns_pending_immigrations += p->pending_immigrations;
			ns_pending_emigrations += p->pending_emigrations;

			client_replica_maps_update(ns, pid);

			pthread_mutex_unlock(&p->lock);
		}

		int ns_all_pending_emigrations = ns_pending_emigrations +
				ns_delayed_emigrations;

		cf_info(AS_PARTITION, "{%s} re-balanced, expected migrations - (%d tx, %d rx)",
				ns->name, ns_all_pending_emigrations, ns_pending_immigrations);

		cf_atomic_int_set(&ns->migrate_tx_partitions_initial,
				ns_all_pending_emigrations);
		cf_atomic_int_set(&ns->migrate_tx_partitions_remaining,
				ns_all_pending_emigrations);

		cf_atomic_int_set(&ns->migrate_rx_partitions_initial,
				ns_pending_immigrations);
		cf_atomic_int_set(&ns->migrate_rx_partitions_remaining,
				ns_pending_immigrations);
	}

	// All partitions now have replicas assigned, ok to allow transactions.
	g_balance_init = BALANCE_INIT_RESOLVED;

	// Note - if we decide this is the best place to first increment this
	// counter, we could get rid of g_balance_init and just use this instead.
	cf_atomic32_incr(&g_partition_generation);

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_storage_info_flush(g_config.namespaces[i]);
	}

	as_partition_balance_allow_migrations();

	partition_migrate_record pmr;

	while (cf_queue_pop(&mq, &pmr, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		as_migrate_emigrate(&pmr);
	}

	cf_queue_destroy(&mq);

	cf_free(node_seq_table);
	cf_free(succession_index_table);
}


uint64_t
as_partition_balance_remaining_migrations()
{
	uint64_t remaining_migrations = 0;

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];

		remaining_migrations += ns->migrate_tx_partitions_remaining;
		remaining_migrations += ns->migrate_rx_partitions_remaining;
	}

	return remaining_migrations;
}


//==========================================================
// Public API - migration-related as_partition methods.
//

// Currently used only for enterprise build.
int
as_partition_pending_immigrations(as_partition* p)
{
	pthread_mutex_lock(&p->lock);

	int n = p->pending_immigrations;

	pthread_mutex_unlock(&p->lock);

	return n;
}


void
as_partition_emigrate_done(as_migrate_state s, as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t tx_flags)
{
	// TODO - better handled outside?
	if (AS_MIGRATE_STATE_DONE != s) {
		if (s == AS_MIGRATE_STATE_ERROR) {
			if (orig_cluster_key == as_paxos_get_cluster_key()) {
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

	bool acting_master = (tx_flags & TX_FLAGS_ACTING_MASTER) != 0;
	bool migration_request = (tx_flags & TX_FLAGS_REQUEST) != 0;
	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (orig_cluster_key != as_paxos_get_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return;
	}

	// Flush writes on the acting master now that it has completed filling the
	// real master with data.
	if (acting_master) {
		p->target = 0;
		p->n_dupl = 0;
		memset(p->dupl_nodes, 0, sizeof(p->dupl_nodes));
	}

	// Check if the migrate has been canceled by a partition rebalancing due to
	// a paxos vote. If this is the case, release the lock and return failure.
	// Otherwise, continue.
	if (p->pending_emigrations == 0) {
		cf_warning(AS_PARTITION, "{%s:%d} concurrency event - paxos reconfiguration occurred during migrate_tx?",
				ns->name, pid);
		pthread_mutex_unlock(&p->lock);
		return;
	}

	p->pending_emigrations--;

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

	if (AS_PARTITION_STATE_ZOMBIE == p->state && 0 == p->pending_emigrations) {
		set_partition_absent_lockfree(p, ns, true);
	}

	if (client_replica_maps_update(ns, pid)) {
		cf_atomic32_incr(&g_partition_generation);
	}

	pthread_mutex_unlock(&p->lock);

	return;
}


as_migrate_result
as_partition_immigrate_start(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, uint32_t start_type, cf_node source_node)
{
	if (! g_allow_migrations) {
		return AS_MIGRATE_AGAIN;
	}

	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (orig_cluster_key != as_paxos_get_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_AGAIN;
	}

	int64_t num_incoming = cf_atomic32_incr(&g_migrate_num_incoming);

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
		if (g_config.self_node != p->replicas[0]) {
			// TODO - deprecate in "six months".
			if (start_type == 0) {
				// Start message from old node. Note that this still has a bug
				// for replication factor > 2, where subsequent normal starts
				// masquerade as duplicate request-type starts.
				start_type = MIG_TYPE_START_IS_NORMAL;

				if (p->has_master_wait) {
					start_type = MIG_TYPE_START_IS_REQUEST;
				}
			}

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
						orig_cluster_key, TX_FLAGS_NONE);
				cf_queue_push(&mq, &r);

				rv = AS_MIGRATE_ALREADY_DONE;
				break;
			}
		}

		if (p->state == AS_PARTITION_STATE_ZOMBIE) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_start - in zombie state %d, fail",
					ns->name, pid, p->state);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		if (g_config.self_node != p->replicas[0]) {
			bool is_replica = false;

			for (uint32_t n = 1; n < p->n_replicas; n++) {
				if (g_config.self_node == p->replicas[n]) {
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
				if (source_node == p->dupl_nodes[dupl_ix]) {
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
as_partition_immigrate_done(as_namespace* ns, uint32_t pid,
		uint64_t orig_cluster_key, cf_node source_node)
{
	if (! g_allow_migrations) {
		return AS_MIGRATE_AGAIN;
	}

	as_partition* p = &ns->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (orig_cluster_key != as_paxos_get_cluster_key()) {
		pthread_mutex_unlock(&p->lock);
		return AS_MIGRATE_FAIL;
	}

	if (p->pending_immigrations == 0) {
		pthread_mutex_unlock(&p->lock);
		cf_warning(AS_PARTITION, "{%s:%d} immigrate_done concurrency event - paxos reconfiguration occurred during migrate_done?",
				ns->name, pid);
		return AS_MIGRATE_FAIL;
	}

	as_migrate_result rv = AS_MIGRATE_OK;
	as_partition_state orig_p_state = p->state;
	cf_queue mq;

	cf_queue_init(&mq, sizeof(partition_migrate_record),
			ns->cfg_replication_factor, false);

	switch (orig_p_state) {
	case AS_PARTITION_STATE_UNDEF:
	case AS_PARTITION_STATE_ABSENT:
	case AS_PARTITION_STATE_ZOMBIE:
		cf_warning(AS_PARTITION, "{%s:%u} immigrate_done received with bad state partition: %u ",
				ns->name, pid, p->state);
		rv = AS_MIGRATE_FAIL;
		break;
	case AS_PARTITION_STATE_DESYNC:
		if (p->origin != source_node || p->pending_immigrations == 0) {
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
					ns->name, pid, p->pending_immigrations, migrates_rx_remaining);
		}

		p->origin = 0;

		set_partition_sync_lockfree(p, ns, true);

		// If this is not the final master, we are done.
		if (g_config.self_node != p->replicas[0]) {
			if (p->pending_immigrations != 0) {
				cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - rx %d is non zero",
						ns->name, pid, p->pending_immigrations);
				rv = AS_MIGRATE_FAIL;
			}

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
			partition_migrate_record_fill(&r, p->dupl_nodes[dupl_ix],
					ns, pid, orig_cluster_key, TX_FLAGS_REQUEST);

			cf_queue_push(&mq, &r);
		}

		if (p->pending_emigrations != p->pending_immigrations) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - rx %d and tx %d values mismatch",
					ns->name, pid, p->pending_immigrations, p->pending_emigrations);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		// Continue to code block below - the state is sync now.

	// No break.
	case AS_PARTITION_STATE_SYNC:
		if (g_config.self_node != p->replicas[0]) {
			cf_warning(AS_PARTITION, "{%s:%d} immigrate_done aborted - state error for sync partition",
					ns->name, pid);
			rv = AS_MIGRATE_FAIL;
			break;
		}

		if (p->n_dupl > 0) {
			bool found = false;
			uint32_t i = 0;

			for (i = 0; i < p->n_dupl; i++) {
				if (p->dupl_nodes[i] == source_node) {
					found = true;
					break;
				}
			}

			if (found) {
				if (i == (p->n_dupl - 1)) { // delete last entry
					p->dupl_nodes[i] = (cf_node)0;
				}
				else { // copy last entry into deleted entry
					p->dupl_nodes[i] = p->dupl_nodes[p->n_dupl - 1];
					p->dupl_nodes[p->n_dupl - 1] = (cf_node)0;
				}

				p->n_dupl--;
				p->pending_immigrations--;

				int64_t migrates_rx_remaining = cf_atomic_int_decr(
						&ns->migrate_rx_partitions_remaining);

				if (migrates_rx_remaining < 0) {
					cf_warning(AS_PARTITION, "{%s:%d} (p%d, g%ld) immigrate_done - partitions schedule exceeded, possibly a race with prior migration",
							ns->name, pid, p->pending_immigrations, migrates_rx_remaining);
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
			break;
		}
		// else - received all expected, send anything pending as needed.

		for (int i = 0; i < AS_CLUSTER_SZ; i++) {
			if (p->replicas_delayed_emigrate[i]) {
				p->replicas_delayed_emigrate[i] = false;
				p->pending_emigrations++;

				partition_migrate_record r;
				partition_migrate_record_fill(&r, p->replicas[i],
						ns, pid, orig_cluster_key, TX_FLAGS_NONE);

				cf_queue_push(&mq, &r);
			}
		}
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
// Local helpers.
//

void
set_partition_version_in_storage(as_namespace* ns, uint32_t pid,
		const as_partition_vinfo* vinfo, bool flush)
{
	if (as_storage_info_set(ns, pid, (uint8_t*)vinfo, sizeof(as_partition_vinfo)) != 0) {
		cf_warning(AS_PARTITION, "{%s:%u} failed to set version %lu in storage",
				ns->name, pid, vinfo->iid);
		return;
	}

	if (flush) {
		as_storage_info_flush(ns);
	}
}


void
generate_new_partition_version(as_partition_vinfo* new_vinfo)
{
	*new_vinfo = NULL_VINFO;
	new_vinfo->iid = as_paxos_get_cluster_key();
	new_vinfo->vtp[0] = 1;
}


// Reduce the replication factor to 1 if the cluster size is less than or equal
// to the specified limit.
void
apply_single_replica_limit(int new_cluster_size)
{
	bool reduce_repl = false;

	cf_info(AS_PARTITION, "setting replication factors: cluster size %d, paxos single replica limit %d",
			new_cluster_size, g_config.paxos_single_replica_limit);

	if (new_cluster_size <= g_config.paxos_single_replica_limit) {
		reduce_repl = true;
	}

	// Normal case - set replication factor.
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace* ns = g_config.namespaces[i];
		uint16_t max_repl = ns->cfg_replication_factor > new_cluster_size ?
				new_cluster_size : ns->cfg_replication_factor;

		ns->replication_factor = reduce_repl ? 1 : max_repl;

		cf_info(AS_PARTITION, "{%s} replication factor is %d", ns->name,
				ns->replication_factor);
	}
}


void
partition_cluster_topology_info()
{
	cf_node* succession = g_paxos->succession;
	uint32_t cluster_size = (uint32_t)g_paxos->cluster_size;

	uint32_t distinct_groups = 0;
	cluster_config_t cc;

	cc_cluster_config_defaults(&cc);

	for (uint32_t cur_n = 0;
			succession[cur_n] != (cf_node)0 && cur_n < cluster_size;
			cur_n++) {
		cc_group_t cur_group = cc_compute_group_id(succession[cur_n]);

		cc_add_fullnode_group_entry(&cc, succession[cur_n]);

		uint32_t prev_n;

		for (prev_n = 0; prev_n < cur_n; prev_n++) {
			if (cc_compute_group_id(succession[prev_n]) == cur_group) {
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


// fill_node_sequence_table()
//
//  Succession list - all nodes in cluster
//  +---------------+
//  | A | B | C | D |
//  +---------------+
//
//  Succession list index (sl_ix) - index in original list
//  +---------------+
//  | 0 | 1 | 2 | 3 |
//  +---------------+
//
// Every partition shuffles the succession list independently, e.g. for pid 0:
// Hash the node names with the pid:
//  H(A,0) = Y, H(B,0) = X, H(C,0) = W, H(D,0) = Z
// Store sl_ix in last byte of hash results so it doesn't affect sort:
//  +-----------------------+
//  | Y_0 | X_1 | W_2 | Z_3 |
//  +-----------------------+
// This sorts to:
//  +-----------------------+
//  | W_2 | X_1 | Y_0 | Z_3 |
//  +-----------------------+
// Replace original node names, and keep sl_ix order, resulting in:
//  +---------------+    +---------------+
//  | C | B | A | D |    | 2 | 1 | 0 | 3 |
//  +---------------+    +---------------+
//
//  Node sequence table      Succession index table
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
// We keep the succession index table so we can refer back to paxos' version
// info table, in which nodes are in the original succession list order.
void
fill_node_sequence_table(cf_node* node_seq_table, int* succession_index_table)
{
	uint32_t cluster_size = (uint32_t)g_paxos->cluster_size;
	cf_node* succession = g_paxos->succession;
	uint64_t hashed_nodes[cluster_size];

	for (uint32_t n = 0; n < cluster_size; n++) {
		hashed_nodes[n] = cf_hash_fnv(&succession[n], sizeof(cf_node));
	}

	// Build the node sequence table.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		inter_hash h;

		h.hashed_pid = g_hashed_pids[pid];

		for (uint32_t n = 0; n < cluster_size; n++) {
			h.hashed_node = hashed_nodes[n];

			cf_node* node_p = &NODE_SEQ(pid, n);

			*node_p = cf_hash_oneatatime(&h, sizeof(h));

			// Overlay index onto last byte.
			*node_p &= AS_CLUSTER_SZ_MASKP;
			*node_p += n;
		}

		// Sort the hashed node values.
		qsort(&node_seq_table[pid*  cluster_size], cluster_size,
				sizeof(cf_node), cf_compare_uint64ptr);

		// Overwrite the sorted hash values with the original node IDs.
		for (uint32_t n = 0; n < cluster_size; n++) {
			uint32_t sl_ix = (uint32_t)(NODE_SEQ(pid, n) & AS_CLUSTER_SZ_MASKN);

			NODE_SEQ(pid, n) = succession[sl_ix];

			// Saved to refer back to the partition version table.
			SL_IX(pid, n) = sl_ix;
		}
	}
}


// adjust_node_sequence_table()
//
// When "rack aware", nodes are in groups (racks).
//
//  Nodes and groups in the cluster
//  +---------------+
//  | Grp 1 | Grp 2 |
//  +---------------+
//  | A | B | C | D |
//  +---------------+
//
// Proles for a partition can't be in the same group as the master, e.g. for
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
adjust_node_sequence_table(cf_node* node_seq_table, int* succession_index_table,
		uint32_t repl_factor)
{
	if (g_config.cluster_mode == CL_MODE_NO_TOPOLOGY ||
			g_paxos->cluster_size == 1) {
		return;
	}

	uint32_t cluster_size = (uint32_t)g_paxos->cluster_size;
	uint32_t n_groups = g_config.cluster.group_count;
	uint32_t n_needed = n_groups < repl_factor ? n_groups : repl_factor;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		uint32_t next_n = n_needed; // next candidate index to swap with

		for (uint32_t cur_n = 1; cur_n < n_needed; cur_n++) {
			cf_node cur_node = NODE_SEQ(pid, cur_n);
			cc_group_t cur_group_id = cc_compute_group_id(cur_node);

			if (cur_node == (cf_node)0) {
				cf_crash(AS_PARTITION, "null node found within cluster_size");
			}

			// If cur_group is unique for nodes < cur_i, continue to next node.
			if (! is_group_distinct_before_n(pid, node_seq_table,
					succession_index_table, cur_group_id, cur_n)) {
				// Find group after cur_i that's unique for groups before cur_i.
				uint32_t swap_n = cur_n; // if swap cannot be found then no change

				while (next_n < cluster_size) {
					cf_node next_node = NODE_SEQ(pid, next_n);
					cc_group_t next_group_id = cc_compute_group_id(next_node);

					if (next_node == (cf_node)0) {
						cf_crash(AS_PARTITION, "null node found within cluster_size");
					}

					if (is_group_distinct_before_n(pid, node_seq_table,
							succession_index_table, next_group_id, cur_n)) {
						swap_n = next_n;
						next_n++;
						break;
					}

					next_n++;
				}

				if (swap_n == cur_n) {
					// No other distinct groups found - shouldn't be possible.
					// We should reach n_needed first.
					cf_crash(AS_PARTITION, "can't find a diff cur:%u swap:%u repl:%u clsz:%u ptn:%u",
							cur_n, swap_n, repl_factor, cluster_size, pid);
				}

				// Now swap cur_n with swap_n.

				// Swap node.
				NODE_SEQ(pid, cur_n) ^= NODE_SEQ(pid, swap_n);
				NODE_SEQ(pid, swap_n) = NODE_SEQ(pid, cur_n) ^ NODE_SEQ(pid, swap_n);
				NODE_SEQ(pid, cur_n) ^= NODE_SEQ(pid, swap_n);

				// Swap succession list index.
				SL_IX(pid, cur_n) ^= SL_IX(pid, swap_n);
				SL_IX(pid, swap_n) = SL_IX(pid, cur_n) ^ SL_IX(pid, swap_n);
				SL_IX(pid, cur_n) ^= SL_IX(pid, swap_n);
			}
		}
	}
}


// Returns true if group_id is unique within nodes list indices less than n.
bool
is_group_distinct_before_n(uint32_t pid, const cf_node* node_seq_table,
		const int* succession_index_table, cc_node_t group_id, uint32_t n)
{
	for (uint32_t cur_n = 0; cur_n < n; cur_n++) {
		cf_node cur_node = NODE_SEQ(pid, cur_n);
		cc_node_t cur_group_id = cc_compute_group_id(cur_node);

		if (cur_group_id == group_id) {
			return false;
		}
	}

	return true;
}


int
set_primary_version(as_partition* p, const cf_node* node_seq_table,
		const int* succession_index_table, as_namespace* ns, bool has_version[],
		int* self_n)
{
	uint32_t cluster_size = (uint32_t)g_paxos->cluster_size;
	uint32_t pid = p->id;
	int first_versioned_n = -1;

	for (uint32_t n = 0; n < cluster_size; n++) {
		if (NODE_SEQ(pid, n) == g_config.self_node) {
			*self_n = n;
		}

		int sl_ix = SL_IX(pid, n);
		as_partition_vinfo* vinfo = &ns->cluster_vinfo[sl_ix][pid];

		if (as_partition_is_null(vinfo)) {
			has_version[n] = false;
		}
		else {
			has_version[n] = true;

			if (first_versioned_n == -1) {
				first_versioned_n = (int)n;
				p->primary_version_info = *vinfo;
			}
		}
	}

	return first_versioned_n;
}


void
handle_lost_partition(as_partition* p, const cf_node* node_seq_table,
		as_namespace* ns, bool has_version[])
{
	for (uint32_t n = 0; n < p->n_replicas; n++) {
		// Each replica initializes its partition version to the same new value.
		if (NODE_SEQ(p->id, n) == g_config.self_node) {
			drop_trees(p, ns);
			set_partition_sync_lockfree(p, ns, false);
		}

		has_version[n] = true;
	}
}


uint32_t
find_duplicates(const as_partition* p, const cf_node* node_seq_table,
		const int* succession_index_table, const as_namespace* ns,
		cf_node dupl_nodes[])
{
	uint32_t cluster_size = (uint32_t)g_paxos->cluster_size;
	uint32_t pid = p->id;

	uint32_t n_dupl = 0;
	as_partition_vinfo dupl_pvinfo[cluster_size];

	memset(&dupl_pvinfo, 0, sizeof(dupl_pvinfo));

	for (uint32_t n = 0; n < cluster_size; n++) {
		int sl_ix = SL_IX(pid, n);
		const as_partition_vinfo* vinfo = &ns->cluster_vinfo[sl_ix][pid];

		// If this partition version is unique, add to duplicates list.

		if (as_partition_is_null(vinfo) ||
				as_partition_vinfo_same(&p->primary_version_info, vinfo)) {
			continue;
		}

		uint32_t d;

		for (d = 0; d < n_dupl; d++) {
			if (as_partition_vinfo_same(&dupl_pvinfo[d], vinfo)) {
				break;
			}
		}

		if (d == n_dupl) {
			// Unique - didn't match primary version or any duplicate.
			dupl_nodes[n_dupl] = NODE_SEQ(pid, n);
			dupl_pvinfo[n_dupl] = *vinfo;
			n_dupl++;
		}
	}

	return n_dupl;
}


// For this partition, check if any replicas in the old succession list are
// missing from the new succession list.
bool
should_advance_version(const as_partition* p, uint32_t old_repl_factor)
{
	uint32_t cluster_size = (uint32_t)g_paxos->cluster_size;
	cf_node* succession = g_paxos->succession;

	for (uint32_t k = 0; k < old_repl_factor; k++) {
		if (p->old_sl[k] == 0) {
			return false;
		}

		uint32_t n;

		for (n = 0; n < cluster_size; n++) {
			if (p->old_sl[k] == succession[n]) {
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
advance_version(as_partition* p, const cf_node* node_seq_table,
		const int* succession_index_table, as_namespace* ns,
		int first_versioned_n)
{
	uint32_t pid = p->id;

	// Find the first versioned node in the old succession list.
	cf_node first_versioned_node = NODE_SEQ(pid, first_versioned_n);
	int n;

	for (n = 0; n < AS_CLUSTER_SZ; n++) {
		if (p->old_sl[n] == (cf_node)0) {
			return;
		}

		if (p->old_sl[n] == first_versioned_node) {
			// n is first versioned node's index in old succession list.
			break;
		}
	}

	// First versioned node not in old succession list - leave version as is.
	if (n == AS_CLUSTER_SZ) {
		return;
	}

	int sl_ix = SL_IX(pid, first_versioned_n);
	as_partition_vinfo adv_vinfo = ns->cluster_vinfo[sl_ix][pid];
	int i;

	for (i = 0; i < AS_PARTITION_MAX_VERSION; i++) {
		if (adv_vinfo.vtp[i] == 0) {
			adv_vinfo.vtp[i] = (uint8_t)n + 1;
			break;
		}
	}

	// If we run out of space generate a completely new version.
	if (i == AS_PARTITION_MAX_VERSION) {
		generate_new_partition_version(&adv_vinfo);
	}

	if (as_partition_vinfo_same(&p->version_info,
			&ns->cluster_vinfo[sl_ix][pid])) {
		set_partition_version_in_storage(ns, pid, &adv_vinfo, false);
		p->version_info = adv_vinfo;
	}

	p->primary_version_info = adv_vinfo;
}


void
queue_namespace_migrations(as_partition* p, const cf_node* node_seq_table,
		as_namespace* ns, int self_n, int first_versioned_n,
		const bool has_version[], uint32_t n_dupl, const cf_node dupl_nodes[],
		cf_queue* mq, int* ns_delayed_emigrations)
{
	uint32_t pid = p->id;
	uint64_t cluster_key = p->cluster_key;
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
			p->origin = NODE_SEQ(pid, first_versioned_n);
			set_partition_desync_lockfree(p, ns, false);
		}

		// Final master expects migrations from each (unique) duplicate.
		if (n_dupl != 0) {
			p->n_dupl = n_dupl;
			memcpy(p->dupl_nodes, dupl_nodes, sizeof(cf_node) * n_dupl);
			p->pending_immigrations += n_dupl;
		}

		// If no expected immigrations, schedule emigrations to versionless
		// replicas right away.
		if (p->pending_immigrations == 0) {
			for (uint32_t n = 1; n < p->n_replicas; n++) {
				if (! has_version[n]) {
					partition_migrate_record_fill(&pmr, NODE_SEQ(pid, n), ns,
							pid, cluster_key, TX_FLAGS_NONE);
					cf_queue_push(mq, &pmr);
					p->pending_emigrations++;
				}
			}

			return;
		}

		// Expecting immigrations - schedule delayed emigrations of merged
		// partition to all replicas (if there are duplicates) or versionless
		// replicas (if there are no duplicates).
		for (uint32_t n = 1; n < p->n_replicas; n++) {
			if (p->n_dupl != 0 || ! has_version[n]) {
				p->replicas_delayed_emigrate[n] = true;
				(*ns_delayed_emigrations)++;
			}
		}

		return;
	}
	// else - <><><><><><>  Not Final Master  <><><><><><>

	// If this node has no version, nothing to emigrate.
	if (! has_version[self_n]) {
		if (self_n < p->n_replicas) {
			// This node is a replica - expect immigration from final master.
			p->origin = NODE_SEQ(pid, 0);
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
			memcpy(p->dupl_nodes, dupl_nodes, sizeof(cf_node) * n_dupl);
		}

		p->target = NODE_SEQ(pid, 0); // only acting master sets p->target

		partition_migrate_record_fill(&pmr, NODE_SEQ(pid, 0), ns, pid,
				cluster_key, TX_FLAGS_ACTING_MASTER);
		cf_queue_push(mq, &pmr);
		p->pending_emigrations++;
	}
	else if (cf_contains64(dupl_nodes, n_dupl, g_config.self_node)) {
		// ... If this node is a duplicate with a versioned final master,
		// schedule emigration to final master immediately ...
		if (has_version[0]) {
			partition_migrate_record_fill(&pmr, NODE_SEQ(pid, 0), ns, pid,
					cluster_key, TX_FLAGS_NONE);
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
	if (self_n < p->n_replicas) {
		if (n_dupl != 0) {
			p->origin = NODE_SEQ(pid, 0);
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


void
partition_migrate_record_fill(partition_migrate_record* pmr, cf_node dest,
		as_namespace* ns, uint32_t pid, uint64_t cluster_key,
		uint32_t tx_flags)
{
	pmr->dest = dest;
	pmr->ns = ns;
	pmr->pid = pid;
	pmr->tx_flags = tx_flags;
	pmr->cluster_key = cluster_key;
}


void
set_partition_sync_lockfree(as_partition* p, as_namespace* ns, bool flush)
{
	p->state = AS_PARTITION_STATE_SYNC;
	p->version_info = p->primary_version_info;
	set_partition_version_in_storage(ns, p->id, &p->version_info, flush);
}


void
set_partition_desync_lockfree(as_partition* p, as_namespace* ns, bool flush)
{
	p->state = AS_PARTITION_STATE_DESYNC;
	p->version_info = NULL_VINFO;
	set_partition_version_in_storage(ns, p->id, &NULL_VINFO, flush);

	drop_trees(p, ns);
}


void
set_partition_absent_lockfree(as_partition* p, as_namespace* ns, bool flush)
{
	p->state = AS_PARTITION_STATE_ABSENT;
	p->version_info = NULL_VINFO;
	set_partition_version_in_storage(ns, p->id, &NULL_VINFO, flush);

	drop_trees(p, ns);

	p->current_outgoing_ldt_version = 0;
}


void
drop_trees(as_partition* p, as_namespace* ns)
{
	uint32_t pid = p->id;
	as_index_tree* temp = p->vp;

	p->vp = as_index_tree_create(ns->arena,
			(as_index_value_destructor)&as_record_destroy, ns,
			ns->tree_roots ? &ns->tree_roots[pid] : NULL);
	as_index_tree_release(temp);

	as_index_tree* sub_temp = p->sub_vp;

	p->sub_vp = as_index_tree_create(ns->arena,
			(as_index_value_destructor)&as_record_destroy, ns,
			ns->sub_tree_roots ? &ns->sub_tree_roots[pid] : NULL);
	as_index_tree_release(sub_temp);

	// TODO - consider p->n_tombstones?
	cf_atomic64_set(&p->max_void_time, 0);
}

