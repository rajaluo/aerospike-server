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

// Define macros for accessing the "global" node-seq and vinfo-index arrays.
#define PX_NODE_SEQ(x, y) px_node_seq_table[(x * g_paxos->cluster_size) + y]
#define PX_VI_IX(x, y) px_vinfo_index_table[(x * g_paxos->cluster_size) + y]

typedef struct inter_hash_s {
	uint64_t hashed_node;
	uint64_t hashed_pid;
} inter_hash;


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
static volatile int g_multi_node = false;

static uint64_t g_hashed_pids[AS_PARTITIONS];


//==========================================================
// Forward declarations.
//

void set_partition_version_in_storage(as_namespace* ns, uint32_t pid, const as_partition_vinfo* vinfo, bool flush);
void generate_new_partition_version(as_partition_vinfo* new_vinfo);
void partition_cluster_topology_info();
void fill_global_tables(cf_node* px_node_seq_table, int* px_vinfo_index_table);
void balance_namespace(cf_node* px_node_seq_table, int* px_vinfo_index_table, as_namespace* ns, cf_queue* mq, const as_partition_vinfo* new_vinfo);
void apply_single_replica_limit(as_namespace* ns);
void rack_aware_adjust_rows(cf_node* ns_node_seq, int* ns_vinfo_index, const as_namespace* ns);
bool is_group_distinct_before_n(const cf_node* ns_node_seq, cc_node_t group_id, uint32_t n);
int set_primary_version(as_partition* p, const cf_node* ns_node_seq, const int* ns_vinfo_index, as_namespace* ns, bool has_version[], int* self_n);
void handle_lost_partition(as_partition* p, const cf_node* ns_node_seq, as_namespace* ns, bool has_version[]);
uint32_t find_duplicates(const as_partition* p, const cf_node* ns_node_seq, const int* ns_vinfo_index, const as_namespace* ns, cf_node dupl_nodes[]);
bool should_advance_version(const as_partition* p, uint32_t old_repl_factor, as_namespace* ns);
void advance_version(as_partition* p, const cf_node* ns_node_seq, const int* ns_vinfo_index, as_namespace* ns, int first_versioned_n);
void queue_namespace_migrations(as_partition* p,
		const cf_node* ns_node_seq, as_namespace* ns, int self_n,
		int first_versioned_n, const bool has_version[], uint32_t n_dupl,
		const cf_node dupl_nodes[], cf_queue* mq, int* ns_delayed_emigrations);
void partition_migrate_record_fill(partition_migrate_record* pmr, cf_node dest, as_namespace* ns, uint32_t pid, uint64_t cluster_key, uint32_t tx_flags);

void set_partition_sync_lockfree(as_partition* p, as_namespace* ns, bool flush);
void set_partition_desync_lockfree(as_partition* p, as_namespace* ns, bool flush);
void set_partition_absent_lockfree(as_partition* p, as_namespace* ns, bool flush);
void drop_trees(as_partition* p, as_namespace* ns);

static inline bool
is_rack_aware()
{
	return g_config.cluster_mode != CL_MODE_NO_TOPOLOGY &&
			g_paxos->cluster_size > 1;
}


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

// Initially, every partition is either ABSENT, or a version was read from
// storage and it is SYNC.
void
as_partition_balance_init()
{
	// Cache hashed pids for all future rebalances.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		g_hashed_pids[pid] = cf_hash_fnv(&pid, sizeof(uint32_t));
	}

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->replication_factor = 1;

		uint32_t n_stored = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			p->cluster_key = as_paxos_get_cluster_key();
			p->old_node_seq[0] = g_config.self_node;

			if (ns->storage_type != AS_STORAGE_ENGINE_SSD) {
				continue;
			}

			as_partition_vinfo vinfo;

			as_storage_info_get(ns, pid, &vinfo);

			if (as_partition_is_null(&vinfo)) {
				// Stores the vinfo length, even when the vinfo is zeroed.
				set_partition_version_in_storage(ns, pid, &NULL_VINFO, false);
			}
			else {
				p->n_replicas = 1;
				p->replicas[0] = g_config.self_node;

				p->primary_version_info = vinfo;
				p->version_info = vinfo;
				p->state = AS_PARTITION_STATE_SYNC;

				ns->cluster_vinfo[0][pid] = vinfo;

				n_stored++;
			}
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

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		uint32_t n_promoted = 0;

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition* p = &ns->partitions[pid];

			// For defrag, which is allowed to operate while we're doing this.
			pthread_mutex_lock(&p->lock);

			if (as_partition_is_null(&p->version_info)) {
				p->n_replicas = 1;
				p->replicas[0] = g_config.self_node;

				p->primary_version_info = new_vinfo;
				p->version_info = new_vinfo;
				p->state = AS_PARTITION_STATE_SYNC;

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
	g_init_balance_done = true;
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
	return g_init_balance_done;
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
	//--------------------------------------------
	// TODO: START move to paxos.
	uint32_t cluster_size = 0;

	while (cluster_size < AS_CLUSTER_SZ) {
		if (g_paxos->succession[cluster_size] == (cf_node)0) {
			break;
		}

		cluster_size++;
	}

	g_paxos->cluster_size = cluster_size;
	cf_info(AS_PARTITION, "CLUSTER SIZE = %u", g_paxos->cluster_size);

	// HACK - for now make all namespaces' succession lists the same as the
	// global cluster list. Eventually the paxos replacement will fill in the
	// namespace lists independently. TODO - do this.
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->cluster_size = cluster_size;
		memset(ns->succession, 0, sizeof(ns->succession));
		memcpy(ns->succession, g_paxos->succession,
				sizeof(cf_node) * cluster_size);
	}

	as_paxos_set_cluster_integrity(g_paxos, true);
	// TODO: END move to paxos.
	//--------------------------------------------

	// Print rack aware info.
	partition_cluster_topology_info();

	cf_node* px_node_seq_table =
			cf_malloc(AS_PARTITIONS * cluster_size * sizeof(cf_node));

	cf_assert(px_node_seq_table, AS_PARTITION, "as_partition_balance: couldn't allocate node sequence table");

	int* px_vinfo_index_table =
			cf_malloc(AS_PARTITIONS * cluster_size * sizeof(int));

	cf_assert(px_vinfo_index_table, AS_PARTITION, "as_partition_balance: couldn't allocate succession index table");

	// Each partition separately shuffles the node succession list to generate
	// its own node sequence.
	fill_global_tables(px_node_seq_table, px_vinfo_index_table);

	// Generate the new partition version based on the cluster key and use this
	// for any newly initialized partition.
	as_partition_vinfo new_vinfo;

	generate_new_partition_version(&new_vinfo);

	cf_queue mq;

	cf_queue_init(&mq, sizeof(partition_migrate_record),
			g_config.n_namespaces * AS_PARTITIONS, false);

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		balance_namespace(px_node_seq_table, px_vinfo_index_table,
				g_config.namespaces[ns_ix], &mq, &new_vinfo);
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

	cf_free(px_node_seq_table);
	cf_free(px_vinfo_index_table);
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
	// TODO - better handled outside?
	if (s != AS_MIGRATE_STATE_DONE) {
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
					orig_cluster_key, TX_FLAGS_NONE);
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

		if (p->n_dupl != 0) {
			bool found = false;
			uint32_t dupl_ix;

			for (dupl_ix = 0; dupl_ix < p->n_dupl; dupl_ix++) {
				if (p->dupl_nodes[dupl_ix] == source_node) {
					found = true;
					break;
				}
			}

			if (found) {
				uint32_t last_dupl_ix = p->n_dupl - 1;

				if (dupl_ix == last_dupl_ix) { // delete last entry
					p->dupl_nodes[dupl_ix] = (cf_node)0;
				}
				else { // copy last entry into deleted entry
					p->dupl_nodes[dupl_ix] = p->dupl_nodes[last_dupl_ix];
					p->dupl_nodes[last_dupl_ix] = (cf_node)0;
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
			break;
		}
		// else - received all expected, send anything pending as needed.

		for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
			if (p->replicas_delayed_emigrate[repl_ix]) {
				p->replicas_delayed_emigrate[repl_ix] = false;
				p->pending_emigrations++;

				partition_migrate_record r;
				partition_migrate_record_fill(&r, p->replicas[repl_ix],
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
	as_storage_info_set(ns, pid, vinfo);

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


void
partition_cluster_topology_info()
{
	cf_node* succession = g_paxos->succession;

	uint32_t distinct_groups = 0;
	cluster_config_t cc;

	cc_cluster_config_defaults(&cc);

	for (uint32_t cur_n = 0;
			succession[cur_n] != (cf_node)0 && cur_n < g_paxos->cluster_size;
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


// fill_global_tables()
//
//  Succession list - all nodes in cluster
//  +---------------+
//  | A | B | C | D |
//  +---------------+
//
//  Succession list index - used as version info table index (vi_ix)
//  +---------------+
//  | 0 | 1 | 2 | 3 |
//  +---------------+
//
// Every partition shuffles the succession list independently, e.g. for pid 0:
// Hash the node names with the pid:
//  H(A,0) = Y, H(B,0) = X, H(C,0) = W, H(D,0) = Z
// Store vi_ix in last byte of hash results so it doesn't affect sort:
//  +-----------------------+
//  | Y_0 | X_1 | W_2 | Z_3 |
//  +-----------------------+
// This sorts to:
//  +-----------------------+
//  | W_2 | X_1 | Y_0 | Z_3 |
//  +-----------------------+
// Replace original node names, and keep vi_ix order, resulting in:
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
// We keep the version info index table so we can refer back to namespaces'
// version info tables, where nodes are in the original succession list order.
void
fill_global_tables(cf_node* px_node_seq_table, int* px_vinfo_index_table)
{
	uint32_t cluster_size = g_paxos->cluster_size;
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

			cf_node* node_p = &PX_NODE_SEQ(pid, n);

			*node_p = cf_hash_oneatatime(&h, sizeof(h));

			// Overlay index onto last byte.
			*node_p &= AS_CLUSTER_SZ_MASKP;
			*node_p += n;
		}

		// Sort the hashed node values.
		qsort(&px_node_seq_table[pid * cluster_size], cluster_size,
				sizeof(cf_node), cf_compare_uint64ptr);

		// Overwrite the sorted hash values with the original node IDs.
		for (uint32_t n = 0; n < cluster_size; n++) {
			cf_node* node_p = &PX_NODE_SEQ(pid, n);
			uint32_t vi_ix = (uint32_t)(*node_p & AS_CLUSTER_SZ_MASKN);

			*node_p = succession[vi_ix];

			// Saved to refer back to the partition version table.
			PX_VI_IX(pid, n) = vi_ix;
		}
	}
}


void
balance_namespace(cf_node* px_node_seq_table, int* px_vinfo_index_table,
		as_namespace* ns, cf_queue* mq, const as_partition_vinfo* new_vinfo)
{
	// Figure out effective replication factor in the face of node failures.
	apply_single_replica_limit(ns);

	int ns_pending_immigrations = 0;
	int ns_pending_emigrations = 0;
	int ns_delayed_emigrations = 0;

	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition* p = &ns->partitions[pid];

		cf_node* px_node_seq = &PX_NODE_SEQ(pid, 0);
		int* px_vinfo_index = &PX_VI_IX(pid, 0);

		// Usually a namespace can simply use the global tables...
		cf_node* ns_node_seq = px_node_seq;
		int* ns_vinfo_index = px_vinfo_index;

		if (is_rack_aware()) {
			rack_aware_adjust_rows(ns_node_seq, ns_vinfo_index, ns);
		}

		pthread_mutex_lock(&p->lock);

		uint32_t old_repl_factor = p->n_replicas;

		p->n_replicas = ns->replication_factor;
		memset(p->replicas, 0, sizeof(p->replicas));
		memcpy(p->replicas, ns_node_seq, p->n_replicas * sizeof(cf_node));

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
		cf_node dupl_nodes[ns->cluster_size]; // nodes with duplicate versions

		memset(&dupl_nodes, 0, sizeof(dupl_nodes));

		bool has_version[ns->cluster_size];
		int self_n = -1;
		int first_versioned_n = set_primary_version(p, ns_node_seq,
				ns_vinfo_index, ns, has_version, &self_n);

		if (first_versioned_n == -1) {
			first_versioned_n = 0;
			p->primary_version_info = *new_vinfo;

			handle_lost_partition(p, ns_node_seq, ns, has_version);
		}
		else {
			n_dupl = find_duplicates(p, ns_node_seq, ns_vinfo_index, ns,
					dupl_nodes);

			if (should_advance_version(p, old_repl_factor, ns)) {
				advance_version(p, ns_node_seq, ns_vinfo_index, ns,
						first_versioned_n);
			}
		}

		queue_namespace_migrations(p, ns_node_seq, ns, self_n,
				first_versioned_n, has_version, n_dupl, dupl_nodes, mq,
				&ns_delayed_emigrations);

		// Copy the new node sequence over the old node sequence.
		memset(p->old_node_seq, 0, sizeof(p->old_node_seq));
		memcpy(p->old_node_seq, ns_node_seq,
				sizeof(cf_node) * ns->cluster_size);

		ns_pending_immigrations += p->pending_immigrations;
		ns_pending_emigrations += p->pending_emigrations;

		client_replica_maps_update(ns, pid);

		pthread_mutex_unlock(&p->lock);
	}

	int ns_all_pending_emigrations = ns_pending_emigrations +
			ns_delayed_emigrations;

	cf_info(AS_PARTITION, "{%s} re-balanced, expected migrations - (%d tx, %d rx)",
			ns->name, ns_all_pending_emigrations, ns_pending_immigrations);

	ns->migrate_tx_partitions_initial = ns_all_pending_emigrations;
	ns->migrate_tx_partitions_remaining = ns_all_pending_emigrations;

	ns->migrate_rx_partitions_initial = ns_pending_immigrations;
	ns->migrate_rx_partitions_remaining = ns_pending_immigrations;
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


// rack_aware_adjust_rows()
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
rack_aware_adjust_rows(cf_node* ns_node_seq, int* ns_vinfo_index,
		const as_namespace* ns)
{
	uint32_t cluster_size = ns->cluster_size;
	uint32_t repl_factor = ns->replication_factor;

	uint32_t n_groups = g_config.cluster.group_count;
	uint32_t n_needed = n_groups < repl_factor ? n_groups : repl_factor;

	uint32_t next_n = n_needed; // next candidate index to swap with

	for (uint32_t cur_n = 1; cur_n < n_needed; cur_n++) {
		cf_node cur_node = ns_node_seq[cur_n];
		cc_group_t cur_group_id = cc_compute_group_id(cur_node);

		if (cur_node == (cf_node)0) {
			cf_crash(AS_PARTITION, "null node found within cluster_size");
		}

		// If cur_group is unique for nodes < cur_i, continue to next node.
		if (is_group_distinct_before_n(ns_node_seq, cur_group_id, cur_n)) {
			continue;
		}

		// Find group after cur_i that's unique for groups before cur_i.
		uint32_t swap_n = cur_n; // if swap cannot be found then no change

		while (next_n < cluster_size) {
			cf_node next_node = ns_node_seq[next_n];
			cc_group_t next_group_id = cc_compute_group_id(next_node);

			if (next_node == (cf_node)0) {
				cf_crash(AS_PARTITION, "null node found within cluster_size");
			}

			if (is_group_distinct_before_n(ns_node_seq, next_group_id, cur_n)) {
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
					cur_n, swap_n, repl_factor, cluster_size);
		}

		// Now swap cur_n with swap_n.

		// Swap node.
		cf_node temp_node = ns_node_seq[swap_n];

		ns_node_seq[swap_n] = ns_node_seq[cur_n];
		ns_node_seq[cur_n] = temp_node;

		// Swap succession list index.
		int temp_ix = ns_vinfo_index[swap_n];

		ns_vinfo_index[swap_n] = ns_vinfo_index[cur_n];
		ns_vinfo_index[cur_n] = temp_ix;
	}
}


// Returns true if group_id is unique within nodes list indices less than n.
bool
is_group_distinct_before_n(const cf_node* ns_node_seq, cc_node_t group_id,
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
		const int* ns_vinfo_index, as_namespace* ns, bool has_version[],
		int* self_n)
{
	int first_versioned_n = -1;

	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		if (ns_node_seq[n] == g_config.self_node) {
			*self_n = n;
		}

		int vi_ix = ns_vinfo_index[n];
		as_partition_vinfo* vinfo = &ns->cluster_vinfo[vi_ix][p->id];

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
find_duplicates(const as_partition* p, const cf_node* ns_node_seq,
		const int* ns_vinfo_index, const as_namespace* ns,
		cf_node dupl_nodes[])
{
	uint32_t n_dupl = 0;
	as_partition_vinfo dupl_pvinfo[ns->cluster_size];

	memset(dupl_pvinfo, 0, sizeof(dupl_pvinfo));

	for (uint32_t n = 0; n < ns->cluster_size; n++) {
		int vi_ix = ns_vinfo_index[n];
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
		if (p->old_node_seq[repl_ix] == 0) {
			return false;
		}

		uint32_t n;

		for (n = 0; n < cluster_size; n++) {
			if (p->old_node_seq[repl_ix] == succession[n]) {
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
advance_version(as_partition* p, const cf_node* ns_node_seq,
		const int* ns_vinfo_index, as_namespace* ns, int first_versioned_n)
{
	// Find the first versioned node in the old node sequence.
	cf_node first_versioned_node = ns_node_seq[first_versioned_n];
	int n;

	for (n = 0; n < AS_CLUSTER_SZ; n++) {
		if (p->old_node_seq[n] == (cf_node)0) {
			return;
		}

		if (p->old_node_seq[n] == first_versioned_node) {
			// n is first versioned node's index in old node sequence.
			break;
		}
	}

	// First versioned node not in old node sequence - leave version as is.
	if (n == AS_CLUSTER_SZ) {
		return;
	}

	int vi_ix = ns_vinfo_index[first_versioned_n];
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
		generate_new_partition_version(&adv_vinfo);
	}

	if (as_partition_vinfo_same(&p->version_info,
			&ns->cluster_vinfo[vi_ix][p->id])) {
		set_partition_version_in_storage(ns, p->id, &adv_vinfo, false);
		p->version_info = adv_vinfo;
	}

	p->primary_version_info = adv_vinfo;
}


void
queue_namespace_migrations(as_partition* p, const cf_node* ns_node_seq,
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
			memcpy(p->dupl_nodes, dupl_nodes, sizeof(cf_node) * n_dupl);
			p->pending_immigrations += n_dupl;
		}

		// If no expected immigrations, schedule emigrations to versionless
		// replicas right away.
		if (p->pending_immigrations == 0) {
			for (uint32_t repl_ix = 1; repl_ix < p->n_replicas; repl_ix++) {
				if (! has_version[repl_ix]) {
					partition_migrate_record_fill(&pmr, ns_node_seq[repl_ix],
							ns, p->id, p->cluster_key, TX_FLAGS_NONE);
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
				p->replicas_delayed_emigrate[repl_ix] = true;
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
			memcpy(p->dupl_nodes, dupl_nodes, sizeof(cf_node) * n_dupl);
		}

		p->target = ns_node_seq[0]; // only acting master sets p->target

		partition_migrate_record_fill(&pmr, ns_node_seq[0], ns, p->id,
				p->cluster_key, TX_FLAGS_ACTING_MASTER);
		cf_queue_push(mq, &pmr);
		p->pending_emigrations++;
	}
	else if (cf_contains64(dupl_nodes, n_dupl, g_config.self_node)) {
		// ... If this node is a duplicate with a versioned final master,
		// schedule emigration to final master immediately ...
		if (has_version[0]) {
			partition_migrate_record_fill(&pmr, ns_node_seq[0], ns, p->id,
					p->cluster_key, TX_FLAGS_NONE);
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

