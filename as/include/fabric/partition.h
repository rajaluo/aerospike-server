/*
 * partition.h
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

// partition.h

#pragma once

#include <stdbool.h>

#include "citrusleaf/cf_atomic.h"

#include "dynbuf.h"
#include "util.h"

#include "fabric/hb.h"

struct as_index_tree_s;
struct as_namespace_s;

// AS_PARTITIONS
// The number of partitions in the system (and a mask for convenience).
#define AS_PARTITIONS 4096
#define AS_PARTITION_MASK (AS_PARTITIONS - 1)

// as_partition_state
// The state of a partition
//    SYNC: fully synchronized
//    DESYNC: unsynchronized, but moving towards synchronization
//    ZOMBIE: sync, but moving towards absent
//    ABSENT: empty
#define AS_PARTITION_STATE_UNDEF 0
#define AS_PARTITION_STATE_SYNC 1
#define AS_PARTITION_STATE_DESYNC 2
#define AS_PARTITION_STATE_ZOMBIE 3
#define AS_PARTITION_STATE_ABSENT 5
typedef uint8_t as_partition_state;

#define AS_PARTITION_MIG_TX_STATE_NONE 0
#define AS_PARTITION_MIG_TX_STATE_SUBRECORD 1
#define AS_PARTITION_MIG_TX_STATE_RECORD 2
typedef uint8_t as_partition_mig_tx_state;

// Counter that tells clients partition ownership has changed.
extern cf_atomic32 g_partition_generation;

// Counter for receiver-side migration flow control.
extern cf_atomic32 g_migrate_num_incoming;

#define AS_PARTITION_ID_UNDEF ((uint16_t)0xFFFF)

#define AS_PARTITION_RESERVATION_INIT(__rsv) \
	__rsv.ns = NULL; \
	__rsv.is_write = false; \
	__rsv.pid = AS_PARTITION_ID_UNDEF; \
	__rsv.p = NULL; \
	__rsv.state = AS_PARTITION_STATE_UNDEF; \
	__rsv.tree = NULL; \
	__rsv.n_dupl = 0; \
	__rsv.cluster_key = 0;

#define AS_PARTITION_RESERVATION_INITP(__rsv) \
	__rsv->ns = NULL; \
	__rsv->is_write = false; \
	__rsv->pid = AS_PARTITION_ID_UNDEF; \
	__rsv->p = NULL; \
	__rsv->state = AS_PARTITION_STATE_UNDEF; \
	__rsv->tree = NULL; \
	__rsv->n_dupl = 0; \
	__rsv->cluster_key = 0;

typedef struct repl_stats_s {
	uint64_t n_master_objects;
	uint64_t n_prole_objects;
	uint64_t n_master_sub_objects;
	uint64_t n_prole_sub_objects;
	uint64_t n_master_tombstones;
	uint64_t n_prole_tombstones;
} repl_stats;

#define AS_PARTITION_MAX_VERSION 16

// as_partition_vinfo
// A partition's version information
typedef struct as_partition_vinfo_s {
	uint64_t iid;                           // iid is the identifier of the cluster at the time the partition was created
	uint8_t vtp[AS_PARTITION_MAX_VERSION];  // vtp is the version string of the partition with the cluster's split-reforms
} as_partition_vinfo;

typedef struct as_partition_s {
	pthread_mutex_t lock;

	cf_node replica[AS_CLUSTER_SZ];
	// origin: the node that is replicating to us. For master, origin could be "acting master" during migration.
	// target: an actual master that we're migrating to.
	cf_node origin, target;
	as_partition_state state;  // used to be consistency
	int pending_migrate_tx, pending_migrate_rx;
	bool replica_tx_onsync[AS_CLUSTER_SZ];

	size_t n_dupl;
	cf_node dupl_nodes[AS_CLUSTER_SZ];
	bool has_master_wait; // TODO - deprecate in "six months"
	bool has_migrate_tx_later;
	as_partition_vinfo primary_version_info; // the version of the primary partition in the cluster
	as_partition_vinfo version_info; // the version of my partition here and now

	cf_node old_sl[AS_CLUSTER_SZ];

	uint64_t cluster_key;

	// the maximum void time of all records in the tree below
	cf_atomic64 max_void_time;

	// the actual data
	struct as_index_tree_s* vp;
	struct as_index_tree_s* sub_vp;
	uint32_t partition_id;
	uint32_t p_repl_factor;
	cf_atomic64 n_tombstones; // relevant only for enterprise edition

	// Track ldt version in transit currently
	uint64_t current_outgoing_ldt_version;
	uint64_t current_incoming_ldt_version;
} as_partition;

// as_partition_reservation
// A structure to hold state on a reserved partition
// NB: Structure elements are organized to make sure access to most
//     common field is a single cache line access ... DO NOT DISTURB
//     unless you what you are doing
typedef struct as_partition_reservation_s {
	struct as_namespace_s* ns;
	bool is_write;
	uint8_t unused;
	as_partition_state state;
	uint8_t n_dupl;
	uint16_t pid;
	uint8_t spare[2];
	//************* 16 byte ******
	as_partition* p;
	struct as_index_tree_s* tree;
	uint64_t cluster_key;
	as_partition_vinfo vinfo;
	//************* 64 byte ******
	struct as_index_tree_s* sub_tree;
	cf_node dupl_nodes[AS_CLUSTER_SZ];
} as_partition_reservation;

#define CLIENT_BITMAP_BYTES ((AS_PARTITIONS + 7) / 8)
#define CLIENT_B64MAP_BYTES (((CLIENT_BITMAP_BYTES + 2) / 3) * 4)

typedef struct client_replica_map_s {
	pthread_mutex_t write_lock;

	volatile uint8_t bitmap[CLIENT_BITMAP_BYTES];
	volatile char b64map[CLIENT_B64MAP_BYTES];
} client_replica_map;

static inline bool
as_partition_vinfo_same(as_partition_vinfo* v1, as_partition_vinfo* v2) {
	if (v1->iid != v2->iid) {
		return false;
	}

	if (0 != memcmp(v1->vtp, v2->vtp, AS_PARTITION_MAX_VERSION)) {
		return false;
	}

	return true;
}

static inline uint32_t
as_partition_getid(cf_digest d)
{
	return cf_digest_gethash(&d, AS_PARTITION_MASK);
}

void as_partition_init(as_partition* p, struct as_namespace_s* ns, uint32_t pid);
void as_partition_reinit(as_partition* p, struct as_namespace_s* ns, uint32_t pid);

int as_partition_get_state_from_storage(struct as_namespace_s* ns, bool* partition_states);

bool as_is_partition_null(as_partition_vinfo* vinfo);
cf_node as_partition_getreplica_read(struct as_namespace_s* ns, uint32_t pid);
int as_partition_getreplica_readall(struct as_namespace_s* ns, uint32_t pid, cf_node* nv);
cf_node as_partition_getreplica_write(struct as_namespace_s* ns, uint32_t pid);

void as_partition_getreplica_read_str(cf_dyn_buf* db);
void as_partition_getreplica_prole_str(cf_dyn_buf* db);
void as_partition_getreplica_write_str(cf_dyn_buf* db);
void as_partition_getreplica_master_str(cf_dyn_buf* db);
void as_partition_get_replicas_all_str(cf_dyn_buf* db);

void client_replica_maps_create(struct as_namespace_s* ns);
bool client_replica_maps_update(struct as_namespace_s* ns, uint32_t pid);
bool client_replica_maps_is_partition_queryable(struct as_namespace_s* ns, uint32_t pid);

void as_partition_get_master_prole_stats(struct as_namespace_s* ns, repl_stats* p_stats);

cf_node as_partition_proxyee_redirect(struct as_namespace_s* ns, uint32_t pid);

int as_partition_prereserve_query(struct as_namespace_s* ns, bool can_partition_query[], as_partition_reservation rsv[]);
int as_partition_reserve_query(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
int as_partition_reserve_write(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, uint64_t* cluster_key);
void as_partition_reserve_migrate(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node);
int as_partition_reserve_migrate_timeout(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, int timeout_ms );
int as_partition_reserve_xdr_read(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
int as_partition_reserve_read(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, uint64_t* cluster_key);
void as_partition_reservation_copy(as_partition_reservation* dst, as_partition_reservation* src);

void as_partition_release(as_partition_reservation* rsv);

void as_partition_getinfo_str(cf_dyn_buf* db);

int as_partition_remaining_immigrations(as_partition* p);
uint64_t as_partition_remaining_migrations();

void as_partition_allow_migrations();
void as_partition_disallow_migrations();
bool as_partition_get_migration_flag();

void as_partition_balance_init();
void as_partition_balance_init_single_node_cluster();
void as_partition_balance_init_multi_node_cluster();
bool as_partition_balance_is_init_resolved();
bool as_partition_balance_is_multi_node_cluster();
void as_partition_balance();
