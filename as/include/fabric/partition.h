/*
 * partition.h
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"

#include "dynbuf.h"
#include "util.h"

#include "fabric/hb.h"


//==========================================================
// Forward declarations.
//

struct as_index_tree_s;
struct as_namespace_s;


//==========================================================
// Typedefs and constants.
//

#define AS_PARTITIONS 4096
#define AS_PARTITION_MASK (AS_PARTITIONS - 1)

#define AS_PARTITION_STATE_UNDEF 0
#define AS_PARTITION_STATE_SYNC 1
#define AS_PARTITION_STATE_DESYNC 2
#define AS_PARTITION_STATE_ZOMBIE 3
#define AS_PARTITION_STATE_ABSENT 5
typedef uint8_t as_partition_state;

#define AS_PARTITION_MAX_VERSION 16

typedef struct as_partition_vinfo_s {
	uint64_t iid;
	uint8_t vtp[AS_PARTITION_MAX_VERSION];
} as_partition_vinfo;

extern const as_partition_vinfo NULL_VINFO;

typedef struct as_partition_s {
	pthread_mutex_t lock;

	uint32_t id;

	struct as_index_tree_s* vp;
	struct as_index_tree_s* sub_vp;

	cf_atomic64 n_tombstones; // relevant only for enterprise edition
	cf_atomic64 max_void_time; // TODO - convert to 32-bit ...

	// Replica information.
	uint32_t n_replicas;
	cf_node replicas[AS_CLUSTER_SZ];

	// Rebalance & migration related:

	uint64_t cluster_key;
	as_partition_vinfo primary_version_info;
	as_partition_vinfo version_info;
	as_partition_state state;

	bool has_master_wait; // TODO - deprecate in "six months"
	int pending_emigrations;
	int pending_immigrations;
	bool replicas_delayed_emigrate[AS_CLUSTER_SZ];

	cf_node origin;
	cf_node target;

	uint32_t n_dupl;
	cf_node dupl_nodes[AS_CLUSTER_SZ];

	cf_node old_sl[AS_CLUSTER_SZ];

	// LDT related.
	uint64_t current_outgoing_ldt_version;
	uint64_t current_incoming_ldt_version;
} as_partition;

typedef struct as_partition_reservation_s {
	struct as_namespace_s* ns;
	as_partition* p;
	struct as_index_tree_s* tree;
	struct as_index_tree_s* sub_tree;
	uint64_t cluster_key;
	as_partition_state state;
	// 3 unused bytes
	uint32_t n_dupl;
	cf_node dupl_nodes[AS_CLUSTER_SZ];
} as_partition_reservation;

typedef struct repl_stats_s {
	uint64_t n_master_objects;
	uint64_t n_prole_objects;
	uint64_t n_master_sub_objects;
	uint64_t n_prole_sub_objects;
	uint64_t n_master_tombstones;
	uint64_t n_prole_tombstones;
} repl_stats;

#define CLIENT_BITMAP_BYTES ((AS_PARTITIONS + 7) / 8)
#define CLIENT_B64MAP_BYTES (((CLIENT_BITMAP_BYTES + 2) / 3) * 4)

typedef struct client_replica_map_s {
	pthread_mutex_t write_lock;

	volatile uint8_t bitmap[CLIENT_BITMAP_BYTES];
	volatile char b64map[CLIENT_B64MAP_BYTES];
} client_replica_map;

typedef enum {
	AS_MIGRATE_OK,
	AS_MIGRATE_FAIL,
	AS_MIGRATE_AGAIN,
	AS_MIGRATE_ALREADY_DONE
} as_migrate_result;

typedef enum {
	AS_MIGRATE_STATE_DONE,
	AS_MIGRATE_STATE_START,
	AS_MIGRATE_STATE_ERROR,
	AS_MIGRATE_STATE_EAGAIN
} as_migrate_state;


//==========================================================
// Macros.
//

#define AS_PARTITION_ID_UNDEF ((uint16_t)0xFFFF)

#define AS_PARTITION_RESERVATION_INIT(__rsv) \
	__rsv.ns = NULL; \
	__rsv.p = NULL; \
	__rsv.tree = NULL; \
	__rsv.sub_tree = NULL; \
	__rsv.cluster_key = 0; \
	__rsv.state = AS_PARTITION_STATE_UNDEF; \
	__rsv.n_dupl = 0;

#define AS_PARTITION_RESERVATION_INITP(__rsv) \
	__rsv->ns = NULL; \
	__rsv->p = NULL; \
	__rsv->tree = NULL; \
	__rsv->sub_tree = NULL; \
	__rsv->cluster_key = 0; \
	__rsv->state = AS_PARTITION_STATE_UNDEF; \
	__rsv->n_dupl = 0;


//==========================================================
// Public API.
//

void as_partition_init(struct as_namespace_s* ns, uint32_t pid);

int as_partition_get_state_from_storage(struct as_namespace_s* ns, bool* partition_states);

uint32_t as_partition_get_other_replicas(as_partition* p, cf_node* nv);

cf_node as_partition_writable_node(struct as_namespace_s* ns, uint32_t pid);
cf_node as_partition_proxyee_redirect(struct as_namespace_s* ns, uint32_t pid);

void as_partition_get_replicas_prole_str(cf_dyn_buf* db); // deprecate in "six months"
void as_partition_get_replicas_master_str(cf_dyn_buf* db);
void as_partition_get_replicas_all_str(cf_dyn_buf* db);

void as_partition_get_master_prole_stats(struct as_namespace_s* ns, repl_stats* p_stats);

int as_partition_reserve_write(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, uint64_t* cluster_key);
int as_partition_reserve_read(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, uint64_t* cluster_key);
void as_partition_reserve_migrate(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node);
int as_partition_reserve_migrate_timeout(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv, cf_node* node, int timeout_ms );
int as_partition_prereserve_query(struct as_namespace_s* ns, bool can_partition_query[], as_partition_reservation rsv[]);
int as_partition_reserve_query(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
int as_partition_reserve_xdr_read(struct as_namespace_s* ns, uint32_t pid, as_partition_reservation* rsv);
void as_partition_reservation_copy(as_partition_reservation* dst, as_partition_reservation* src);

void as_partition_release(as_partition_reservation* rsv);

void as_partition_getinfo_str(cf_dyn_buf* db);

static inline bool
as_partition_is_null(const as_partition_vinfo* vinfo)
{
	return vinfo->iid == 0;
}

static inline bool
as_partition_vinfo_same(const as_partition_vinfo* v1, const as_partition_vinfo* v2)
{
	if (v1->iid != v2->iid) {
		return false;
	}

	return memcmp(v1->vtp, v2->vtp, AS_PARTITION_MAX_VERSION) == 0;
}

static inline uint32_t
as_partition_getid(const cf_digest d)
{
	return cf_digest_gethash(&d, AS_PARTITION_MASK);
}


//==========================================================
// Public API - client view replica maps.
//

void client_replica_maps_create(struct as_namespace_s* ns);
bool client_replica_maps_update(struct as_namespace_s* ns, uint32_t pid);
bool client_replica_maps_is_partition_queryable(const struct as_namespace_s* ns, uint32_t pid);
