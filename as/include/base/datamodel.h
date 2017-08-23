/*
 * datamodel.h
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

/*
 * core data model structures and definitions
 */

#pragma once

#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_val.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_shash.h"

#include "arenax.h"
#include "dynbuf.h"
#include "hist.h"
#include "hist_track.h"
#include "linear_hist.h"
#include "msg.h"
#include "node.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/proto.h"
#include "base/rec_props.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "storage/storage.h"


#define AS_STORAGE_MAX_DEVICES (32) // maximum devices per namespace
#define AS_STORAGE_MAX_FILES (32) // maximum files per namespace
#define AS_STORAGE_MAX_DEVICE_SIZE (2L * 1024L * 1024L * 1024L * 1024L) // 2Tb, due to rblock_id in as_index

#define OBJ_SIZE_HIST_NUM_BUCKETS 100
#define TTL_HIST_NUM_BUCKETS 100

#define MAX_ALLOWED_TTL (3600 * 24 * 365 * 10) // 10 years

/*
 * Subrecord Digest Scramble Position
 */
// [0-1] For Partitionid
// [1-2] For tree sprigs and locks
// [2-3] For the Lock
// [4-6] Scrambled bytes (4-7 used for rw_request hash)
#define DIGEST_SCRAMBLE_BYTE1       4
#define DIGEST_SCRAMBLE_BYTE2       5
#define DIGEST_SCRAMBLE_BYTE3       6
// [8-11]   SSD device hash
// Note - overlaps 3 old LDT clock bytes (9-11), meaning old subrecords of one
// LDT can now be spread to different devices.
#define DIGEST_STORAGE_BASE_BYTE	8

// [7] [12-13]  // 3 byte clock
#define DIGEST_CLOCK_ZERO_BYTE      7
#define DIGEST_CLOCK_START_BYTE     12 // up to 13

// [14-19]  // 6 byte version
#define DIGEST_VERSION_START_POS   14 // up to 19
// Define the size of the Version Info that we'll write into the LDT control Map
#define LDT_VERSION_SIZE  6

/* SYNOPSIS
 * Data model
 *
 * Objects are stored in a hierarchy: namespace:record:bin:particle.
 * The records in a namespace are further partitioned for distribution
 * amongst the participating nodes in the cluster.
 */



/* Forward declarations */
typedef struct as_namespace_s as_namespace;
typedef struct as_index_s as_record;
typedef struct as_bin_s as_bin;
typedef struct as_index_ref_s as_index_ref;
typedef struct as_set_s as_set;
typedef struct as_treex_s as_treex;

struct as_index_tree_s;


/* AS_ID_[NAMESPACE,SET,BIN,INAME]_SZ
 * The maximum length, in bytes, of an identification field; by convention,
 * these values are null-terminated UTF-8 */
#define AS_ID_NAMESPACE_SZ 32
#define AS_ID_BIN_SZ 15 // size used in storage format
#define AS_ID_INAME_SZ 256
#define VMAP_BIN_NAME_MAX_SZ ((AS_ID_BIN_SZ + 3) & ~3) // round up to multiple of 4
#define MAX_BIN_NAMES 0x10000 // no need for more - numeric ID is 16 bits
#define BIN_NAMES_QUOTA (MAX_BIN_NAMES / 2) // don't add more names than this via client transactions


// now dynamic
// #define AS_OBJECT_INDEX_OVERHEAD_BYTES 80
// #define AS_OBJECT_INDEX_OVERHEAD_BYTES 230

/* as_generation
 * A generation ID */
typedef uint32_t as_generation;

/*
 * Compare two 16-bit generation counts, allowing wrap-arounds.
 * Works correctly, if:
 *
 *   - rhs is ahead of lhs, but rhs isn't ahead more than 32,768.
 *   - lhs is ahead of rhs, but lhs isn't ahead more than 32,767.
 */

static inline bool
as_gen_less_than(uint16_t lhs, uint16_t rhs)
{
	return (uint16_t)(lhs - rhs) >= 32768;
}


/* as_particle_type
 * Particles are typed, which reflects their contents:
 *    NULL: no associated content (not sure I really need this internally?)
 *    INTEGER: a signed, 64-bit integer
 *    FLOAT: a floating point
 *    STRING: a null-terminated UTF-8 string
 *    BLOB: arbitrary-length binary data
 *    TIMESTAMP: milliseconds since 1 January 1970, 00:00:00 GMT
 *    DIGEST: an internal Aerospike key digest */
typedef enum {
	AS_PARTICLE_TYPE_NULL = 0,
	AS_PARTICLE_TYPE_INTEGER = 1,
	AS_PARTICLE_TYPE_FLOAT = 2,
	AS_PARTICLE_TYPE_STRING = 3,
	AS_PARTICLE_TYPE_BLOB = 4,
	AS_PARTICLE_TYPE_TIMESTAMP = 5,
	AS_PARTICLE_TYPE_UNUSED_6 = 6,
	AS_PARTICLE_TYPE_JAVA_BLOB = 7,
	AS_PARTICLE_TYPE_CSHARP_BLOB = 8,
	AS_PARTICLE_TYPE_PYTHON_BLOB = 9,
	AS_PARTICLE_TYPE_RUBY_BLOB = 10,
	AS_PARTICLE_TYPE_PHP_BLOB = 11,
	AS_PARTICLE_TYPE_ERLANG_BLOB = 12,
	AS_PARTICLE_TYPE_MAP = 19,
	AS_PARTICLE_TYPE_LIST = 20,
	AS_PARTICLE_TYPE_HIDDEN_LIST = 21,
	AS_PARTICLE_TYPE_HIDDEN_MAP = 22, // hidden map/list - can only be manipulated by system UDF
	AS_PARTICLE_TYPE_GEOJSON = 23,
	AS_PARTICLE_TYPE_MAX = 24,
	AS_PARTICLE_TYPE_BAD = AS_PARTICLE_TYPE_MAX
} as_particle_type;

/* as_particle
 * The common part of a particle
 * this is poor man's subclassing - IE, how to do a subclassed interface in C
 * Go look in particle.c to see all the subclass implementation and structure */
typedef struct as_particle_s {
	uint8_t		metadata;		// used by the iparticle for is_integer and inuse, as well as version in multi bin mode only
								// used by *particle for type
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle;

// Bit Flag constants used for the particle state value (4 bits, 16 values)
#define AS_BIN_STATE_UNUSED			0
#define AS_BIN_STATE_INUSE_INTEGER	1
#define AS_BIN_STATE_INUSE_HIDDEN	2 // Denotes a server-side, hidden bin
#define AS_BIN_STATE_INUSE_OTHER	3
#define AS_BIN_STATE_INUSE_FLOAT	4

typedef struct as_particle_iparticle_s {
	uint8_t		version: 4;		// now unused - and can't be used in single-bin config
	uint8_t		state: 4;		// see AS_BIN_STATE_...
	uint8_t		data[];
} __attribute__ ((__packed__)) as_particle_iparticle;

/* Particle function declarations */

static inline bool
is_embedded_particle_type(as_particle_type type)
{
	return type == AS_PARTICLE_TYPE_INTEGER || type == AS_PARTICLE_TYPE_FLOAT;
}

extern as_particle_type as_particle_type_from_asval(const as_val *val);
extern as_particle_type as_particle_type_from_msgpack(const uint8_t *packed, uint32_t packed_size);

extern int32_t as_particle_size_from_client(const as_msg_op *op); // TODO - will we ever need this?
extern int32_t as_particle_size_from_pickled(uint8_t **p_pickled);
extern uint32_t as_particle_size_from_asval(const as_val *val);
extern int32_t as_particle_size_from_flat(const uint8_t *flat, uint32_t flat_size); // TODO - will we ever need this?

extern uint32_t as_particle_asval_client_value_size(const as_val *val);
extern uint32_t as_particle_asval_to_client(const as_val *val, as_msg_op *op);

// as_bin particle function declarations

extern void as_bin_particle_destroy(as_bin *b, bool free_particle);
extern uint32_t as_bin_particle_size(as_bin *b);

// wire:
extern int32_t as_bin_particle_size_modify_from_client(as_bin *b, const as_msg_op *op); // TODO - will we ever need this?
extern int as_bin_particle_alloc_modify_from_client(as_bin *b, const as_msg_op *op);
extern int as_bin_particle_stack_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op);
extern int as_bin_particle_alloc_from_client(as_bin *b, const as_msg_op *op);
extern int as_bin_particle_stack_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op);
extern int as_bin_particle_replace_from_pickled(as_bin *b, uint8_t **p_pickled);
extern int32_t as_bin_particle_stack_from_pickled(as_bin *b, uint8_t* stack, uint8_t **p_pickled);
extern int as_bin_particle_compare_from_pickled(const as_bin *b, uint8_t **p_pickled);
extern uint32_t as_bin_particle_client_value_size(const as_bin *b);
extern uint32_t as_bin_particle_to_client(const as_bin *b, as_msg_op *op);
extern uint32_t as_bin_particle_pickled_size(const as_bin *b);
extern uint32_t as_bin_particle_to_pickled(const as_bin *b, uint8_t *pickled);

// Different for CDTs - the operations may return results, so we don't use the
// normal APIs and particle table functions.
extern int as_bin_cdt_read_from_client(const as_bin *b, as_msg_op *op, as_bin *result);
extern int as_bin_cdt_alloc_modify_from_client(as_bin *b, as_msg_op *op, as_bin *result);
extern int as_bin_cdt_stack_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op, as_bin *result);

// Different for LDTs - an LDT's as_list is expensive to generate, so we return
// it from the sizing method, and cache it for later use by the packing method:
extern uint32_t as_ldt_particle_client_value_size(as_storage_rd *rd, as_bin *b, as_val **p_val);
extern uint32_t as_ldt_particle_to_client(as_val *val, as_msg_op *op);

// as_val:
extern int as_bin_particle_replace_from_asval(as_bin *b, const as_val *val);
extern void as_bin_particle_stack_from_asval(as_bin *b, uint8_t* stack, const as_val *val);
extern as_val *as_bin_particle_to_asval(const as_bin *b);

// msgpack:
extern int as_bin_particle_alloc_from_msgpack(as_bin *b, const uint8_t *packed, uint32_t packed_size);

// flat:
extern int as_bin_particle_cast_from_flat(as_bin *b, uint8_t *flat, uint32_t flat_size);
extern int as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, uint32_t flat_size);
extern uint32_t as_bin_particle_flat_size(as_bin *b);
extern uint32_t as_bin_particle_to_flat(const as_bin *b, uint8_t *flat);

// odd as_bin particle functions for specific particle types

// integer:
extern int64_t as_bin_particle_integer_value(const as_bin *b);
extern void as_bin_particle_integer_set(as_bin *b, int64_t i);

// string:
extern uint32_t as_bin_particle_string_ptr(const as_bin *b, char **p_value);

// geojson:
typedef void * geo_region_t;
#define MAX_REGION_CELLS    32
#define MAX_REGION_LEVELS   30
extern size_t as_bin_particle_geojson_cellids(const as_bin *b, uint64_t **pp_cells);
extern bool as_particle_geojson_match(as_particle *p, uint64_t cellid, geo_region_t region, bool is_strict);
extern bool as_particle_geojson_match_asval(const as_val *val, uint64_t cellid, geo_region_t region, bool is_strict);
char const *as_geojson_mem_jsonstr(const as_particle *p, size_t *p_jsonsz);

// list:
struct cdt_payload_s;
struct rollback_alloc_s;
extern void as_bin_particle_list_set_hidden(as_bin *b);
extern void as_bin_particle_list_get_packed_val(const as_bin *b, struct cdt_payload_s *packed);
extern int as_bin_cdt_packed_read(const as_bin *b, as_msg_op *op, as_bin *result);
extern int as_bin_cdt_packed_modify(as_bin *b, as_msg_op *op, as_bin *result, cf_ll_buf *particles_llb);
extern as_particle *packed_list_simple_create_from_buf(struct rollback_alloc_s *alloc_buf, uint32_t ele_count, const uint8_t *buf, uint32_t size);
extern as_particle *packed_list_simple_create_empty(struct rollback_alloc_s *alloc_buf);

// map:
extern void as_bin_particle_map_set_hidden(as_bin *b);


/* as_bin
 * A bin container - null name means unused */
struct as_bin_s {
	as_particle	iparticle;	// 1 byte
	as_particle	*particle;	// for embedded particle this is value, not pointer

	// Never read or write these bytes in single-bin configuration:
	uint16_t	id;			// ID of bin name
	uint8_t		unused;		// pad to 12 bytes (multiple of 4) - legacy
} __attribute__ ((__packed__)) ;

// For data-in-memory namespaces in multi-bin mode, we keep an array of as_bin
// structs in memory, accessed via this struct.
typedef struct as_bin_space_s {
	uint16_t	n_bins;
	as_bin		bins[];
} __attribute__ ((__packed__)) as_bin_space;

// TODO - Do we really need to pad as_bin to 12 bytes for thread safety?
// Do we ever write & read adjacent as_bin structures in a bins array from
// different threads when not under the record lock? And if we're worried about
// 4-byte alignment for that or any other reason, wouldn't we also have to pad
// after n_bins in as_bin_space?

// For data-in-memory namespaces in multi-bin mode, if we're storing extra
// record metadata, we access it via this struct. In this case the index points
// here instead of directly to an as_bin_space.
typedef struct as_rec_space_s {
	as_bin_space*	bin_space;

	// So far the key is the only extra record metadata we store in memory.
	uint32_t		key_size;
	uint8_t			key[];
} __attribute__ ((__packed__)) as_rec_space;

// For copying as_bin structs without the last 3 bytes.
static inline void
as_single_bin_copy(as_bin *to, const as_bin *from)
{
	to->iparticle = from->iparticle;
	to->particle = from->particle;
}

static inline bool
as_bin_inuse(const as_bin *b)
{
	return (((as_particle_iparticle *)b)->state);
}

static inline uint8_t
as_bin_state(const as_bin *b)
{
	return ((as_particle_iparticle *)b)->state;
}

static inline void
as_bin_state_set(as_bin *b, uint8_t val)
{
	((as_particle_iparticle *)b)->state = val;
}

static inline void
as_bin_state_set_from_type(as_bin *b, as_particle_type type)
{
	switch (type) {
	case AS_PARTICLE_TYPE_NULL:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_UNUSED;
		break;
	case AS_PARTICLE_TYPE_INTEGER:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_INTEGER;
		break;
	case AS_PARTICLE_TYPE_FLOAT:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_FLOAT;
		break;
	case AS_PARTICLE_TYPE_TIMESTAMP:
		// TODO - unsupported
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_UNUSED;
		break;
	case AS_PARTICLE_TYPE_HIDDEN_LIST:
	case AS_PARTICLE_TYPE_HIDDEN_MAP:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_HIDDEN;
		break;
	default:
		((as_particle_iparticle *)b)->state = AS_BIN_STATE_INUSE_OTHER;
		break;
	}
}

static inline bool
as_bin_inuse_has(as_storage_rd *rd)
{
	// In-use bins are at the beginning - only need to check the first bin.
	return (rd->n_bins && as_bin_inuse(rd->bins));
}

static inline void
as_bin_set_empty(as_bin *b)
{
	as_bin_state_set(b, AS_BIN_STATE_UNUSED);
}

static inline void
as_bin_set_empty_shift(as_storage_rd *rd, uint32_t i)
{
	// Shift the bins over, so there's no space between used bins.
	// This can overwrite the "emptied" bin, and that's fine.

	uint16_t j;

	for (j = i + 1; j < rd->n_bins; j++) {
		if (! as_bin_inuse(&rd->bins[j])) {
			break;
		}
	}

	uint16_t n = j - (i + 1);

	if (n) {
		memmove(&rd->bins[i], &rd->bins[i + 1], n * sizeof(as_bin));
	}

	// Mark the last bin that was *formerly* in use as null.
	as_bin_set_empty(&rd->bins[j - 1]);
}

static inline void
as_bin_set_empty_from(as_storage_rd *rd, uint16_t from) {
	for (uint16_t i = from; i < rd->n_bins; i++) {
		as_bin_set_empty(&rd->bins[i]);
	}
}

static inline void
as_bin_set_all_empty(as_storage_rd *rd) {
	as_bin_set_empty_from(rd, 0);
}

static inline bool
as_bin_is_embedded_particle(const as_bin *b) {
	return ((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_INTEGER ||
			((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_FLOAT;
}

static inline bool
as_bin_is_external_particle(const as_bin *b) {
	return ((as_particle_iparticle *)b)->state == AS_BIN_STATE_INUSE_OTHER;
}

static inline as_particle *
as_bin_get_particle(as_bin *b) {
	return as_bin_is_embedded_particle(b) ? &b->iparticle : b->particle;
}

static inline bool
as_bin_is_hidden(const as_bin *b) {
	return  (((as_particle_iparticle *)b)->state) == AS_BIN_STATE_INUSE_HIDDEN;
}

// "Embedded" types like integer are stored directly, but other bin types
// ("other" or "hidden") must follow an indirection to get the actual type.
static inline uint8_t
as_bin_get_particle_type(const as_bin *b) {
	switch (((as_particle_iparticle *)b)->state) {
		case AS_BIN_STATE_INUSE_INTEGER:
			return AS_PARTICLE_TYPE_INTEGER;
		case AS_BIN_STATE_INUSE_FLOAT:
			return AS_PARTICLE_TYPE_FLOAT;
		case AS_BIN_STATE_INUSE_OTHER:
			return b->particle->metadata;
		case AS_BIN_STATE_INUSE_HIDDEN:
			return b->particle->metadata;
		default:
			return AS_PARTICLE_TYPE_NULL;
	}
}


/* Bin function declarations */
extern int16_t as_bin_get_id(as_namespace *ns, const char *name);
extern uint16_t as_bin_get_or_assign_id(as_namespace *ns, const char *name);
extern uint16_t as_bin_get_or_assign_id_w_len(as_namespace *ns, const char *name, size_t len);
extern const char* as_bin_get_name_from_id(as_namespace *ns, uint16_t id);
extern bool as_bin_name_within_quota(as_namespace *ns, const char *name);
extern int as_storage_rd_load_n_bins(as_storage_rd *rd);
extern int as_storage_rd_load_bins(as_storage_rd *rd, as_bin *stack_bins);
extern void as_bin_get_all_p(as_storage_rd *rd, as_bin **bin_ptrs);
extern as_bin *as_bin_create(as_storage_rd *rd, const char *name);
extern as_bin *as_bin_create_from_buf(as_storage_rd *rd, uint8_t *name, size_t namesz);
extern as_bin *as_bin_get(as_storage_rd *rd, const char *name);
extern as_bin *as_bin_get_by_id(as_storage_rd *rd, uint32_t id);
extern as_bin *as_bin_get_from_buf(as_storage_rd *rd, uint8_t *name, size_t namesz);
extern as_bin *as_bin_get_or_create(as_storage_rd *rd, const char *name);
extern as_bin *as_bin_get_or_create_from_buf(as_storage_rd *rd, uint8_t *name, size_t namesz, int *p_result);
extern int32_t as_bin_get_index(as_storage_rd *rd, const char *name);
extern int32_t as_bin_get_index_from_buf(as_storage_rd *rd, uint8_t *name, size_t namesz);
extern void as_bin_allocate_bin_space(as_record *r, as_storage_rd *rd, int32_t delta);
extern void as_bin_destroy(as_storage_rd *rd, uint16_t i);
extern void as_bin_destroy_from(as_storage_rd *rd, uint16_t i);
extern void as_bin_destroy_all(as_storage_rd *rd);
extern uint16_t as_bin_inuse_count(as_storage_rd *rd);
extern void as_bin_all_dump(as_storage_rd *rd, char *msg);

extern void as_bin_init(as_namespace *ns, as_bin *b, const char *name);
extern void as_bin_copy(as_namespace *ns, as_bin *to, const as_bin *from);

typedef enum {
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF = 0,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION = 1,
	AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME = 2
} conflict_resolution_pol;

/* Record function declarations */
extern bool as_record_is_live(const as_record *r);
extern int as_record_get_create(struct as_index_tree_s *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns, bool);
extern int as_record_get(struct as_index_tree_s *tree, cf_digest *keyd, as_index_ref *r_ref);
extern int as_record_get_live(struct as_index_tree_s *tree, cf_digest *keyd, as_index_ref *r_ref, as_namespace *ns);
extern int as_record_exists(struct as_index_tree_s *tree, cf_digest *keyd);
extern int as_record_exists_live(struct as_index_tree_s *tree, cf_digest *keyd, as_namespace *ns);
extern void as_record_rescue(as_index_ref *r_ref, as_namespace *ns);

extern void as_record_clean_bins_from(as_storage_rd *rd, uint16_t from);
extern void as_record_clean_bins(as_storage_rd *rd);
extern void as_record_free_bin_space(as_record *r);

extern void as_record_destroy(as_record *r, as_namespace *ns);
extern void as_record_done(as_index_ref *r_ref, as_namespace *ns);

void as_record_drop_stats(as_record* r, as_namespace* ns);

extern void as_record_allocate_key(as_record* r, const uint8_t* key, uint32_t key_size);
extern void as_record_remove_key(as_record* r);
extern int as_record_resolve_conflict(conflict_resolution_pol policy, uint16_t left_gen, uint64_t left_lut, uint16_t right_gen, uint64_t right_lut);
extern int as_record_pickle(as_record *r, as_storage_rd *rd, uint8_t **buf_r, size_t *len_r);
extern int as_record_unpickle_replace(as_record *r, as_storage_rd *rd, uint8_t *buf, size_t bufsz, uint8_t **stack_particles, bool has_sindex);
extern void as_record_apply_pickle(as_storage_rd *rd);
extern bool as_record_apply_replica(as_storage_rd *rd, uint32_t info, struct as_index_tree_s *tree);
extern void as_record_apply_properties(as_record *r, as_namespace *ns, const as_rec_props *p_rec_props);
extern void as_record_clear_properties(as_record *r, as_namespace *ns);
extern void as_record_set_properties(as_storage_rd *rd, const as_rec_props *rec_props);
extern int as_record_set_set_from_msg(as_record *r, as_namespace *ns, as_msg *m);

static inline bool
as_record_pickle_is_binless(const uint8_t *buf)
{
	return *(uint16_t *)buf == 0;
}

extern int32_t as_record_buf_get_stack_particles_sz(uint8_t *buf);

// Set in component if it is dummy (no data). This in
// conjunction with LDT_REC is used to determine if merge
// can be done or not. If this flag is not set then it is
// normal record
#define AS_COMPONENT_FLAG_LDT_DUMMY       0x01
#define AS_COMPONENT_FLAG_LDT_REC         0x02
#define AS_COMPONENT_FLAG_LDT_SUBREC   	  0x04
#define AS_COMPONENT_FLAG_LDT_ESR         0x08
#define AS_COMPONENT_FLAG_MIG             0x10
#define AS_COMPONENT_FLAG_DUP             0x20
#define AS_COMPONENT_FLAG_UNUSED3         0x40
#define AS_COMPONENT_FLAG_UNUSED4         0x80

#define COMPONENT_IS_MIG(c) \
	((c)->flag & AS_COMPONENT_FLAG_MIG)

#define COMPONENT_IS_DUP(c) \
	((c)->flag & AS_COMPONENT_FLAG_DUP)

#define COMPONENT_IS_LDT_PARENT(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_REC)

#define COMPONENT_IS_LDT_DUMMY(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_DUMMY)

#define COMPONENT_IS_LDT_SUBREC(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_SUBREC)

#define COMPONENT_IS_LDT_ESR(c) \
	((c)->flag & AS_COMPONENT_FLAG_LDT_ESR)

#define COMPONENT_IS_LDT_SUB(c) \
	(((c)->flag & AS_COMPONENT_FLAG_LDT_ESR) \
		|| ((c)->flag & AS_COMPONENT_FLAG_LDT_SUBREC))

#define COMPONENT_IS_LDT(c) \
	(COMPONENT_IS_LDT_PARENT((c)) \
		|| COMPONENT_IS_LDT_SUB((c)))

typedef struct {
	uint8_t					*record_buf;
	size_t					record_buf_sz;
	uint32_t				generation;
	uint32_t				void_time;
	uint64_t				last_update_time;
	as_rec_props			rec_props;
	char					flag;
	cf_digest               pdigest;
	cf_digest               edigest;
	uint32_t                pgeneration;
	uint32_t                pvoid_time;
	uint64_t                version;
} as_record_merge_component;

extern int as_record_flatten(as_partition_reservation *rsv, cf_digest *keyd,
		uint32_t n_components, as_record_merge_component *components, int *winner_idx);

// a simpler call that gives seconds in the right epoch
#define as_record_void_time_get() cf_clepoch_seconds()
bool as_record_is_expired(const as_record *r); // TODO - eventually inline

static inline bool
as_record_is_doomed(const as_record *r, struct as_namespace_s *ns)
{
	return as_record_is_expired(r) || as_truncate_record_is_truncated(r, ns);
}

#define AS_SINDEX_MAX		256

#define MIN_PARTITIONS_PER_INDEX 1
#define MAX_PARTITIONS_PER_INDEX 256
#define DEFAULT_PARTITIONS_PER_INDEX 32
#define MAX_PARTITIONS_PER_INDEX_CHAR 3 // Number of characters in max paritions per index

// as_sindex structure which hangs from the ns.
#define AS_SINDEX_INACTIVE			1 // On init, pre-loading
#define AS_SINDEX_ACTIVE			2 // On creation and afterwards
#define AS_SINDEX_DESTROY			3 // On destroy
// dummy sindex state when ai_btree_create() returns error this
// sindex is not available for any of the DML operations
#define AS_SINDEX_NOTCREATED		4 // Un-used flag
#define AS_SINDEX_FLAG_WACTIVE			0x01 // On ai btree create of sindex, never reset
#define AS_SINDEX_FLAG_RACTIVE			0x02 // When sindex scan of database is completed
#define AS_SINDEX_FLAG_DESTROY_CLEANUP 	0x04 // Called for AI clean-up during si deletion
#define AS_SINDEX_FLAG_MIGRATE_CLEANUP  0x08 // Un-used
#define AS_SINDEX_FLAG_POPULATING		0x10 // Indicates current si scan job, reset when scan is done.

struct as_sindex_s;
struct as_sindex_config_s;

#define AS_SET_MAX_COUNT 0xFF	// ID's 8 bits worth minus 1 (ID 0 means no set)
#define AS_BINID_HAS_SINDEX_SIZE  MAX_BIN_NAMES / ( sizeof(uint32_t) * CHAR_BIT )

#define  NS_READ_CONSISTENCY_LEVEL_NAME()								\
	(ns->read_consistency_level_override ?								\
	 (AS_POLICY_CONSISTENCY_LEVEL_ALL == ns->read_consistency_level ? "all" : "one") \
	 : "off")

#define NS_WRITE_COMMIT_LEVEL_NAME()									\
	(ns->write_commit_level_override ?									\
	 (AS_POLICY_COMMIT_LEVEL_ALL == ns->write_commit_level ? "all" : "master") \
	 : "off")

/* as_namespace[_id]
 * A namespace container */
typedef int32_t as_namespace_id; // signed to denote -1 bad namespace id

typedef struct ns_ldt_stats_s {

	/* LDT Operational Statistics */
	cf_atomic_int	ldt_write_reqs;
	cf_atomic_int	ldt_write_success;

	cf_atomic_int	ldt_read_reqs;
	cf_atomic_int	ldt_read_success;

	cf_atomic_int	ldt_delete_reqs;
	cf_atomic_int	ldt_delete_success;

	cf_atomic_int	ldt_update_reqs;

	cf_atomic_int	ldt_errs;
	cf_atomic_int   ldt_err_unknown;
	cf_atomic_int	ldt_err_toprec_not_found;
	cf_atomic_int	ldt_err_item_not_found;
	cf_atomic_int	ldt_err_internal;
	cf_atomic_int	ldt_err_unique_key_violation;

	cf_atomic_int	ldt_err_insert_fail;
	cf_atomic_int	ldt_err_search_fail;
	cf_atomic_int	ldt_err_delete_fail;
	cf_atomic_int	ldt_err_version_mismatch;

	cf_atomic_int	ldt_err_capacity_exceeded;
	cf_atomic_int	ldt_err_param;

	cf_atomic_int	ldt_err_op_bintype_mismatch;
	cf_atomic_int	ldt_err_too_many_open_subrec;
	cf_atomic_int	ldt_err_subrec_not_found;

	cf_atomic_int	ldt_err_bin_does_not_exist;
	cf_atomic_int	ldt_err_bin_exits;
	cf_atomic_int	ldt_err_bin_damaged;

	cf_atomic_int	ldt_err_subrec_internal;
	cf_atomic_int	ldt_err_toprec_internal;
	cf_atomic_int   ldt_err_filter;
	cf_atomic_int	ldt_err_key;
	cf_atomic_int	ldt_err_createspec;
	cf_atomic_int	ldt_err_usermodule;
	cf_atomic_int	ldt_err_input_too_large;
	cf_atomic_int	ldt_err_ldt_not_enabled;

	cf_atomic_int   ldt_gc_io;
	cf_atomic_int   ldt_gc_cnt;
	cf_atomic_int   ldt_gc_no_esr_cnt;
	cf_atomic_int   ldt_gc_no_parent_cnt;
	cf_atomic_int   ldt_gc_parent_version_mismatch_cnt;
	cf_atomic_int   ldt_gc_processed;

	cf_atomic_int	ldt_randomizer_retry;

} ns_ldt_stats;


// TODO - would be nice to put this in as_index.h:
// Callback invoked when as_index is destroyed.
typedef void (*as_index_value_destructor) (struct as_index_s* v, void* udata);

// TODO - would be nice to put this in as_index.h:
typedef struct as_index_tree_shared_s {
	as_index_value_destructor destructor;
	void*			destructor_udata;

	// Number of lock pairs and sprigs per partition tree.
	uint32_t		n_lock_pairs;
	uint32_t		n_sprigs;

	// Bit-shifts used to calculate indexes from digest bits.
	uint32_t		locks_shift;
	uint32_t		sprigs_shift;

	// Offset into as_index_tree struct's variable-sized data.
	uint32_t		sprigs_offset;
} as_index_tree_shared;


struct as_namespace_s {

	char name[AS_ID_NAMESPACE_SZ];
	as_namespace_id id;
	uint32_t namehash;

	//--------------------------------------------
	// Persistent memory.
	//

	// Persistent memory "base" block ID for this namespace.
	uint32_t		xmem_id;

	// Pointer to the persistent memory "base" block.
	uint8_t*		xmem_base;

	// Pointer to partition tree info in persistent memory "treex" block.
	as_treex*		xmem_roots;
	as_treex*		sub_tree_roots; // pointer within treex block

	// Pointer to arena structure (not stages) in persistent memory base block.
	cf_arenax*		arena;

	// Pointer to bin name vmap in persistent memory base block.
	cf_vmapx*		p_bin_name_vmap;

	// Pointer to set information vmap in persistent memory base block.
	cf_vmapx*		p_sets_vmap;

	// Temporary array of sets to hold config values until sets vmap is ready.
	as_set*			sets_cfg_array;
	uint32_t		sets_cfg_count;

	// Configuration flags relevant for warm restart.
	uint32_t		xmem_flags;

	//--------------------------------------------
	// Cold-start.
	//

	// If true, read storage devices to build index at startup.
	bool			cold_start;

	// Flag for cold-start ticker and eviction threshold check.
	bool			cold_start_loading;

	// For cold-start eviction.
	pthread_mutex_t	cold_start_evict_lock;
	uint32_t		cold_start_record_add_count;
	cf_atomic32		cold_start_threshold_void_time;
	uint32_t		cold_start_max_void_time;

	//--------------------------------------------
	// Memory management.
	//

	// JEMalloc arena to be used for long-term storage in this namespace (-1 if nonexistent.)
	int jem_arena;

	// Cached partition ownership info for clients.
	client_replica_map* replica_maps;

	// Common partition tree information. Contains two configuration items.
	as_index_tree_shared tree_shared;

	//--------------------------------------------
	// Storage management.
	//

	// This is typecast to (drv_ssds*) in storage code.
	void*			storage_private;

	uint64_t		ssd_size; // discovered (and rounded) size of drive
	int				storage_last_avail_pct; // most recently calculated available percent
	int				storage_max_write_q; // storage_max_write_cache is converted to this
	uint32_t		saved_defrag_sleep; // restore after defrag at startup is done
	uint32_t		defrag_lwm_size; // storage_defrag_lwm_pct % of storage_write_block_size

	// For data-not-in-memory, we optionally cache swbs after writing to device.
	// To track fraction of reads from cache:
	cf_atomic32		n_reads_from_cache;
	cf_atomic32		n_reads_from_device;

	//--------------------------------------------
	// Truncate records.
	//

	as_truncate		truncate;

	//--------------------------------------------
	// Secondary index.
	//

	int				sindex_cnt;
	uint32_t		n_setless_sindexes;
	struct as_sindex_s* sindex; // array with AS_MAX_SINDEX metadata
	shash*			sindex_set_binid_hash;
	shash*			sindex_iname_hash;
	uint32_t		binid_has_sindex[AS_BINID_HAS_SINDEX_SIZE];

	//--------------------------------------------
	// Configuration.
	//

	uint32_t		cfg_replication_factor;
	uint32_t		replication_factor; // indirect config - can become less than cfg_replication_factor
	uint64_t		memory_size;
	uint64_t		default_ttl;

	PAD_BOOL		enable_xdr;
	PAD_BOOL		sets_enable_xdr; // namespace-level flag to enable set-based xdr shipping
	PAD_BOOL		ns_forward_xdr_writes; // namespace-level flag to enable forwarding of xdr writes
	PAD_BOOL		ns_allow_nonxdr_writes; // namespace-level flag to allow nonxdr writes or not
	PAD_BOOL		ns_allow_xdr_writes; // namespace-level flag to allow xdr writes or not

	uint32_t		cold_start_evict_ttl;
	conflict_resolution_pol conflict_resolution_policy;
	PAD_BOOL		data_in_index; // with single-bin, allows warm restart for data-in-memory (with storage-engine device)
	PAD_BOOL		disallow_null_setname;
	PAD_BOOL		batch_sub_benchmarks_enabled;
	PAD_BOOL		read_benchmarks_enabled;
	PAD_BOOL		udf_benchmarks_enabled;
	PAD_BOOL		udf_sub_benchmarks_enabled;
	PAD_BOOL		write_benchmarks_enabled;
	PAD_BOOL		proxy_hist_enabled;
	uint32_t		evict_hist_buckets;
	uint32_t		evict_tenths_pct;
	uint32_t		hwm_disk_pct;
	uint32_t		hwm_memory_pct;
	PAD_BOOL		ldt_enabled;
	uint32_t		ldt_gc_sleep_us;
	uint32_t		ldt_page_size;
	uint64_t		max_ttl;
	uint32_t		migrate_order;
	uint32_t		migrate_retransmit_ms;
	uint32_t		migrate_sleep;
	cf_atomic32		obj_size_hist_max; // TODO - doesn't need to be atomic, really.
	uint32_t		rack_id;
	as_policy_consistency_level read_consistency_level;
	PAD_BOOL		read_consistency_level_override;
	PAD_BOOL		single_bin; // restrict the namespace to objects with exactly one bin
	uint32_t		stop_writes_pct;
	uint32_t		tomb_raider_eligible_age; // relevant only for enterprise edition
	uint32_t		tomb_raider_period; // relevant only for enterprise edition
	as_policy_commit_level write_commit_level;
	PAD_BOOL		write_commit_level_override;
	cf_vector		xdr_dclist_v;

	as_storage_type storage_type;

	char*			storage_devices[AS_STORAGE_MAX_DEVICES];
	char*			storage_shadows[AS_STORAGE_MAX_DEVICES];
	char*			storage_files[AS_STORAGE_MAX_FILES];
	off_t			storage_filesize;
	char*			storage_scheduler_mode; // relevant for devices only, not files
	uint32_t		storage_write_block_size;
	PAD_BOOL		storage_data_in_memory;
	PAD_BOOL		storage_cold_start_empty;
	uint32_t		storage_defrag_lwm_pct;
	uint32_t		storage_defrag_queue_min;
	uint32_t		storage_defrag_sleep;
	int				storage_defrag_startup_minimum;
	PAD_BOOL		storage_disable_odirect;
	PAD_BOOL		storage_benchmarks_enabled; // histograms are per-drive except device-read-size & device-write-size
	PAD_BOOL		storage_enable_osync;
	uint64_t		storage_flush_max_us;
	uint64_t		storage_fsync_max_us;
	uint64_t		storage_max_write_cache;
	uint32_t		storage_min_avail_pct;
	cf_atomic32 	storage_post_write_queue; // number of swbs/device held after writing to device
	uint32_t		storage_tomb_raider_sleep; // relevant only for enterprise edition
	uint32_t		storage_write_threads;

	uint32_t		sindex_num_partitions;

	PAD_BOOL		geo2dsphere_within_strict;
	uint16_t		geo2dsphere_within_min_level;
	uint16_t		geo2dsphere_within_max_level;
	uint16_t		geo2dsphere_within_max_cells;
	uint16_t		geo2dsphere_within_level_mod;
	uint32_t		geo2dsphere_within_earth_radius_meters;

	//--------------------------------------------
	// Statistics and histograms.
	//

	// Object counts.

	cf_atomic64		n_objects;
	cf_atomic64		n_sub_objects;
	cf_atomic64		n_tombstones; // relevant only for enterprise edition

	// Expiration & eviction (nsup) stats.

	cf_atomic32		stop_writes;
	cf_atomic32		hwm_breached;

	uint64_t		non_expirable_objects;

	cf_atomic64		n_expired_objects;
	cf_atomic64		n_evicted_objects;

	cf_atomic64		evict_ttl;

	uint32_t		nsup_cycle_duration; // seconds taken for most recent nsup cycle
	uint32_t		nsup_cycle_sleep_pct; // fraction of most recent nsup cycle that was spent sleeping

	// Memory usage stats.

	cf_atomic_int	n_bytes_memory;
	cf_atomic64		n_bytes_sindex_memory;

	// Persistent storage stats.

	float			cache_read_pct;

	// Migration stats.

	cf_atomic_int	migrate_tx_partitions_imbalance; // debug only
	cf_atomic_int	migrate_tx_instance_count; // debug only
	cf_atomic_int	migrate_rx_instance_count; // debug only
	cf_atomic_int	migrate_tx_partitions_active;
	cf_atomic_int	migrate_rx_partitions_active;
	cf_atomic_int	migrate_tx_partitions_initial;
	cf_atomic_int	migrate_tx_partitions_remaining;
	cf_atomic_int	migrate_rx_partitions_initial;
	cf_atomic_int	migrate_rx_partitions_remaining;
	cf_atomic_int	migrate_signals_active;
	cf_atomic_int	migrate_signals_remaining;

	// Per-record migration stats:
	cf_atomic_int	migrate_records_skipped; // relevant only for enterprise edition
	cf_atomic_int	migrate_records_transmitted;
	cf_atomic_int	migrate_record_retransmits;
	cf_atomic_int	migrate_record_receives;

	// From-client transaction stats.

	cf_atomic64		n_client_tsvc_error;
	cf_atomic64		n_client_tsvc_timeout;

	cf_atomic64		n_client_proxy_complete;
	cf_atomic64		n_client_proxy_error;
	cf_atomic64		n_client_proxy_timeout;

	cf_atomic64		n_client_read_success;
	cf_atomic64		n_client_read_error;
	cf_atomic64		n_client_read_timeout;
	cf_atomic64		n_client_read_not_found;

	cf_atomic64		n_client_write_success;
	cf_atomic64		n_client_write_error;
	cf_atomic64		n_client_write_timeout;

	// Subset of n_client_write_... above, respectively.
	cf_atomic64		n_xdr_write_success;
	cf_atomic64		n_xdr_write_error;
	cf_atomic64		n_xdr_write_timeout;

	cf_atomic64		n_client_delete_success;
	cf_atomic64		n_client_delete_error;
	cf_atomic64		n_client_delete_timeout;
	cf_atomic64		n_client_delete_not_found;

	cf_atomic64		n_client_udf_complete;
	cf_atomic64		n_client_udf_error;
	cf_atomic64		n_client_udf_timeout;

	cf_atomic64		n_client_lang_read_success;
	cf_atomic64		n_client_lang_write_success;
	cf_atomic64		n_client_lang_delete_success;
	cf_atomic64		n_client_lang_error;

	// Batch sub-transaction stats.

	cf_atomic64		n_batch_sub_tsvc_error;
	cf_atomic64		n_batch_sub_tsvc_timeout;

	cf_atomic64		n_batch_sub_proxy_complete;
	cf_atomic64		n_batch_sub_proxy_error;
	cf_atomic64		n_batch_sub_proxy_timeout;

	cf_atomic64		n_batch_sub_read_success;
	cf_atomic64		n_batch_sub_read_error;
	cf_atomic64		n_batch_sub_read_timeout;
	cf_atomic64		n_batch_sub_read_not_found;

	// Internal-UDF sub-transaction stats.

	cf_atomic64		n_udf_sub_tsvc_error;
	cf_atomic64		n_udf_sub_tsvc_timeout;

	cf_atomic64		n_udf_sub_udf_complete;
	cf_atomic64		n_udf_sub_udf_error;
	cf_atomic64		n_udf_sub_udf_timeout;

	cf_atomic64		n_udf_sub_lang_read_success;
	cf_atomic64		n_udf_sub_lang_write_success;
	cf_atomic64		n_udf_sub_lang_delete_success;
	cf_atomic64		n_udf_sub_lang_error;

	// Transaction retransmit stats.

	uint64_t		n_retransmit_client_read_dup_res;

	uint64_t		n_retransmit_client_write_dup_res;
	uint64_t		n_retransmit_client_write_repl_write;

	uint64_t		n_retransmit_client_delete_dup_res;
	uint64_t		n_retransmit_client_delete_repl_write;

	uint64_t		n_retransmit_client_udf_dup_res;
	uint64_t		n_retransmit_client_udf_repl_write;

	uint64_t		n_retransmit_batch_sub_dup_res;

	uint64_t		n_retransmit_udf_sub_dup_res;
	uint64_t		n_retransmit_udf_sub_repl_write;

	uint64_t		n_retransmit_nsup_repl_write;

	// Scan stats.

	cf_atomic64		n_scan_basic_complete;
	cf_atomic64		n_scan_basic_error;
	cf_atomic64		n_scan_basic_abort;

	cf_atomic64		n_scan_aggr_complete;
	cf_atomic64		n_scan_aggr_error;
	cf_atomic64		n_scan_aggr_abort;

	cf_atomic64		n_scan_udf_bg_complete;
	cf_atomic64		n_scan_udf_bg_error;
	cf_atomic64		n_scan_udf_bg_abort;

	// Query stats.

	cf_atomic64		query_reqs;
	cf_atomic64		query_fail;
	cf_atomic64		query_short_queue_full;
	cf_atomic64		query_long_queue_full;
	cf_atomic64		query_short_reqs;
	cf_atomic64		query_long_reqs;

	cf_atomic64		n_lookup;
	cf_atomic64		n_lookup_success;
	cf_atomic64		n_lookup_abort;
	cf_atomic64		n_lookup_errs;
	cf_atomic64		lookup_response_size;
	cf_atomic64		lookup_num_records;

	cf_atomic64		n_aggregation;
	cf_atomic64		n_agg_success;
	cf_atomic64		n_agg_abort;
	cf_atomic64		n_agg_errs;
	cf_atomic64		agg_response_size;
	cf_atomic64		agg_num_records;

	cf_atomic64		n_query_udf_bg_success;
	cf_atomic64		n_query_udf_bg_failure;

	// Geospatial query stats:
	cf_atomic64		geo_region_query_count;		// number of region queries
	cf_atomic64		geo_region_query_cells;		// number of cells used by region queries
	cf_atomic64		geo_region_query_points;	// number of valid points found
	cf_atomic64		geo_region_query_falsepos;	// number of false positives found

	// Special errors that deserve their own counters:

	cf_atomic64		n_fail_xdr_forbidden;
	cf_atomic64		n_fail_key_busy;
	cf_atomic64		n_fail_generation;
	cf_atomic64		n_fail_record_too_big;

	// Special non-error counters:

	cf_atomic64		n_deleted_last_bin;

	// LDT stats.

	ns_ldt_stats	lstats;

	// One-way automatically activated histograms.

	cf_hist_track*	read_hist;
	cf_hist_track*	write_hist;
	cf_hist_track*	udf_hist;
	cf_hist_track*	query_hist;
	histogram*		query_rec_count_hist;

	PAD_BOOL		read_hist_active;
	PAD_BOOL		write_hist_active;
	PAD_BOOL		udf_hist_active;
	PAD_BOOL		query_hist_active;
	PAD_BOOL		query_rec_count_hist_active;

	// Activate-by-config histograms.

	histogram*		proxy_hist;

	histogram*		read_start_hist;
	histogram*		read_restart_hist;
	histogram*		read_dup_res_hist;
	histogram*		read_local_hist;
	histogram*		read_response_hist;

	histogram*		write_start_hist;
	histogram*		write_restart_hist;
	histogram*		write_dup_res_hist;
	histogram*		write_master_hist; // split this?
	histogram*		write_repl_write_hist;
	histogram*		write_response_hist;

	histogram*		udf_start_hist;
	histogram*		udf_restart_hist;
	histogram*		udf_dup_res_hist;
	histogram*		udf_master_hist; // split this?
	histogram*		udf_repl_write_hist;
	histogram*		udf_response_hist;

	histogram*		batch_sub_start_hist;
	histogram*		batch_sub_restart_hist;
	histogram*		batch_sub_dup_res_hist;
	histogram*		batch_sub_read_local_hist;
	histogram*		batch_sub_response_hist;

	histogram*		udf_sub_start_hist;
	histogram*		udf_sub_restart_hist;
	histogram*		udf_sub_dup_res_hist;
	histogram*		udf_sub_master_hist; // split this?
	histogram*		udf_sub_repl_write_hist;
	histogram*		udf_sub_response_hist;

	histogram*		device_read_size_hist;
	histogram*		device_write_size_hist;

	// Histograms of master object storage sizes. (Meaningful for drive-backed
	// namespaces only.)
	linear_hist*	obj_size_hist;
	linear_hist*	set_obj_size_hists[AS_SET_MAX_COUNT + 1];

	// Histograms used for general eviction and expiration.
	linear_hist*	evict_hist; // not just for info
	linear_hist*	ttl_hist;
	linear_hist*	set_ttl_hists[AS_SET_MAX_COUNT + 1];

	//--------------------------------------------
	// Data partitions.
	//

	as_partition partitions[AS_PARTITIONS];

	//--------------------------------------------
	// Information for rebalancing.
	//

	uint32_t cluster_size;
	cf_node succession[AS_CLUSTER_SZ];
	as_partition_version cluster_versions[AS_CLUSTER_SZ][AS_PARTITIONS];
	uint32_t rack_ids[AS_CLUSTER_SZ];
};

#define AS_SET_NAME_MAX_SIZE	64		// includes space for null-terminator

#define INVALID_SET_ID 0

#define IS_SET_EVICTION_DISABLED(p_set)		(cf_atomic32_get(p_set->disable_eviction) == 1)
#define DISABLE_SET_EVICTION(p_set, on_off)	(cf_atomic32_set(&p_set->disable_eviction, on_off ? 1 : 0))

typedef enum {
	AS_SET_ENABLE_XDR_DEFAULT = 0,
	AS_SET_ENABLE_XDR_TRUE = 1,
	AS_SET_ENABLE_XDR_FALSE = 2
} as_set_enable_xdr_flag;

// Caution - changing this struct could break warm restart.
struct as_set_s {
	char			name[AS_SET_NAME_MAX_SIZE];
	cf_atomic64		n_objects;
	cf_atomic64		n_tombstones;		// relevant only for enterprise edition
	cf_atomic64		n_bytes_memory;		// for data-in-memory only - sets's total record data size
	cf_atomic64		stop_writes_count;	// restrict number of records in a set
	uint64_t		truncate_lut;		// records with last-update-time less than this are truncated
	cf_atomic32		disable_eviction;	// don't evict anything in this set (note - expiration still works)
	cf_atomic32		enable_xdr;			// white-list (AS_SET_ENABLE_XDR_TRUE) or black-list (AS_SET_ENABLE_XDR_FALSE) a set for XDR replication
	uint32_t		n_sindexes;
	uint8_t padding[12];
};

static inline bool
as_set_stop_writes(as_set *p_set) {
	uint64_t n_objects = cf_atomic64_get(p_set->n_objects);
	uint64_t stop_writes_count = cf_atomic64_get(p_set->stop_writes_count);

	return stop_writes_count != 0 && n_objects >= stop_writes_count;
}

// These bin functions must be below definition of struct as_namespace_s:

static inline void
as_bin_set_id_from_name_buf(as_namespace *ns, as_bin *b, uint8_t *buf, int len) {
	if (! ns->single_bin) {
		b->id = as_bin_get_or_assign_id_w_len(ns, (const char *)buf, len);
	}
}

static inline void
as_bin_set_id_from_name(as_namespace *ns, as_bin *b, const char *name) {
	if (! ns->single_bin) {
		b->id = as_bin_get_or_assign_id(ns, name);
	}
}

static inline size_t
as_bin_memcpy_name(as_namespace *ns, uint8_t *buf, as_bin *b) {
	size_t len = 0;

	if (! ns->single_bin) {
		const char *name = as_bin_get_name_from_id(ns, b->id);

		len = strlen(name);
		memcpy(buf, name, len);
	}

	return len;
}

// forward ref
struct as_msg_field_s;

/* Namespace function declarations */
extern as_namespace *as_namespace_create(char *name);
extern void as_namespaces_init(bool cold_start_cmd, uint32_t instance);
extern void as_namespaces_setup(bool cold_start_cmd, uint32_t instance, uint32_t stage_capacity);
extern bool as_namespace_configure_sets(as_namespace *ns);
extern as_namespace *as_namespace_get_byname(char *name);
extern as_namespace *as_namespace_get_byid(uint32_t id);
extern as_namespace *as_namespace_get_bymsgfield(struct as_msg_field_s *fp);
extern as_namespace *as_namespace_get_bybuf(uint8_t *name, size_t len);
extern void as_namespace_eval_write_state(as_namespace *ns, bool *hwm_breached, bool *stop_writes);
extern const char *as_namespace_get_set_name(as_namespace *ns, uint16_t set_id);
extern uint16_t as_namespace_get_set_id(as_namespace *ns, const char *set_name);
extern uint16_t as_namespace_get_create_set_id(as_namespace *ns, const char *set_name);
extern int as_namespace_set_set_w_len(as_namespace *ns, const char *set_name, size_t len, uint16_t *p_set_id, bool apply_restrictions);
extern int as_namespace_get_create_set_w_len(as_namespace *ns, const char *set_name, size_t len, as_set **pp_set, uint16_t *p_set_id);
extern as_set *as_namespace_get_set_by_name(as_namespace *ns, const char *set_name);
extern as_set* as_namespace_get_set_by_id(as_namespace* ns, uint16_t set_id);
extern as_set* as_namespace_get_record_set(as_namespace *ns, const as_record *r);
extern void as_namespace_get_set_info(as_namespace *ns, const char *set_name, cf_dyn_buf *db);
extern void as_namespace_adjust_set_memory(as_namespace *ns, uint16_t set_id, int64_t delta_bytes);
extern void as_namespace_release_set_id(as_namespace *ns, uint16_t set_id);
extern void as_namespace_get_bins_info(as_namespace *ns, cf_dyn_buf *db, bool show_ns);
extern void as_namespace_get_hist_info(as_namespace *ns, char *set_name, char *hist_name,
		cf_dyn_buf *db, bool show_ns);
extern int as_namespace_check_set_limits(as_set * p_set, as_namespace * ns);

// Persistent Memory Management

struct as_treex_s {
	uint64_t root_h: 40;
} __attribute__ ((__packed__));

void as_namespace_xmem_trusted(as_namespace *ns);

// Not namespace class functions, but they live in namespace.c:
uint32_t as_mem_check();

// XXX POST-JUMP - remove in "six months".
static inline uint32_t
truncate_void_time(as_namespace *ns, uint32_t void_time)
{
	uint32_t max_void_time = as_record_void_time_get() + (uint32_t)ns->max_ttl;
	return void_time > max_void_time ? max_void_time : void_time;
}
