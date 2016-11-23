/*
 * storage.h
 *
 * Copyright (C) 2009-2015 Aerospike, Inc.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "base/rec_props.h"


// The type of backing storage configured.
typedef enum {
	AS_STORAGE_ENGINE_UNDEF		= 0,
	AS_STORAGE_ENGINE_MEMORY	= 1,
	AS_STORAGE_ENGINE_SSD		= 2,
	AS_STORAGE_ENGINE_KV		= 3
} as_storage_type;

#define NAMESPACE_HAS_PERSISTENCE(ns) \
	(ns->storage_type != AS_STORAGE_ENGINE_MEMORY) 
// For sizing the storage API "v-tables".
#define AS_STORAGE_ENGINE_TYPES 4

// For invalidating the ssd.file_id and ssd.rblock_id bits in as_index.
#define STORAGE_INVALID_FILE_ID	0x3F // 6 bits
#define STORAGE_INVALID_RBLOCK	0x3FFFFffff // 34 bits

// Forward declarations.
struct as_bin_s;
struct as_index_s;
struct as_partition_vinfo_s;
struct as_namespace_s;
struct drv_ssd_s;
struct drv_ssd_block_s;
struct drv_kv_s;
struct drv_kv_block_s;

// A record descriptor.
typedef struct as_storage_rd_s {
	struct as_index_s		*r;
	struct as_namespace_s	*ns;				// the namespace, which contain all files
	as_rec_props			rec_props;			// list of metadata name-value pairs, e.g. name of set
	struct as_bin_s			*bins;				// pointer to the appropriate bin_space, which is either
												// part of the record (single bin, data_in_memory == true),
												// memalloc'd (multi-bin, data_in_memory == true), or
												// temporary (data_in_memory == false)
												// enables this record's data to be written to drive
	uint16_t		n_bins;
	bool			record_on_device;			// if true, record exists on device
	bool			ignore_record_on_device;	// if true, never read record off device (such as in replace case)
	cf_digest		keyd;						// when doing a write, we'll need to stash this to do the "callback"

	// Parameters used when handling key storage:
	uint32_t		key_size;
	uint8_t			*key;

	bool			is_durable_delete;			// enterprise only

	as_storage_type storage_type;

	union {
		struct {
			struct drv_ssd_block_s	*block;				// data that was read in at one point
			uint8_t					*must_free_block;	// if not null, must free this pointer - may be different to block pointer
														// if null, part of a bigger block that will be freed elsewhere
			struct drv_ssd_s		*ssd;				// the particular ssd object we're using
		} ssd;
		struct {
			struct drv_kv_block_s	*block;				// data that was read in at one point
			uint8_t					*must_free_block;	// if not null, must free this pointer - may be different to block pointer
														// if null, part of a bigger block that will be freed elsewhere
			struct drv_kv_s			*kv;				// the particular kv object we're using
		} kv;
	} u;
} as_storage_rd;


//------------------------------------------------
// Generic "base class" functions that call
// through storage-engine "v-tables".
//

extern void as_storage_init();
extern void as_storage_start_tomb_raider();
extern int as_storage_namespace_destroy(struct as_namespace_s *ns);

extern int as_storage_has_index(struct as_namespace_s *ns);
extern int as_storage_record_exists(struct as_namespace_s *ns, cf_digest *keyd);
extern int as_storage_record_destroy(struct as_namespace_s *ns, struct as_index_s *r); // not the counterpart of as_storage_record_create()

// Start and finish an as_storage_rd usage cycle.
extern int as_storage_record_create(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_open(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_close(as_storage_rd *rd);

// Called within as_storage_rd usage cycle.
extern int as_storage_record_load_n_bins(as_storage_rd *rd);
extern int as_storage_record_load_bins(as_storage_rd *rd);
extern bool as_storage_record_size_and_check(as_storage_rd *rd);
extern int as_storage_record_write(as_storage_rd *rd);

// Storage capacity monitoring.
extern void as_storage_wait_for_defrag();
extern bool as_storage_overloaded(struct as_namespace_s *ns); // returns true if write queue is too backed up
extern bool as_storage_has_space(struct as_namespace_s *ns);
extern void as_storage_defrag_sweep(struct as_namespace_s *ns);

// Storage of generic data into device headers.
extern void as_storage_info_set(struct as_namespace_s *ns, uint32_t pid, const struct as_partition_vinfo_s *vinfo);
extern void as_storage_info_get(struct as_namespace_s *ns, uint32_t pid, struct as_partition_vinfo_s *vinfo);
extern int as_storage_info_flush(struct as_namespace_s *ns);
extern void as_storage_save_evict_void_time(struct as_namespace_s *ns, uint32_t evict_void_time);

// Statistics.
extern int as_storage_stats(struct as_namespace_s *ns, int *available_pct, uint64_t *inuse_disk_bytes); // available percent is that of worst device
extern int as_storage_ticker_stats(struct as_namespace_s *ns); // prints SSD histograms to the info ticker
extern int as_storage_histogram_clear_all(struct as_namespace_s *ns); // clears all SSD histograms


//------------------------------------------------
// Generic functions that don't use "v-tables".
//

// Called within as_storage_rd usage cycle.
extern uint64_t as_storage_record_get_n_bytes_memory(as_storage_rd *rd);
extern void as_storage_record_adjust_mem_stats(as_storage_rd *rd, uint64_t start_bytes);
extern void as_storage_record_drop_from_mem_stats(as_storage_rd *rd);
extern bool as_storage_record_get_key(as_storage_rd *rd);
extern size_t as_storage_record_rec_props_size(as_storage_rd *rd);
extern void as_storage_record_set_rec_props(as_storage_rd *rd, uint8_t* rec_props_data);
extern uint32_t as_storage_record_copy_rec_props(as_storage_rd *rd, as_rec_props *p_rec_props);

// Called only at shutdown to flush all device write-queues.
extern void as_storage_shutdown();


//------------------------------------------------
// AS_STORAGE_ENGINE_MEMORY functions.
//

extern int as_storage_namespace_init_memory(struct as_namespace_s *ns, cf_queue *complete_q, void *udata);
extern void as_storage_start_tomb_raider_memory(struct as_namespace_s *ns);
extern int as_storage_namespace_destroy_memory(struct as_namespace_s *ns);

extern int as_storage_record_write_memory(as_storage_rd *rd);

extern int as_storage_stats_memory(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);


//------------------------------------------------
// AS_STORAGE_ENGINE_SSD functions.
//

extern int as_storage_namespace_init_ssd(struct as_namespace_s *ns, cf_queue *complete_q, void *udata);
extern void as_storage_start_tomb_raider_ssd(struct as_namespace_s *ns);
extern void as_storage_cold_start_ticker_ssd(); // called directly by as_storage_init()
extern int as_storage_namespace_destroy_ssd(struct as_namespace_s *ns);

extern int as_storage_record_destroy_ssd(struct as_namespace_s *ns, struct as_index_s *r);

extern int as_storage_record_create_ssd(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_open_ssd(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_close_ssd(as_storage_rd *rd);

extern int as_storage_record_load_n_bins_ssd(as_storage_rd *rd);
extern int as_storage_record_load_bins_ssd(as_storage_rd *rd);
extern bool as_storage_record_size_and_check_ssd(as_storage_rd *rd);
extern int as_storage_record_write_ssd(as_storage_rd *rd);

extern void as_storage_wait_for_defrag_ssd(struct as_namespace_s *ns);
extern bool as_storage_overloaded_ssd(struct as_namespace_s *ns);
extern bool as_storage_has_space_ssd(struct as_namespace_s *ns);
extern void as_storage_defrag_sweep_ssd(struct as_namespace_s *ns);

extern void as_storage_info_set_ssd(struct as_namespace_s *ns, uint32_t pid, const struct as_partition_vinfo_s *vinfo);
extern void as_storage_info_get_ssd(struct as_namespace_s *ns, uint32_t pid, struct as_partition_vinfo_s *vinfo);
extern int as_storage_info_flush_ssd(struct as_namespace_s *ns);
extern void as_storage_save_evict_void_time_ssd(struct as_namespace_s *ns, uint32_t evict_void_time);

extern int as_storage_stats_ssd(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);
extern int as_storage_ticker_stats_ssd(struct as_namespace_s *ns);
extern int as_storage_histogram_clear_ssd(struct as_namespace_s *ns);

// Called by "base class" functions but not via table.
extern bool as_storage_record_get_key_ssd(as_storage_rd *rd);
extern void as_storage_shutdown_ssd(struct as_namespace_s *ns);


//------------------------------------------------
// AS_STORAGE_ENGINE_KV functions.
//

extern int as_storage_namespace_init_kv(struct as_namespace_s *ns, cf_queue *complete_q, void *udata);
extern int as_storage_namespace_destroy_kv(struct as_namespace_s *ns);

extern int as_storage_has_index_kv(struct as_namespace_s *ns);
extern int as_storage_record_exists_kv(struct as_namespace_s *ns, cf_digest *keyd);

extern int as_storage_record_create_kv(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_open_kv(struct as_namespace_s *ns, struct as_index_s *r, as_storage_rd *rd, cf_digest *keyd);
extern int as_storage_record_close_kv(as_storage_rd *rd);

extern int as_storage_record_load_n_bins_kv(as_storage_rd *rd);
extern int as_storage_record_load_bins_kv(as_storage_rd *rd);
extern int as_storage_record_write_kv(as_storage_rd *rd);

extern int as_storage_stats_kv(struct as_namespace_s *ns, int *available_pct, uint64_t *used_disk_bytes);
