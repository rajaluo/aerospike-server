/*
 * drv_ssd.c
 *
 * Copyright (C) 2009-2016 Aerospike, Inc.
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

/* SYNOPSIS
 * "file" based storage driver, which applies to both SSD namespaces and, in
 * some cases, to file-backed main-memory namespaces.
 */

#include "storage/drv_ssd.h"

#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <linux/fs.h> // for BLKGETSIZE64
#include <sys/ioctl.h>
#include <sys/param.h> // for MAX()

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_random.h"

#include "fault.h"
#include "hist.h"
#include "vmapx.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "base/rec_props.h"
#include "base/secondary_index.h"
#include "base/truncate.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

// Defined in thr_nsup.c, for historical reasons.
extern bool as_cold_start_evict_if_needed(as_namespace* ns);


//==========================================================
// Constants.
//

#define MAX_WRITE_BLOCK_SIZE	(1024 * 1024 * 8)
#define LOAD_BUF_SIZE			MAX_WRITE_BLOCK_SIZE // must be multiple of MAX_WRITE_BLOCK_SIZE

// We round usable device/file size down to SSD_DEFAULT_HEADER_LENGTH plus a
// multiple of LOAD_BUF_SIZE. If we ever change SSD_DEFAULT_HEADER_LENGTH we
// may break backward compatibility since an old header with different size
// could render the rounding ineffective. (There are backward compatibility
// issues anyway if we think we need to change SSD_DEFAULT_HEADER_LENGTH...)
#define SSD_DEFAULT_HEADER_LENGTH	(1024 * 1024 * 8)
#define SSD_HEADER_INFO_STRIDE		(128)

#define DEFRAG_STARTUP_RESERVE	4
#define DEFRAG_RUNTIME_RESERVE	4


//==========================================================
// Typedefs.
//

// Info slice in device header block.
typedef struct info_buf_s {
	uint32_t unused; // used to be len, but was never read
	as_partition_version version;
} __attribute__ ((__packed__)) info_buf;


//------------------------------------------------
// Per-bin metadata on device.
//
typedef struct drv_ssd_bin_s {
	char		name[AS_ID_BIN_SZ];	// 15 aligns well
	uint8_t		version;			// now unused
	uint32_t	offset;				// offset of bin data within block
	uint32_t	len;				// size of bin data
	uint32_t	next;				// location of next bin: block offset
} __attribute__ ((__packed__)) drv_ssd_bin;


//==========================================================
// Miscellaneous utility functions.
//

// Get an open file descriptor from the pool, or a fresh one if necessary.
int
ssd_fd_get(drv_ssd *ssd)
{
	int fd = -1;
	int rv = cf_queue_pop(ssd->fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->name, errno, cf_strerror(errno));
		}
	}

	return fd;
}


int
ssd_shadow_fd_get(drv_ssd *ssd)
{
	int fd = -1;
	int rv = cf_queue_pop(ssd->shadow_fd_q, (void*)&fd, CF_QUEUE_NOWAIT);

	if (rv != CF_QUEUE_OK) {
		fd = open(ssd->shadow_name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED open: errno %d (%s)",
					ssd->shadow_name, errno, cf_strerror(errno));
		}
	}

	return fd;
}


// Save an open file descriptor in the pool
static inline void
ssd_fd_put(drv_ssd *ssd, int fd)
{
	cf_queue_push(ssd->fd_q, (void*)&fd);
}


static inline void
ssd_shadow_fd_put(drv_ssd *ssd, int fd)
{
	cf_queue_push(ssd->shadow_fd_q, (void*)&fd);
}


// Decide which device a record belongs on.
static inline uint32_t
ssd_get_file_id(drv_ssds *ssds, cf_digest *keyd)
{
	return *(uint32_t*)&keyd->digest[DIGEST_STORAGE_BASE_BYTE] % ssds->n_ssds;
}


// Put a wblock on the free queue for reuse.
void
push_wblock_to_free_q(drv_ssd *ssd, uint32_t wblock_id, e_free_to free_to)
{
	if (! ssd->free_wblock_q) { // null until devices are loaded at startup
		return;
	}

	// temp debugging:
	if (wblock_id >= ssd->alloc_table->n_wblocks) {
		cf_warning(AS_DRV_SSD, "pushing invalid wblock_id %d to free_wblock_q",
				(int32_t)wblock_id);
		return;
	}

	if (free_to == FREE_TO_HEAD) {
		cf_queue_push_head(ssd->free_wblock_q, &wblock_id);
	}
	else {
		cf_queue_push(ssd->free_wblock_q, &wblock_id);
	}
}


// Put a wblock on the defrag queue.
static inline void
push_wblock_to_defrag_q(drv_ssd *ssd, uint32_t wblock_id)
{
	if (ssd->defrag_wblock_q) { // null until devices are loaded at startup
		ssd->alloc_table->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(ssd->defrag_wblock_q, &wblock_id);
		cf_atomic64_incr(&ssd->n_defrag_wblock_reads);
	}
}


// Available contiguous size.
static inline uint64_t
available_size(drv_ssd *ssd)
{
	return ssd->free_wblock_q ? // null until devices are loaded at startup
			(uint64_t)cf_queue_sz(ssd->free_wblock_q) * ssd->write_block_size :
			ssd->file_size;

	// Note - returns 100% available during cold start, to make it irrelevant in
	// cold-start eviction threshold check.
}


// Since some write threads can't yet unwind on failure, we ensure that they'll
// succeed by checking before writing on all threads that there's at least one
// wblock per thread. TODO - deprecate this methodology when we can unwind.
static inline int
min_free_wblocks(as_namespace *ns)
{
	// Data-in-memory namespaces process transactions in service threads.
	int n_service_threads = ns->storage_data_in_memory ?
			(int)g_config.n_service_threads : 0;

	int n_transaction_threads = (int)
			(g_config.n_transaction_queues * g_config.n_transaction_threads_per_queue);

	return	n_service_threads +			// client writes
			n_transaction_threads +		// client writes
			g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_RW] + // prole writes
			g_config.n_fabric_channel_recv_threads[AS_FABRIC_CHANNEL_BULK] + // migration writes
			1 +							// always 1 defrag thread
			DEFRAG_RUNTIME_RESERVE +	// reserve for defrag at runtime
			DEFRAG_STARTUP_RESERVE;		// reserve for defrag at startup
			// TODO - what about UDFs?
}


//------------------------------------------------
// ssd_write_buf "swb" methods.
//

static inline ssd_write_buf*
swb_create(drv_ssd *ssd)
{
	ssd_write_buf *swb = (ssd_write_buf*)cf_malloc(sizeof(ssd_write_buf));

	if (! swb) {
		cf_warning(AS_DRV_SSD, "device %s - swb malloc failed", ssd->name);
		return NULL;
	}

	swb->buf = cf_valloc(ssd->write_block_size);

	if (! swb->buf) {
		cf_warning(AS_DRV_SSD, "device %s - swb buf valloc failed", ssd->name);
		cf_free(swb);
		return NULL;
	}

	return swb;
}

static inline void
swb_destroy(ssd_write_buf *swb)
{
	cf_free(swb->buf);
	cf_free(swb);
}

static inline void
swb_reset(ssd_write_buf *swb)
{
	swb->skip_post_write_q = false;
	swb->wblock_id = STORAGE_INVALID_WBLOCK;
	swb->pos = 0;
}

#define swb_reserve(_swb) cf_atomic32_incr(&(_swb)->rc)

static inline void
swb_check_and_reserve(ssd_wblock_state *wblock_state, ssd_write_buf **p_swb)
{
	pthread_mutex_lock(&wblock_state->LOCK);

	if (wblock_state->swb) {
		*p_swb = wblock_state->swb;
		swb_reserve(*p_swb);
	}

	pthread_mutex_unlock(&wblock_state->LOCK);
}

static inline void
swb_release(ssd_write_buf *swb)
{
	if (0 == cf_atomic32_decr(&swb->rc)) {
		swb_reset(swb);

		// Put the swb back on the free queue for reuse.
		cf_queue_push(swb->ssd->swb_free_q, &swb);
	}
}

static inline void
swb_dereference_and_release(drv_ssd *ssd, uint32_t wblock_id,
		ssd_write_buf *swb)
{
	ssd_wblock_state *wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	pthread_mutex_lock(&wblock_state->LOCK);

	if (swb != wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "releasing wrong swb! %p (%d) != %p (%d), thread %lu",
			swb, (int32_t)swb->wblock_id, wblock_state->swb,
			(int32_t)wblock_state->swb->wblock_id, pthread_self());
	}

	swb_release(wblock_state->swb);
	wblock_state->swb = 0;

	if (wblock_state->state != WBLOCK_STATE_DEFRAG) {
		uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

		// Free wblock if all three gating conditions hold.
		if (inuse_sz == 0) {
			push_wblock_to_free_q(ssd, wblock_id, FREE_TO_HEAD);
		}
		// Queue wblock for defrag if applicable.
		else if (inuse_sz < ssd->ns->defrag_lwm_size) {
			push_wblock_to_defrag_q(ssd, wblock_id);
		}
	}
	else {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state is DEFRAG on swb release",
				ssd->name, wblock_id);
	}

	pthread_mutex_unlock(&wblock_state->LOCK);
}

ssd_write_buf *
swb_get(drv_ssd *ssd)
{
	ssd_write_buf *swb;

	if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
		if (! (swb = swb_create(ssd))) {
			return NULL;
		}

		swb->rc = 0;
		swb->n_writers = 0;
		swb->skip_post_write_q = false;
		swb->ssd = ssd;
		swb->wblock_id = STORAGE_INVALID_WBLOCK;
		swb->pos = 0;
	}

	// Find a device block to write to.
	if (CF_QUEUE_OK != cf_queue_pop(ssd->free_wblock_q, &swb->wblock_id,
			CF_QUEUE_NOWAIT)) {
		cf_queue_push(ssd->swb_free_q, &swb);
		return NULL;
	}

	ssd_wblock_state* p_wblock_state =
			&ssd->alloc_table->wblock_state[swb->wblock_id];

	// Sanity checks.
	if (cf_atomic32_get(p_wblock_state->inuse_sz) != 0) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u inuse-size %u off free-q",
				ssd->name, swb->wblock_id,
				cf_atomic32_get(p_wblock_state->inuse_sz));
	}
	if (p_wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not null off free-q",
				ssd->name, swb->wblock_id);
	}
	if (p_wblock_state->state != WBLOCK_STATE_NONE) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not NONE off free-q",
				ssd->name, swb->wblock_id);
	}

	pthread_mutex_lock(&p_wblock_state->LOCK);

	swb_reserve(swb);
	p_wblock_state->swb = swb;

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	return swb;
}

//
// END - ssd_write_buf "swb" methods.
//------------------------------------------------


// Reduce wblock's used size, if result is 0 put it in the "free" pool, if it's
// below the defrag threshold put it in the defrag queue.
void
ssd_block_free(drv_ssd *ssd, uint64_t rblock_id, uint64_t n_rblocks, char *msg)
{
	if (n_rblocks == 0) {
		cf_warning(AS_DRV_SSD, "%s: %s: freeing 0 rblocks, rblock_id %lu",
				ssd->name, msg, rblock_id);
		return;
	}

	// Determine which wblock we're reducing used size in.
	uint64_t start_byte = RBLOCKS_TO_BYTES(rblock_id);
	uint64_t size = RBLOCKS_TO_BYTES(n_rblocks);
	uint32_t wblock_id = BYTES_TO_WBLOCK_ID(ssd, start_byte);
	uint32_t end_wblock_id = BYTES_TO_WBLOCK_ID(ssd, start_byte + size - 1);
	ssd_alloc_table *at = ssd->alloc_table;

	// Sanity-checks.
	if (! (start_byte >= ssd->header_size && wblock_id < at->n_wblocks &&
			wblock_id == end_wblock_id)) {
		cf_warning(AS_DRV_SSD, "%s: %s: invalid range to free, rblock_id %lu, n_rblocks %lu",
				ssd->name, msg, rblock_id, n_rblocks);
		return;
	}

	cf_atomic64_sub(&ssd->inuse_size, size);

	ssd_wblock_state *p_wblock_state = &at->wblock_state[wblock_id];

	pthread_mutex_lock(&p_wblock_state->LOCK);

	int64_t resulting_inuse_sz = cf_atomic32_sub(&p_wblock_state->inuse_sz,
			(int32_t)size);

	if (resulting_inuse_sz < 0 ||
			resulting_inuse_sz >= (int64_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "%s: %s: wblock %d %s, subtracted %d now %ld",
				ssd->name, msg, wblock_id,
				resulting_inuse_sz < 0 ? "over-freed" : "has crazy inuse_sz",
				(int32_t)size, resulting_inuse_sz);

		// TODO - really?
		cf_atomic32_set(&p_wblock_state->inuse_sz, ssd->write_block_size);
	}
	else if (! p_wblock_state->swb &&
			p_wblock_state->state != WBLOCK_STATE_DEFRAG) {
		// Free wblock if all three gating conditions hold.
		if (resulting_inuse_sz == 0) {
			push_wblock_to_free_q(ssd, wblock_id, FREE_TO_HEAD);
		}
		// Queue wblock for defrag if appropriate.
		else if (resulting_inuse_sz < ssd->ns->defrag_lwm_size) {
			push_wblock_to_defrag_q(ssd, wblock_id);
		}
	}

	pthread_mutex_unlock(&p_wblock_state->LOCK);
}


static void
log_bad_record(const char* ns_name, uint32_t n_bins, uint32_t block_bins,
		const drv_ssd_bin* ssd_bin, const char* tag)
{
	cf_info(AS_DRV_SSD, "untrustworthy data from disk [%s], ignoring record", tag);
	cf_info(AS_DRV_SSD, "   ns->name = %s", ns_name);
	cf_info(AS_DRV_SSD, "   bin %" PRIu32 " [of %" PRIu32 "]", (block_bins - n_bins) + 1, block_bins);

	if (ssd_bin) {
		cf_info(AS_DRV_SSD, "   ssd_bin->offset = %" PRIu32, ssd_bin->offset);
		cf_info(AS_DRV_SSD, "   ssd_bin->len = %" PRIu32, ssd_bin->len);
		cf_info(AS_DRV_SSD, "   ssd_bin->next = %" PRIu32, ssd_bin->next);
	}
}


// TODO - sanity-check rec-props?
static bool
is_valid_record(const drv_ssd_block* block, const char* ns_name)
{
	uint8_t* block_head = (uint8_t*)block;
	uint64_t size = (uint64_t)(block->length + LENGTH_BASE);
	drv_ssd_bin* ssd_bin_end = (drv_ssd_bin*)(block_head + size - sizeof(drv_ssd_bin));
	drv_ssd_bin* ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);
	uint32_t n_bins = block->n_bins;

	if (! ssd_cold_start_is_valid_n_bins(n_bins)) {
		log_bad_record(ns_name, n_bins, n_bins, NULL, "bins");
		return false;
	}

	while (n_bins > 0) {
		if (ssd_bin > ssd_bin_end) {
			log_bad_record(ns_name, n_bins, block->n_bins, NULL, "bin ptr");
			return false;
		}

		uint64_t data_offset = (uint64_t)((uint8_t*)(ssd_bin + 1) - block_head);

		if ((uint64_t)ssd_bin->offset != data_offset) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "offset");
			return false;
		}

		uint64_t bin_end_offset = data_offset + (uint64_t)ssd_bin->len;

		if (bin_end_offset > size) {
			log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "length");
			return false;
		}

		if (n_bins > 1) {
			if ((uint64_t)ssd_bin->next != bin_end_offset) {
				log_bad_record(ns_name, n_bins, block->n_bins, ssd_bin, "next ptr");
				return false;
			}

			ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
		}

		n_bins--;
	}

	return true;
}


void
defrag_move_record(drv_ssd *ssd, drv_ssd_block *block, as_index *r)
{
	drv_ssd *old_ssd = ssd;
	uint64_t old_rblock_id = r->rblock_id;
	uint16_t old_n_rblocks = r->n_rblocks;

	drv_ssds *ssds = (drv_ssds*)ssd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	ssd = &ssds->ssds[ssd_get_file_id(ssds, &block->keyd)];

	if (! ssd) {
		cf_warning(AS_DRV_SSD, "{%s} defrag_move_record: no drv_ssd for file_id %u",
				ssds->ns->name, ssd_get_file_id(ssds, &block->keyd));
		return;
	}

	uint32_t write_size = block->length + LENGTH_BASE;

	pthread_mutex_lock(&ssd->defrag_lock);

	ssd_write_buf *swb = ssd->defrag_swb;

	if (! swb) {
		swb = swb_get(ssd);
		ssd->defrag_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "defrag_move_record: couldn't get swb");
			pthread_mutex_unlock(&ssd->defrag_lock);
			return;
		}
	}

	// Check if there's enough space in defrag buffer - if not, free and zero
	// any remaining unused space, enqueue it to be flushed to device, and grab
	// a new buffer.
	if (write_size > ssd->write_block_size - swb->pos) {
		if (ssd->write_block_size != swb->pos) {
			// Clean the end of the buffer before pushing to write queue.
			memset(swb->buf + swb->pos, 0, ssd->write_block_size - swb->pos);
		}

		// Enqueue the buffer, to be flushed to device.
		swb->skip_post_write_q = true;
		cf_queue_push(ssd->swb_write_q, &swb);
		cf_atomic64_incr(&ssd->n_defrag_wblock_writes);

		// Get the new buffer.
		swb = swb_get(ssd);
		ssd->defrag_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "defrag_move_record: couldn't get swb");
			pthread_mutex_unlock(&ssd->defrag_lock);
			return;
		}
	}

	memcpy(swb->buf + swb->pos, (const uint8_t*)block, write_size);

	r->file_id = ssd->file_id;
	r->rblock_id = BYTES_TO_RBLOCKS(WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id) + swb->pos);
	r->n_rblocks = BYTES_TO_RBLOCKS(write_size);

	swb->pos += write_size;

	cf_atomic64_add(&ssd->inuse_size, (int64_t)write_size);
	cf_atomic32_add(&ssd->alloc_table->wblock_state[swb->wblock_id].inuse_sz, (int32_t)write_size);

	pthread_mutex_unlock(&ssd->defrag_lock);

	ssd_block_free(old_ssd, old_rblock_id, old_n_rblocks, "defrag-write");
}


int
ssd_record_defrag(drv_ssd *ssd, drv_ssd_block *block, uint64_t rblock_id,
		uint32_t n_rblocks, uint64_t filepos)
{
	as_namespace *ns = ssd->ns;
	as_partition_reservation rsv;
	uint32_t pid = as_partition_getid(&block->keyd);

	as_partition_reserve_migrate(ns, pid, &rsv, 0);

	int rv;
	as_index_ref r_ref;
	r_ref.skip_lock = false;

	bool found = 0 == as_record_get(rsv.tree, &block->keyd, &r_ref);
	bool is_subrec = false;

	if (ns->ldt_enabled && ! found) {
		found = 0 == as_record_get(rsv.sub_tree, &block->keyd, &r_ref);
		is_subrec = true;
	}

	if (found) {
		as_index *r = r_ref.r;

		if (r->file_id == ssd->file_id && r->rblock_id == rblock_id) {
			if (r->generation != block->generation) {
				cf_warning_digest(AS_DRV_SSD, &r->keyd, "device %s defrag: rblock_id %lu generation mismatch (%u:%u)%s ",
						ssd->name, rblock_id, r->generation, block->generation,
						is_subrec ? " subrec" : "");
			}

			if (r->n_rblocks != n_rblocks) {
				cf_warning_digest(AS_DRV_SSD, &r->keyd, "device %s defrag: rblock_id %lu n_blocks mismatch (%u:%u)%s ",
						ssd->name, rblock_id, r->n_rblocks, n_rblocks,
						is_subrec ? " subrec" : "");
			}

			defrag_move_record(ssd, block, r);

			rv = 0; // record was in index tree and current - moved it
		}
		else {
			rv = -1; // record was in index tree - presumably was overwritten
		}

		as_record_done(&r_ref, ns);
	}
	else {
		rv = -2; // record was not in index tree - presumably was deleted
	}

	as_partition_release(&rsv);

	return rv;
}


bool
ssd_is_full(drv_ssd *ssd, uint32_t wblock_id)
{
	if (cf_queue_sz(ssd->free_wblock_q) > DEFRAG_STARTUP_RESERVE) {
		return false;
	}

	ssd_wblock_state* p_wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	pthread_mutex_lock(&p_wblock_state->LOCK);

	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0) {
		// Lucky - wblock is empty, let ssd_defrag_wblock() free it.
		pthread_mutex_unlock(&p_wblock_state->LOCK);

		return false;
	}

	cf_warning(AS_DRV_SSD, "{%s}: defrag: drive %s totally full, re-queuing wblock %u",
			ssd->ns->name, ssd->name, wblock_id);

	// Not using push_wblock_to_defrag_q() - state is already DEFRAG, we
	// definitely have a queue, and it's better to push back to head.
	cf_queue_push_head(ssd->defrag_wblock_q, &wblock_id);

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	// If we got here, we used all our runtime reserve wblocks, but the wblocks
	// we defragged must still have non-zero inuse_sz. Must wait for those to
	// become free. Sleep prevents retries from overwhelming the log.
	sleep(1);

	return true;
}


int
ssd_defrag_wblock(drv_ssd *ssd, uint32_t wblock_id, uint8_t *read_buf)
{
	if (ssd_is_full(ssd, wblock_id)) {
		return 0;
	}

	int record_count = 0;
	int num_old_records = 0;
	int num_deleted_records = 0;

	ssd_wblock_state* p_wblock_state = &ssd->alloc_table->wblock_state[wblock_id];

	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0) {
		goto Finished;
	}

	int fd = ssd_fd_get(ssd);
	uint64_t file_offset = WBLOCK_ID_TO_BYTES(ssd, wblock_id);

	uint64_t start_ns = ssd->ns->storage_benchmarks_enabled ? cf_getns() : 0;

	if (lseek(fd, (off_t)file_offset, SEEK_SET) != (off_t)file_offset) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: offset %lu: errno %d (%s)",
				ssd->name, file_offset, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Finished;
	}

	ssize_t rlen = read(fd, read_buf, ssd->write_block_size);

	if (rlen != (ssize_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd->name, rlen, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Finished;
	}

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_large_block_read, start_ns);
	}

	ssd_fd_put(ssd, fd);

	size_t wblock_offset = 0; // current offset within the wblock, in bytes

	while (wblock_offset < ssd->write_block_size &&
			cf_atomic32_get(p_wblock_state->inuse_sz) != 0) {
		drv_ssd_block *block = (drv_ssd_block*)&read_buf[wblock_offset];

		if (block->magic != SSD_BLOCK_MAGIC) {
			// First block must have magic.
			if (wblock_offset == 0) {
				cf_warning(AS_DRV_SSD, "BLOCK CORRUPTED: device %s has bad data on wblock %d",
						ssd->name, wblock_id);
				break;
			}

			// Later blocks may have no magic, just skip to next block.
			wblock_offset += RBLOCK_SIZE;
			continue;
		}

		// Note - if block->length is sane, we don't need to round up to a
		// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
		size_t next_wblock_offset = wblock_offset +
				BYTES_TO_RBLOCK_BYTES(block->length + LENGTH_BASE);

		if (next_wblock_offset > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "error: block extends over read size: foff %"PRIu64" boff %"PRIu64" blen %"PRIu64,
				file_offset, wblock_offset, (uint64_t)block->length);
			break;
		}

		// Found a good record, move it if it's current.
		int rv = ssd_record_defrag(ssd, block,
				BYTES_TO_RBLOCKS(file_offset + wblock_offset),
				(uint32_t)BYTES_TO_RBLOCKS(next_wblock_offset - wblock_offset),
				file_offset + wblock_offset);

		if (rv == 0) {
			record_count++;
		}
		else if (rv == -1) {
			num_old_records++;
		}
		else if (rv == -2) {
			num_deleted_records++;
		}

		wblock_offset = next_wblock_offset;
	}

Finished:

	// Note - usually wblock's inuse_sz is 0 here, but may legitimately be non-0
	// e.g. if a dropped partition's tree is not done purging. In this case, we
	// may have found deleted records in the wblock whose used-size contribution
	// has not yet been subtracted.

	cf_detail(AS_DRV_SSD, "device %s: wblock-id %u defragged, final in-use-sz %d records (%d:%d:%d)",
			ssd->name, wblock_id, cf_atomic32_get(p_wblock_state->inuse_sz),
			record_count, num_old_records, num_deleted_records);

	// Sanity checks.
	if (p_wblock_state->swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not null while defragging",
				ssd->name, wblock_id);
	}
	if (p_wblock_state->state != WBLOCK_STATE_DEFRAG) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not DEFRAG while defragging",
				ssd->name, wblock_id);
	}

	pthread_mutex_lock(&p_wblock_state->LOCK);

	p_wblock_state->state = WBLOCK_STATE_NONE;

	// Free the wblock if it's empty.
	if (cf_atomic32_get(p_wblock_state->inuse_sz) == 0 &&
			! p_wblock_state->swb) {
		push_wblock_to_free_q(ssd, wblock_id, FREE_TO_HEAD);
	}

	pthread_mutex_unlock(&p_wblock_state->LOCK);

	return record_count;
}


// Thread "run" function to service a device's defrag queue.
void*
run_defrag(void *pv_data)
{
	drv_ssd *ssd = (drv_ssd*)pv_data;
	uint32_t wblock_id;
	uint8_t *read_buf = cf_valloc(ssd->write_block_size);

	if (! read_buf) {
		cf_crash(AS_DRV_SSD, "device %s: defrag valloc failed", ssd->name);
	}

	while (true) {
		uint32_t q_min = ssd->ns->storage_defrag_queue_min;

		if (q_min != 0) {
			if (cf_queue_sz(ssd->defrag_wblock_q) > q_min) {
				if (CF_QUEUE_OK !=
						cf_queue_pop(ssd->defrag_wblock_q, &wblock_id,
								CF_QUEUE_NOWAIT)) {
					// Should never get here!
					break;
				}
			}
			else {
				usleep(1000 * 50);
				continue;
			}
		}
		else {
			if (CF_QUEUE_OK !=
					cf_queue_pop(ssd->defrag_wblock_q, &wblock_id,
							CF_QUEUE_FOREVER)) {
				// Should never get here!
				break;
			}
		}

		ssd_defrag_wblock(ssd, wblock_id, read_buf);

		uint32_t sleep_us = ssd->ns->storage_defrag_sleep;

		if (sleep_us != 0) {
			usleep(sleep_us);
		}
	}

	// Although we ever expect to get here...
	cf_free(read_buf);
	cf_warning(AS_DRV_SSD, "device %s: quit defrag - queue error", ssd->name);

	return NULL;
}


void
ssd_start_defrag_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} starting defrag threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		if (pthread_create(&ssd->defrag_thread, NULL, run_defrag,
				(void*)ssd) != 0) {
			cf_crash(AS_DRV_SSD, "%s defrag thread failed", ssd->name);
		}
	}
}


//------------------------------------------------
// defrag_pen class.
//

#define DEFRAG_PEN_INIT_CAPACITY (8 * 1024)

typedef struct defrag_pen_s {
	uint32_t n_ids;
	uint32_t capacity;
	uint32_t *ids;
	uint32_t stack_ids[DEFRAG_PEN_INIT_CAPACITY];
} defrag_pen;

static void
defrag_pen_init(defrag_pen *pen)
{
	pen->n_ids = 0;
	pen->capacity = DEFRAG_PEN_INIT_CAPACITY;
	pen->ids = pen->stack_ids;
}

static void
defrag_pen_destroy(defrag_pen *pen)
{
	if (pen->ids != pen->stack_ids) {
		cf_free(pen->ids);
	}
}

static void
defrag_pen_add(defrag_pen *pen, uint32_t wblock_id)
{
	if (pen->n_ids == pen->capacity) {
		if (pen->capacity == DEFRAG_PEN_INIT_CAPACITY) {
			pen->capacity <<= 2;
			pen->ids = cf_malloc(pen->capacity * sizeof(uint32_t));
			memcpy(pen->ids, pen->stack_ids, sizeof(pen->stack_ids));
		}
		else {
			pen->capacity <<= 1;
			pen->ids = cf_realloc(pen->ids, pen->capacity * sizeof(uint32_t));
		}
	}

	pen->ids[pen->n_ids++] = wblock_id;
}

static void
defrag_pen_transfer(defrag_pen *pen, drv_ssd *ssd)
{
	// For speed, "customize" instead of using push_wblock_to_defrag_q()...
	for (uint32_t i = 0; i < pen->n_ids; i++) {
		uint32_t wblock_id = pen->ids[i];

		ssd->alloc_table->wblock_state[wblock_id].state = WBLOCK_STATE_DEFRAG;
		cf_queue_push(ssd->defrag_wblock_q, &wblock_id);
	}
}

static void
defrag_pens_dump(defrag_pen pens[], uint32_t n_pens, const char* ssd_name)
{
	char buf[2048];
	uint32_t n = 0;
	int pos = sprintf(buf, "%u", pens[n++].n_ids);

	while (n < n_pens) {
		pos += sprintf(buf + pos, ",%u", pens[n++].n_ids);
	}

	cf_info(AS_DRV_SSD, "%s init defrag profile: %s", ssd_name, buf);
}

//
// END - defrag_pen class.
//------------------------------------------------


// Thread "run" function to create and load a device's (wblock) free & defrag
// queues at startup. Sorts defrag-eligible wblocks so the most depleted ones
// are at the head of the defrag queue.
void*
run_load_queues(void *pv_data)
{
	drv_ssd *ssd = (drv_ssd*)pv_data;

	// TODO - would be nice to have a queue create of specified capacity.
	if (! (ssd->free_wblock_q = cf_queue_create(sizeof(uint32_t), true))) {
		cf_crash(AS_DRV_SSD, "%s free wblock queue create failed", ssd->name);
	}

	if (! (ssd->defrag_wblock_q = cf_queue_create(sizeof(uint32_t), true))) {
		cf_crash(AS_DRV_SSD, "%s defrag queue create failed", ssd->name);
	}

	as_namespace *ns = ssd->ns;
	uint32_t lwm_pct = ns->storage_defrag_lwm_pct;
	uint32_t lwm_size = ns->defrag_lwm_size;
	defrag_pen pens[lwm_pct];

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_init(&pens[n]);
	}

	ssd_alloc_table* at = ssd->alloc_table;
	uint32_t first_id = BYTES_TO_WBLOCK_ID(ssd, ssd->header_size);
	uint32_t last_id = at->n_wblocks;

	for (uint32_t wblock_id = first_id; wblock_id < last_id; wblock_id++) {
		uint32_t inuse_sz = at->wblock_state[wblock_id].inuse_sz;

		if (inuse_sz == 0) {
			// Faster than using push_wblock_to_free_q() here...
			cf_queue_push(ssd->free_wblock_q, &wblock_id);
		}
		else if (inuse_sz < lwm_size) {
			defrag_pen_add(&pens[(inuse_sz * lwm_pct) / lwm_size], wblock_id);
		}
	}

	defrag_pens_dump(pens, lwm_pct, ssd->name);

	for (uint32_t n = 0; n < lwm_pct; n++) {
		defrag_pen_transfer(&pens[n], ssd);
		defrag_pen_destroy(&pens[n]);
	}

	ssd->n_defrag_wblock_reads = (uint64_t)cf_queue_sz(ssd->defrag_wblock_q);

	return NULL;
}


void
ssd_load_wblock_queues(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} loading free & defrag queues", ssds->ns->name);

	// Split this task across multiple threads.
	pthread_t q_load_threads[ssds->n_ssds];

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		if (pthread_create(&q_load_threads[i], NULL, run_load_queues,
				(void*)ssd) != 0) {
			cf_crash(AS_DRV_SSD, "%s load queues thread failed", ssd->name);
		}
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		pthread_join(q_load_threads[i], NULL);
	}
	// Now we're single-threaded again.

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		cf_info(AS_DRV_SSD, "%s init wblock free-q %d, defrag-q %d", ssd->name,
				cf_queue_sz(ssd->free_wblock_q),
				cf_queue_sz(ssd->defrag_wblock_q));
	}
}


void
ssd_wblock_init(drv_ssd *ssd)
{
	uint32_t n_wblocks = ssd->file_size / ssd->write_block_size;

	cf_info(AS_DRV_SSD, "%s has %u wblocks of size %u", ssd->name, n_wblocks,
			ssd->write_block_size);

	ssd_alloc_table *at = cf_malloc(sizeof(ssd_alloc_table) + (n_wblocks * sizeof(ssd_wblock_state)));

	at->n_wblocks = n_wblocks;

	// Device header wblocks' inuse_sz will (also) be 0 but that doesn't matter.
	for (uint32_t i = 0; i < n_wblocks; i++) {
		pthread_mutex_init(&at->wblock_state[i].LOCK, 0);
		at->wblock_state[i].state = WBLOCK_STATE_NONE;
		cf_atomic32_set(&at->wblock_state[i].inuse_sz, 0);
		at->wblock_state[i].swb = 0;
	}

	ssd->alloc_table = at;
}


//==========================================================
// Record reading utilities.
//

int
ssd_read_record(as_storage_rd *rd)
{
	as_namespace *ns = rd->ns;
	as_record *r = rd->r;

	if (STORAGE_RBLOCK_IS_INVALID(r->rblock_id)) {
		cf_warning_digest(AS_DRV_SSD, &r->keyd, "{%s} read_ssd: invalid rblock_id ",
				ns->name);
		return -1;
	}

	uint64_t record_offset = RBLOCKS_TO_BYTES(r->rblock_id);
	uint64_t record_size = RBLOCKS_TO_BYTES(r->n_rblocks);

	uint8_t *read_buf = NULL;
	drv_ssd_block *block = NULL;

	drv_ssd *ssd = rd->ssd;
	ssd_write_buf *swb = 0;
	uint32_t wblock = RBLOCK_ID_TO_WBLOCK_ID(ssd, r->rblock_id);

	swb_check_and_reserve(&ssd->alloc_table->wblock_state[wblock], &swb);

	if (swb) {
		// Data is in write buffer, so read it from there.
		cf_atomic32_incr(&ns->n_reads_from_cache);

		read_buf = cf_malloc(record_size);

		if (! read_buf) {
			return -1;
		}

		block = (drv_ssd_block*)read_buf;

		int swb_offset = record_offset - WBLOCK_ID_TO_BYTES(ssd, wblock);
		memcpy(read_buf, swb->buf + swb_offset, record_size);
		swb_release(swb);
	}
	else {
		// Normal case - data is read from device.
		cf_atomic32_incr(&ns->n_reads_from_device);

		uint64_t record_end_offset = record_offset + record_size;
		uint64_t read_offset = BYTES_DOWN_TO_IO_MIN(ssd, record_offset);
		uint64_t read_end_offset = BYTES_UP_TO_IO_MIN(ssd, record_end_offset);
		size_t read_size = read_end_offset - read_offset;
		uint64_t record_buf_indent = record_offset - read_offset;

		read_buf = cf_valloc(read_size);

		if (! read_buf) {
			return -1;
		}

		int fd = ssd_fd_get(ssd);

		uint64_t start_ns = ns->storage_benchmarks_enabled ? cf_getns() : 0;

		if (lseek(fd, (off_t)read_offset, SEEK_SET) != (off_t)read_offset) {
			cf_warning(AS_DRV_SSD, "%s: seek failed: offset %lu: errno %d (%s)",
					ssd->name, read_offset, errno, cf_strerror(errno));
			cf_free(read_buf);
			close(fd);
			return -1;
		}

		ssize_t rv = read(fd, read_buf, read_size);

		if (rv != (ssize_t)read_size) {
			cf_warning(AS_DRV_SSD, "%s: read failed (%ld): size %lu: errno %d (%s)",
					ssd->name, rv, read_size, errno, cf_strerror(errno));
			cf_free(read_buf);
			close(fd);
			return -1;
		}

		if (start_ns != 0) {
			histogram_insert_data_point(ssd->hist_read, start_ns);
		}

		ssd_fd_put(ssd, fd);

		block = (drv_ssd_block*)(read_buf + record_buf_indent);

		// Sanity checks.
		if (block->magic != SSD_BLOCK_MAGIC) {
			cf_warning(AS_DRV_SSD, "read: bad block magic offset %"PRIu64,
					read_offset);
			cf_free(read_buf);
			return -1;
		}
		if (0 != cf_digest_compare(&block->keyd, &r->keyd)) {
			cf_warning(AS_DRV_SSD, "read: read wrong key: expecting %"PRIx64" got %"PRIx64,
				*(uint64_t*)&r->keyd, *(uint64_t*)&block->keyd);
			cf_free(read_buf);
			return -1;
		}

		if (ns->storage_benchmarks_enabled) {
			histogram_insert_raw(ns->device_read_size_hist, read_size);
		}
	}

	rd->block = block;
	rd->must_free_block = read_buf;

	return 0;
}


//==========================================================
// Storage API implementation: reading records.
//

int
as_storage_record_load_n_bins_ssd(as_storage_rd *rd)
{
	if (! as_record_is_live(rd->r)) {
		rd->n_bins = 0;
		return 0; // no need to read device
	}

	// If the record hasn't been read, read it.
	if (! rd->block && ssd_read_record(rd) != 0) {
		cf_warning(AS_DRV_SSD, "load_n_bins: failed ssd_read_record()");
		return -1;
	}

	rd->n_bins = rd->block->n_bins;
	return 0;
}


int
as_storage_record_load_bins_ssd(as_storage_rd *rd)
{
	if (! as_record_is_live(rd->r)) {
		return 0; // no need to read device
	}

	// If the record hasn't been read, read it.
	if (! rd->block && ssd_read_record(rd) != 0) {
		cf_warning(AS_DRV_SSD, "load_bins: failed ssd_read_record()");
		return -1;
	}

	drv_ssd_block *block = rd->block;
	uint8_t *block_head = (uint8_t*)rd->block;

	drv_ssd_bin *ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);

	for (uint16_t i = 0; i < block->n_bins; i++) {
		as_bin_set_id_from_name(rd->ns, &rd->bins[i], ssd_bin->name);

		int rv = as_bin_particle_cast_from_flat(&rd->bins[i],
				block_head + ssd_bin->offset, ssd_bin->len);

		if (0 != rv) {
			return rv;
		}

		ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);
	}

	return 0;
}


bool
as_storage_record_get_key_ssd(as_storage_rd *rd)
{
	// If the record hasn't been read, read it.
	if (! rd->block && ssd_read_record(rd) != 0) {
		cf_warning(AS_DRV_SSD, "get_key: failed ssd_read_record()");
		return false;
	}

	drv_ssd_block *block = rd->block;
	as_rec_props props;

	props.size = block->bins_offset;

	if (props.size == 0) {
		return false;
	}

	props.p_data = block->data;

	return as_rec_props_get_value(&props, CL_REC_PROPS_FIELD_KEY,
			&rd->key_size, &rd->key) == 0;
}


//==========================================================
// Record writing utilities.
//

void
ssd_flush_swb(drv_ssd *ssd, ssd_write_buf *swb)
{
	// Wait for all writers to finish.
	while (cf_atomic32_get(swb->n_writers) != 0) {
		;
	}

	int fd = ssd_fd_get(ssd);
	off_t write_offset = (off_t)WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id);

	uint64_t start_ns = ssd->ns->storage_benchmarks_enabled ? cf_getns() : 0;

	if (lseek(fd, write_offset, SEEK_SET) != write_offset) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: offset %ld: errno %d (%s)",
				ssd->name, write_offset, errno, cf_strerror(errno));
	}

	ssize_t rv_s = write(fd, swb->buf, ssd->write_block_size);

	if (rv_s != (ssize_t)ssd->write_block_size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_write, start_ns);
	}

	ssd_fd_put(ssd, fd);
}


void
ssd_shadow_flush_swb(drv_ssd *ssd, ssd_write_buf *swb)
{
	int fd = ssd_shadow_fd_get(ssd);
	off_t write_offset = (off_t)WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id);

	uint64_t start_ns = ssd->ns->storage_benchmarks_enabled ? cf_getns() : 0;

	if (lseek(fd, write_offset, SEEK_SET) != write_offset) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: offset %ld: errno %d (%s)",
				ssd->shadow_name, write_offset, errno, cf_strerror(errno));
	}

	ssize_t rv_s = write(fd, swb->buf, ssd->write_block_size);

	if (rv_s != (ssize_t)ssd->write_block_size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_shadow_write, start_ns);
	}

	ssd_shadow_fd_put(ssd, fd);
}


void
ssd_write_sanity_checks(drv_ssd *ssd, ssd_write_buf *swb)
{
	ssd_wblock_state* p_wblock_state =
			&ssd->alloc_table->wblock_state[swb->wblock_id];

	if (p_wblock_state->swb != swb) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u swb not consistent while writing",
				ssd->name, swb->wblock_id);
	}

	if (p_wblock_state->state != WBLOCK_STATE_NONE) {
		cf_warning(AS_DRV_SSD, "device %s: wblock-id %u state not NONE while writing",
				ssd->name, swb->wblock_id);
	}
}


void
ssd_post_write(drv_ssd *ssd, ssd_write_buf *swb)
{
	if (cf_atomic32_get(ssd->ns->storage_post_write_queue) == 0 ||
			swb->skip_post_write_q) {
		swb_dereference_and_release(ssd, swb->wblock_id, swb);
	}
	else {
		// Transfer swb to post-write queue.
		cf_queue_push(ssd->post_write_q, &swb);
	}

	if (ssd->post_write_q) {
		// Release post-write queue swbs if we're over the limit.
		while ((uint32_t)cf_queue_sz(ssd->post_write_q) >
				cf_atomic32_get(ssd->ns->storage_post_write_queue)) {
			ssd_write_buf* cached_swb;

			if (CF_QUEUE_OK != cf_queue_pop(ssd->post_write_q, &cached_swb,
					CF_QUEUE_NOWAIT)) {
				// Should never happen.
				cf_warning(AS_DRV_SSD, "device %s: post-write queue pop failed",
						ssd->name);
				break;
			}

			swb_dereference_and_release(ssd, cached_swb->wblock_id,
					cached_swb);
		}
	}
}


// Thread "run" function that flushes write buffers to device.
void *
ssd_write_worker(void *arg)
{
	drv_ssd *ssd = (drv_ssd*)arg;

	while (ssd->running) {
		ssd_write_buf *swb;

		if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_write_q, &swb, 100)) {
			continue;
		}

		// Sanity checks (optional).
		ssd_write_sanity_checks(ssd, swb);

		// Flush to the device.
		ssd_flush_swb(ssd, swb);

		if (ssd->shadow_name) {
			// Queue for shadow device write.
			cf_queue_push(ssd->swb_shadow_q, &swb);
		}
		else {
			// Transfer to post-write queue, or release swb, as appropriate.
			ssd_post_write(ssd, swb);
		}
	} // infinite event loop waiting for block to write

	return NULL;
}


// Thread "run" function that flushes write buffers to shadow device.
void *
ssd_shadow_worker(void *arg)
{
	drv_ssd *ssd = (drv_ssd*)arg;

	while (ssd->running) {
		ssd_write_buf *swb;

		if (CF_QUEUE_OK != cf_queue_pop(ssd->swb_shadow_q, &swb, 100)) {
			continue;
		}

		// Sanity checks (optional).
		ssd_write_sanity_checks(ssd, swb);

		// Flush to the shadow device.
		ssd_shadow_flush_swb(ssd, swb);

		// Transfer to post-write queue, or release swb, as appropriate.
		ssd_post_write(ssd, swb);
	}

	return NULL;
}


void
ssd_start_write_worker_threads(drv_ssds *ssds)
{
	if (ssds->ns->storage_write_threads > MAX_SSD_THREADS) {
		cf_warning(AS_DRV_SSD, "configured number of write threads %u greater than max, using %d instead",
				ssds->ns->storage_write_threads, MAX_SSD_THREADS);
		ssds->ns->storage_write_threads = MAX_SSD_THREADS;
	}

	cf_info(AS_DRV_SSD, "{%s} starting write worker threads", ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		for (uint32_t j = 0; j < ssds->ns->storage_write_threads; j++) {
			pthread_create(&ssd->write_worker_thread[j], 0, ssd_write_worker,
					(void*)ssd);
		}

		if (ssd->shadow_name) {
			pthread_create(&ssd->shadow_worker_thread, 0, ssd_shadow_worker,
					(void*)ssd);
		}
	}
}


uint32_t
as_storage_record_overhead_size(as_storage_rd *rd)
{
	// Start with size of record header struct.
	size_t size = sizeof(drv_ssd_block);

	// Add size of any record properties.
	if (rd->rec_props.p_data) {
		size += rd->rec_props.size;
	}

	return (uint32_t)size;
}


uint32_t
as_storage_record_size(as_storage_rd *rd)
{
	// Start with the record storage overhead, including vinfo and rec-props.
	uint32_t write_size = as_storage_record_overhead_size(rd);

	// Add the bins' sizes, including bin overhead.
	for (uint16_t i = 0; i < rd->n_bins; i++) {
		as_bin *bin = &rd->bins[i];

		if (! as_bin_inuse(bin)) {
			break;
		}

		// TODO: could factor out sizeof(drv_ssd_bin) and multiply by i, but
		// for now let's favor the low bin-count case and leave it this way.
		write_size += sizeof(drv_ssd_bin) + as_bin_particle_flat_size(bin);
	}

	return write_size;
}


static inline uint32_t
ssd_write_calculate_size(as_storage_rd *rd)
{
	// Note - this function is the only place where rounding size (up to a
	// multiple of RBLOCK_SIZE) is really necessary.

	return BYTES_TO_RBLOCK_BYTES(as_storage_record_size(rd));
}


int
ssd_write_bins(as_storage_rd *rd)
{
	as_namespace *ns = rd->ns;
	as_record *r = rd->r;
	drv_ssd *ssd = rd->ssd;

	uint32_t write_size = ssd_write_calculate_size(rd);

	if (write_size > ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "write: rejecting %"PRIx64" write size: %u",
				*(uint64_t*)&r->keyd, write_size);
		return -AS_PROTO_RESULT_FAIL_RECORD_TOO_BIG;
	}

	// Reserve the portion of the current swb where this record will be written.
	pthread_mutex_lock(&ssd->write_lock);

	ssd_write_buf *swb = ssd->current_swb;

	if (! swb) {
		swb = swb_get(ssd);
		ssd->current_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "write bins: couldn't get swb");
			pthread_mutex_unlock(&ssd->write_lock);
			return -AS_PROTO_RESULT_FAIL_OUT_OF_SPACE;
		}
	}

	// Check if there's enough space in current buffer - if not, free and zero
	// any remaining unused space, enqueue it to be flushed to device, and grab
	// a new buffer.
	if (write_size > ssd->write_block_size - swb->pos) {
		if (ssd->write_block_size != swb->pos) {
			// Clean the end of the buffer before pushing to write queue.
			memset(&swb->buf[swb->pos], 0, ssd->write_block_size - swb->pos);
		}

		// Enqueue the buffer, to be flushed to device.
		cf_queue_push(ssd->swb_write_q, &swb);
		cf_atomic64_incr(&ssd->n_wblock_writes);

		// Get the new buffer.
		swb = swb_get(ssd);
		ssd->current_swb = swb;

		if (! swb) {
			cf_warning(AS_DRV_SSD, "write bins: couldn't get swb");
			pthread_mutex_unlock(&ssd->write_lock);
			return -AS_PROTO_RESULT_FAIL_OUT_OF_SPACE;
		}
	}

	// There's enough space - save the position where this record will be
	// written, and advance swb->pos for the next writer.
	uint32_t swb_pos = swb->pos;

	swb->pos += write_size;
	cf_atomic32_incr(&swb->n_writers);

	pthread_mutex_unlock(&ssd->write_lock);
	// May now write this record concurrently with others in this swb.

	// Flatten data into the block.

	uint8_t *buf = &swb->buf[swb_pos];
	uint8_t *buf_start = buf;

	drv_ssd_block *block = (drv_ssd_block*)buf;

	buf += sizeof(drv_ssd_block);

	// Properties list goes just before bins.
	if (rd->rec_props.p_data) {
		memcpy(buf, rd->rec_props.p_data, rd->rec_props.size);
		buf += rd->rec_props.size;
	}

	uint16_t n_bins_written;

	for (n_bins_written = 0; n_bins_written < rd->n_bins; n_bins_written++) {
		as_bin *bin = &rd->bins[n_bins_written];

		if (! as_bin_inuse(bin)) {
			break;
		}

		drv_ssd_bin *ssd_bin = (drv_ssd_bin*)buf;

		buf += sizeof(drv_ssd_bin);

		ssd_bin->version = 0;

		if (ns->single_bin) {
			ssd_bin->name[0] = 0;
		}
		else {
			strcpy(ssd_bin->name, as_bin_get_name_from_id(ns, bin->id));
		}

		ssd_bin->offset = buf - buf_start;

		uint32_t particle_flat_size = as_bin_particle_to_flat(bin, buf);

		buf += particle_flat_size;
		ssd_bin->len = particle_flat_size;
		ssd_bin->next = buf - buf_start;
	}

	block->sig = 0; // deprecated
	block->length = write_size - LENGTH_BASE;
	block->magic = SSD_BLOCK_MAGIC;
	block->keyd = r->keyd;
	block->generation = r->generation;
	block->void_time = r->void_time;
	block->bins_offset = rd->rec_props.p_data ? rd->rec_props.size : 0;
	block->n_bins = n_bins_written;
	block->last_update_time = r->last_update_time;

	r->file_id = ssd->file_id;
	r->rblock_id = BYTES_TO_RBLOCKS(WBLOCK_ID_TO_BYTES(ssd, swb->wblock_id) + swb_pos);
	r->n_rblocks = BYTES_TO_RBLOCKS(write_size);

	cf_atomic64_add(&ssd->inuse_size, (int64_t)write_size);
	cf_atomic32_add(&ssd->alloc_table->wblock_state[swb->wblock_id].inuse_sz, (int32_t)write_size);

	// We are finished writing to the buffer.
	cf_atomic32_decr(&swb->n_writers);

	if (ns->storage_benchmarks_enabled) {
		histogram_insert_raw(ns->device_write_size_hist, write_size);
	}

	return 0;
}


int
ssd_write(as_storage_rd *rd)
{
	as_record *r = rd->r;

	drv_ssd *old_ssd = NULL;
	uint64_t old_rblock_id = 0;
	uint16_t old_n_rblocks = 0;

	if (STORAGE_RBLOCK_IS_VALID(r->rblock_id)) {
		// Replacing an old record.
		old_ssd = rd->ssd;
		old_rblock_id = r->rblock_id;
		old_n_rblocks = r->n_rblocks;
	}

	drv_ssds *ssds = (drv_ssds*)rd->ns->storage_private;

	// Figure out which device to write to. When replacing an old record, it's
	// possible this is different from the old device (e.g. if we've added a
	// fresh device), so derive it from the digest each time.
	rd->ssd = &ssds->ssds[ssd_get_file_id(ssds, &r->keyd)];

	drv_ssd *ssd = rd->ssd;

	if (! ssd) {
		cf_warning(AS_DRV_SSD, "{%s} ssd_write: no drv_ssd for file_id %u",
				rd->ns->name, ssd_get_file_id(ssds, &r->keyd));
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	int rv = ssd_write_bins(rd);

	if (rv == 0 && old_ssd) {
		ssd_block_free(old_ssd, old_rblock_id, old_n_rblocks, "ssd-write");
	}

	return rv;
}


//==========================================================
// Storage statistics utilities.
//

void
as_storage_show_wblock_stats(as_namespace *ns)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return;
	}

	if (ns->storage_private) {
		drv_ssds *ssds = ns->storage_private;

		for (int d = 0; d < ssds->n_ssds; d++) {
			int num_free_blocks = 0;
			int num_full_blocks = 0;
			int num_full_swb = 0;
			int num_above_wm = 0;
			int num_defraggable = 0;

			drv_ssd *ssd = &ssds->ssds[d];
			ssd_alloc_table *at = ssd->alloc_table;
			uint32_t lwm_size = ns->defrag_lwm_size;

			for (uint32_t i = 0; i < at->n_wblocks; i++) {
				ssd_wblock_state *wblock_state = &at->wblock_state[i];
				uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

				if (inuse_sz == 0) {
					num_free_blocks++;
				}
				else if (inuse_sz == ssd->write_block_size) {
					if (wblock_state->swb) {
						num_full_swb++;
					}
					else {
						num_full_blocks++;
					}
				}
				else {
					if (inuse_sz > ssd->write_block_size || inuse_sz < lwm_size) {
						cf_info(AS_DRV_SSD, "dev %d, wblock %"PRIu32", inuse_sz %"PRIu32", %s swb",
								d, i, inuse_sz, wblock_state->swb ? "has" : "no");

						num_defraggable++;
					}
					else {
						num_above_wm++;
					}
				}
			}

			cf_info(AS_DRV_SSD, "device %s free %d full %d fullswb %d pfull %d defrag %d freeq %d",
				ssd->name, num_free_blocks, num_full_blocks, num_full_swb,
				num_above_wm, num_defraggable, cf_queue_sz(ssd->free_wblock_q));
		}
	}
	else {
		cf_info(AS_DRV_SSD, "no devices");
	}
}


void
as_storage_summarize_wblock_stats(as_namespace *ns)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return;
	}

	if (! ns->storage_private) {
		cf_info(AS_DRV_SSD, "no devices");
		return;
	}

	drv_ssds *ssds = ns->storage_private;
	uint32_t total_num_defraggable = 0;
	uint32_t total_num_above_wm = 0;
	uint64_t defraggable_sz = 0;
	uint64_t non_defraggable_sz = 0;

	// Note: This is a sparse array that could be more efficiently stored.
	// (In addition, ranges of block sizes could be binned together to
	// compress the histogram, rather than using one bin per block size.)
	uint32_t *wb_hist = cf_calloc(1, sizeof(uint32_t) * MAX_WRITE_BLOCK_SIZE);

	for (uint32_t d = 0; d < ssds->n_ssds; d++) {
		drv_ssd *ssd = &ssds->ssds[d];
		ssd_alloc_table *at = ssd->alloc_table;
		uint32_t num_free_blocks = 0;
		uint32_t num_full_swb = 0;
		uint32_t num_full_blocks = 0;
		uint32_t lwm_size = ns->defrag_lwm_size;
		uint32_t num_defraggable = 0;
		uint32_t num_above_wm = 0;

		for (uint32_t i = 0; i < at->n_wblocks; i++) {
			ssd_wblock_state *wblock_state = &at->wblock_state[i];
			uint32_t inuse_sz = cf_atomic32_get(wblock_state->inuse_sz);

			if (inuse_sz > ssd->write_block_size) {
				cf_warning(AS_DRV_SSD, "wblock size (%d > %d) too large ~~ not counting in histogram",
						inuse_sz, ssd->write_block_size);
			}
			else {
				wb_hist[inuse_sz]++;
			}

			if (inuse_sz == 0) {
				num_free_blocks++;
			}
			else if (inuse_sz == ssd->write_block_size) {
				if (wblock_state->swb) {
					num_full_swb++;
				}
				else {
					num_full_blocks++;
				}
			}
			else if (inuse_sz < lwm_size) {
				defraggable_sz += inuse_sz;
				num_defraggable++;
			}
			else {
				non_defraggable_sz += inuse_sz;
				num_above_wm++;
			}
		}

		total_num_defraggable += num_defraggable;
		total_num_above_wm += num_above_wm;

		cf_info(AS_DRV_SSD, "device %s free %u full %u fullswb %u pfull %u defrag %u freeq %u",
				ssd->name, num_free_blocks, num_full_blocks, num_full_swb,
				num_above_wm, num_defraggable, cf_queue_sz(ssd->free_wblock_q));
	}

	cf_info(AS_DRV_SSD, "WBH: Storage histogram for namespace \"%s\":",
			ns->name);
	cf_info(AS_DRV_SSD, "WBH: Average wblock size of: defraggable blocks: %lu bytes; nondefraggable blocks: %lu bytes; all blocks: %lu bytes",
			defraggable_sz / MAX(1, total_num_defraggable),
			non_defraggable_sz / MAX(1, total_num_above_wm),
			(defraggable_sz + non_defraggable_sz) /
					MAX(1, (total_num_defraggable + total_num_above_wm)));

	for (uint32_t i = 0; i < MAX_WRITE_BLOCK_SIZE; i++) {
		if (wb_hist[i] > 0) {
			cf_info(AS_DRV_SSD, "WBH: %u block%s of size %u bytes",
					wb_hist[i], (wb_hist[i] != 1 ? "s" : ""), i);
		}
	}

	cf_free(wb_hist);
}


// TODO - do something more useful with this info command.
int
as_storage_analyze_wblock(as_namespace* ns, int device_index,
		uint32_t wblock_id)
{
	if (AS_STORAGE_ENGINE_SSD != ns->storage_type) {
		cf_info(AS_DRV_SSD, "Storage engine type must be SSD (%d), not %d.",
				AS_STORAGE_ENGINE_SSD, ns->storage_type);
		return -1;
	}

	cf_info(AS_DRV_SSD, "analyze wblock: {%s}, device-index %d, wblock-id %u",
			ns->name, device_index, wblock_id);

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: no devices");
		return -1;
	}

	if (device_index < 0 || device_index >= ssds->n_ssds) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: bad device-index");
		return -1;
	}

	drv_ssd* ssd = &ssds->ssds[device_index];
	uint8_t* read_buf = cf_valloc(ssd->write_block_size);

	if (! read_buf) {
		cf_warning(AS_DRV_SSD, "analyze wblock ERROR: fail valloc");
		return -1;
	}

	int fd = ssd_fd_get(ssd);
	uint64_t file_offset = WBLOCK_ID_TO_BYTES(ssd, wblock_id);

	if (lseek(fd, (off_t)file_offset, SEEK_SET) != (off_t)file_offset) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: offset %lu: errno %d (%s)",
				ssd->name, file_offset, errno, cf_strerror(errno));
		cf_free(read_buf);
		close(fd);
		return -1;
	}

	ssize_t rlen = read(fd, read_buf, ssd->write_block_size);

	if (rlen != (ssize_t)ssd->write_block_size) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd->name, rlen, errno, cf_strerror(errno));
		cf_free(read_buf);
		close(fd);
		return -1;
	}

	ssd_fd_put(ssd, fd);

	uint32_t living_populations[AS_PARTITIONS];
	uint32_t zombie_populations[AS_PARTITIONS];

	memset(living_populations, 0, sizeof(living_populations));
	memset(zombie_populations, 0, sizeof(zombie_populations));

	uint32_t inuse_sz_start =
			cf_atomic32_get(ssd->alloc_table->wblock_state[wblock_id].inuse_sz);
	uint32_t offset = 0;

	while (offset < ssd->write_block_size) {
		drv_ssd_block* p_block = (drv_ssd_block*)&read_buf[offset];

		if (p_block->magic != SSD_BLOCK_MAGIC) {
			if (offset == 0) {
				// First block must have magic.
				cf_warning(AS_DRV_SSD, "analyze wblock ERROR: 1st block has no magic");
				cf_free(read_buf);
				return -1;
			}

			// Later blocks may have no magic, just skip to next block.
			offset += RBLOCK_SIZE;
			continue;
		}

		// Note - if block->length is sane, we don't need to round up to a
		// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
		uint32_t next_offset = offset +
				BYTES_TO_RBLOCK_BYTES(p_block->length + LENGTH_BASE);

		if (next_offset > ssd->write_block_size) {
			cf_warning(AS_DRV_SSD, "analyze wblock ERROR: record overflows wblock");
			cf_free(read_buf);
			return -1;
		}

		uint64_t rblock_id = BYTES_TO_RBLOCKS(file_offset + offset);
		uint32_t n_rblocks = (uint32_t)BYTES_TO_RBLOCKS(next_offset - offset);

		bool living = false;
		uint32_t pid = as_partition_getid(&p_block->keyd);
		as_partition_reservation rsv;

		as_partition_reserve_migrate(ns, pid, &rsv, 0);

		as_index_ref r_ref;
		r_ref.skip_lock = false;

		if (0 == as_record_get(rsv.tree, &p_block->keyd, &r_ref)) {
			as_index* r = r_ref.r;

			if (r->rblock_id == rblock_id && r->n_rblocks == n_rblocks) {
				living = true;
			}

			as_record_done(&r_ref, ns);
		}
		else if (ns->ldt_enabled &&
				0 == as_record_get(rsv.sub_tree, &p_block->keyd, &r_ref)) {
			as_index* r = r_ref.r;

			if (r->rblock_id == rblock_id && r->n_rblocks == n_rblocks) {
				living = true;
			}

			as_record_done(&r_ref, ns);
		}
		// else it was deleted (?) so call it a zombie...

		as_partition_release(&rsv);

		if (living) {
			living_populations[pid]++;
		}
		else {
			zombie_populations[pid]++;
		}

		offset = next_offset;
	}

	cf_free(read_buf);

	uint32_t inuse_sz_end =
			cf_atomic32_get(ssd->alloc_table->wblock_state[wblock_id].inuse_sz);

	cf_info(AS_DRV_SSD, "analyze wblock: inuse_sz %u (before) -> %u (after)",
			inuse_sz_start, inuse_sz_end);

	for (int i = 0; i < AS_PARTITIONS; i++) {
		if (living_populations[i] > 0 || zombie_populations[i] > 0) {
			cf_info(AS_DRV_SSD, "analyze wblock: pid %4d - live %u, dead %u",
					i, living_populations[i], zombie_populations[i]);
		}
	}

	return 0;
}


//==========================================================
// Per-device background jobs.
//

#define LOG_STATS_INTERVAL_sec 20

void
ssd_log_stats(drv_ssd *ssd, uint64_t *p_prev_n_total_writes,
		uint64_t *p_prev_n_defrag_reads, uint64_t *p_prev_n_defrag_writes,
		uint64_t *p_prev_n_tomb_raider_reads)
{
	uint64_t n_defrag_reads = cf_atomic64_get(ssd->n_defrag_wblock_reads);
	uint64_t n_defrag_writes = cf_atomic64_get(ssd->n_defrag_wblock_writes);
	uint64_t n_total_writes = cf_atomic64_get(ssd->n_wblock_writes) +
			n_defrag_writes;

	float total_write_rate = (float)(n_total_writes - *p_prev_n_total_writes) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_read_rate = (float)(n_defrag_reads - *p_prev_n_defrag_reads) /
			(float)LOG_STATS_INTERVAL_sec;
	float defrag_write_rate = (float)(n_defrag_writes - *p_prev_n_defrag_writes) /
			(float)LOG_STATS_INTERVAL_sec;

	uint64_t n_tomb_raider_reads = ssd->n_tomb_raider_reads;
	char tomb_raider_str[64];

	*tomb_raider_str = 0;

	if (n_tomb_raider_reads != 0) {
		if (*p_prev_n_tomb_raider_reads > n_tomb_raider_reads) {
			*p_prev_n_tomb_raider_reads = 0;
		}

		float tomb_raider_read_rate =
				(float)(n_tomb_raider_reads - *p_prev_n_tomb_raider_reads) /
				(float)LOG_STATS_INTERVAL_sec;

		sprintf(tomb_raider_str, " tomb-raider-read (%lu,%.1f)",
				n_tomb_raider_reads, tomb_raider_read_rate);
	}

	char shadow_str[64];

	*shadow_str = 0;

	if (ssd->shadow_name) {
		sprintf(shadow_str, " shadow-write-q %d",
				cf_queue_sz(ssd->swb_shadow_q));
	}

	cf_info(AS_DRV_SSD, "{%s} %s: used-bytes %lu free-wblocks %d write-q %d write (%lu,%.1f) defrag-q %d defrag-read (%lu,%.1f) defrag-write (%lu,%.1f)%s%s",
			ssd->ns->name, ssd->name,
			ssd->inuse_size, cf_queue_sz(ssd->free_wblock_q),
			cf_queue_sz(ssd->swb_write_q),
			n_total_writes, total_write_rate,
			cf_queue_sz(ssd->defrag_wblock_q), n_defrag_reads, defrag_read_rate,
			n_defrag_writes, defrag_write_rate,
			shadow_str, tomb_raider_str);

	*p_prev_n_total_writes = n_total_writes;
	*p_prev_n_defrag_reads = n_defrag_reads;
	*p_prev_n_defrag_writes = n_defrag_writes;
	*p_prev_n_tomb_raider_reads = n_tomb_raider_reads;

	if (cf_queue_sz(ssd->free_wblock_q) == 0) {
		cf_warning(AS_DRV_SSD, "device %s: out of storage space", ssd->name);
	}
}


void
ssd_free_swbs(drv_ssd *ssd)
{
	// Try to recover swbs, 16 at a time, down to 16.
	for (int i = 0; i < 16 && cf_queue_sz(ssd->swb_free_q) > 16; i++) {
		ssd_write_buf* swb;

		if (CF_QUEUE_OK !=
				cf_queue_pop(ssd->swb_free_q, &swb, CF_QUEUE_NOWAIT)) {
			break;
		}

		swb_destroy(swb);
	}
}


void
ssd_flush_current_swb(drv_ssd *ssd, uint64_t *p_prev_n_writes,
		uint32_t *p_prev_size)
{
	uint64_t n_writes = cf_atomic64_get(ssd->n_wblock_writes);

	// If there's an active write load, we don't need to flush.
	if (n_writes != *p_prev_n_writes) {
		*p_prev_n_writes = n_writes;
		*p_prev_size = 0;
		return;
	}

	pthread_mutex_lock(&ssd->write_lock);

	n_writes = cf_atomic64_get(ssd->n_wblock_writes);

	// Must check under the lock, could be racing a current swb just queued.
	if (n_writes != *p_prev_n_writes) {

		pthread_mutex_unlock(&ssd->write_lock);

		*p_prev_n_writes = n_writes;
		*p_prev_size = 0;
		return;
	}

	// Flush the current swb if it isn't empty, and has been written to since
	// last flushed.

	ssd_write_buf *swb = ssd->current_swb;

	if (swb && swb->pos != *p_prev_size) {
		*p_prev_size = swb->pos;

		// Clean the end of the buffer before flushing.
		if (ssd->write_block_size != swb->pos) {
			memset(&swb->buf[swb->pos], 0, ssd->write_block_size - swb->pos);
		}

		// Flush it.
		ssd_flush_swb(ssd, swb);
	}

	pthread_mutex_unlock(&ssd->write_lock);
}


void
ssd_flush_defrag_swb(drv_ssd *ssd, uint64_t *p_prev_n_writes,
		uint32_t *p_prev_size)
{
	uint64_t n_writes = cf_atomic64_get(ssd->n_defrag_wblock_writes);

	// If there's an active write load, we don't need to flush.
	if (n_writes != *p_prev_n_writes) {
		*p_prev_n_writes = n_writes;
		*p_prev_size = 0;
		return;
	}

	pthread_mutex_lock(&ssd->defrag_lock);

	n_writes = cf_atomic64_get(ssd->n_defrag_wblock_writes);

	// Must check under the lock, could be racing a defrag swb just queued.
	if (n_writes != *p_prev_n_writes) {

		pthread_mutex_unlock(&ssd->defrag_lock);

		*p_prev_n_writes = n_writes;
		*p_prev_size = 0;
		return;
	}

	// Flush the defrag swb if it isn't empty, and has been written to since
	// last flushed.

	ssd_write_buf *swb = ssd->defrag_swb;

	if (swb && swb->pos != *p_prev_size) {
		*p_prev_size = swb->pos;

		// Clean the end of the buffer before flushing.
		if (ssd->write_block_size != swb->pos) {
			memset(&swb->buf[swb->pos], 0, ssd->write_block_size - swb->pos);
		}

		// Flush it.
		ssd_flush_swb(ssd, swb);
	}

	pthread_mutex_unlock(&ssd->defrag_lock);
}


void
ssd_fsync(drv_ssd *ssd)
{
	int fd = ssd_fd_get(ssd);

	uint64_t start_ns = ssd->ns->storage_benchmarks_enabled ? cf_getns() : 0;

	fsync(fd);

	if (start_ns != 0) {
		histogram_insert_data_point(ssd->hist_fsync, start_ns);
	}

	ssd_fd_put(ssd, fd);
}


// Check all wblocks to load a device's defrag queue at runtime. Triggered only
// when defrag-lwm-pct is increased by manual intervention.
void
ssd_defrag_sweep(drv_ssd *ssd)
{
	ssd_alloc_table* at = ssd->alloc_table;
	uint32_t first_id = BYTES_TO_WBLOCK_ID(ssd, ssd->header_size);
	uint32_t last_id = at->n_wblocks;
	uint32_t n_queued = 0;

	for (uint32_t wblock_id = first_id; wblock_id < last_id; wblock_id++) {
		ssd_wblock_state *p_wblock_state = &at->wblock_state[wblock_id];

		pthread_mutex_lock(&p_wblock_state->LOCK);

		uint32_t inuse_sz = cf_atomic32_get(p_wblock_state->inuse_sz);

		if (! p_wblock_state->swb &&
				p_wblock_state->state != WBLOCK_STATE_DEFRAG &&
					inuse_sz != 0 &&
						inuse_sz < ssd->ns->defrag_lwm_size) {
			push_wblock_to_defrag_q(ssd, wblock_id);
			n_queued++;
		}

		pthread_mutex_unlock(&p_wblock_state->LOCK);
	}

	cf_info(AS_DRV_SSD, "... %s sweep queued %u wblocks for defrag", ssd->name,
			n_queued);
}


static inline uint64_t
next_time(uint64_t now, uint64_t job_interval, uint64_t next)
{
	uint64_t next_job = now + job_interval;

	return next_job < next ? next_job : next;
}


// All in microseconds since we're using usleep().
#define MAX_INTERVAL		(1000 * 1000)
#define LOG_STATS_INTERVAL	(1000 * 1000 * LOG_STATS_INTERVAL_sec)
#define FREE_SWBS_INTERVAL	(1000 * 1000 * 20)

// Thread "run" function to perform various background jobs per device.
void *
run_ssd_maintenance(void *udata)
{
	drv_ssd *ssd = (drv_ssd*)udata;
	as_namespace *ns = ssd->ns;

	uint64_t prev_n_total_writes = 0;
	uint64_t prev_n_defrag_reads = 0;
	uint64_t prev_n_defrag_writes = 0;
	uint64_t prev_n_tomb_raider_reads = 0;

	uint64_t prev_n_writes_flush = 0;
	uint32_t prev_size_flush = 0;
	uint64_t prev_n_writes_defrag_flush = 0;
	uint32_t prev_size_defrag_flush = 0;

	uint64_t now = cf_getus();
	uint64_t next = now + MAX_INTERVAL;

	uint64_t prev_log_stats = now;
	uint64_t prev_free_swbs = now;
	uint64_t prev_flush = now;
	uint64_t prev_defrag_flush = now;
	uint64_t prev_fsync = now;

	// If any job's (initial) interval is less than MAX_INTERVAL and we want it
	// done on its interval the first time through, add a next_time() call for
	// that job here to adjust 'next'. (No such jobs for now.)

	uint64_t sleep_us = next - now;

	while (true) {
		usleep((uint32_t)sleep_us);

		now = cf_getus();
		next = now + MAX_INTERVAL;

		if (now >= prev_log_stats + LOG_STATS_INTERVAL) {
			ssd_log_stats(ssd, &prev_n_total_writes, &prev_n_defrag_reads,
					&prev_n_defrag_writes, &prev_n_tomb_raider_reads);
			prev_log_stats = now;
			next = next_time(now, LOG_STATS_INTERVAL, next);
		}

		if (now >= prev_free_swbs + FREE_SWBS_INTERVAL) {
			ssd_free_swbs(ssd);
			prev_free_swbs = now;
			next = next_time(now, FREE_SWBS_INTERVAL, next);
		}

		uint64_t flush_max_us = ns->storage_flush_max_us;

		if (flush_max_us != 0 && now >= prev_flush + flush_max_us) {
			ssd_flush_current_swb(ssd, &prev_n_writes_flush, &prev_size_flush);
			prev_flush = now;
			next = next_time(now, flush_max_us, next);
		}

		if (flush_max_us != 0 && now >= prev_defrag_flush + flush_max_us) {
			ssd_flush_defrag_swb(ssd, &prev_n_writes_defrag_flush, &prev_size_defrag_flush);
			prev_defrag_flush = now;
			next = next_time(now, flush_max_us, next);
		}

		uint64_t fsync_max_us = ns->storage_fsync_max_us;

		if (fsync_max_us != 0 && now >= prev_fsync + fsync_max_us) {
			ssd_fsync(ssd);
			prev_fsync = now;
			next = next_time(now, fsync_max_us, next);
		}

		if (cf_atomic32_get(ssd->defrag_sweep) != 0) {
			// May take long enough to mess up other jobs' schedules, but it's a
			// very rare manually-triggered intervention.
			ssd_defrag_sweep(ssd);
			cf_atomic32_decr(&ssd->defrag_sweep);
		}

		now = cf_getus(); // refresh in case jobs took significant time
		sleep_us = next > now ? next - now : 1;
	}

	return NULL;
}


static void
ssd_start_maintenance_threads(drv_ssds *ssds)
{
	cf_info(AS_DRV_SSD, "{%s} starting device maintenance threads",
			ssds->ns->name);

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];

		pthread_create(&ssd->maintenance_thread, 0, run_ssd_maintenance, ssd);
	}
}


//==========================================================
// Device header utilities.
//

// -1 means unrecoverable error
// -2 means not formatted, please overwrite me
int
as_storage_read_header(drv_ssd *ssd, as_namespace *ns,
		ssd_device_header **header_r)
{
	*header_r = 0;

	int rv = -1;

	bool use_shadow = ns->cold_start && ssd->shadow_name;
	const char *ssd_name = use_shadow ? ssd->shadow_name : ssd->name;
	int fd = use_shadow ? ssd_shadow_fd_get(ssd) : ssd_fd_get(ssd);

	size_t peek_size = BYTES_UP_TO_IO_MIN(ssd, sizeof(ssd_device_header));
	ssd_device_header *header = cf_valloc(peek_size);

	if (! header) {
		goto Fail;
	}

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: errno %d (%s)", ssd_name,
				errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	ssize_t sz = read(fd, (void*)header, peek_size);

	if (sz != (ssize_t)peek_size) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd_name, sz, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	// Make sure all following checks that return -1 or -2 are also done in
	// peek_devices() in the enterprise repo.

	if (header->magic != SSD_HEADER_MAGIC) { // normal path for a fresh drive
		cf_detail(AS_DRV_SSD, "read_header: device %s no magic, not a Citrusleaf drive",
				ssd_name);
		rv = -2;
		goto Fail;
	}

	if (header->version != SSD_VERSION) {
		if (can_convert_storage_version(header->version)) {
			cf_info(AS_DRV_SSD, "read_header: device %s converting storage version %u to %u",
					ssd_name, header->version, SSD_VERSION);
		}
		else {
			cf_warning(AS_DRV_SSD, "read_header: device %s has unconvertible storage version %u",
					ssd_name, header->version);
			goto Fail;
		}
	}

	if (header->write_block_size != 0 &&
			ns->storage_write_block_size % header->write_block_size != 0) {
		cf_warning(AS_DRV_SSD, "read header: device %s can't change write-block-size from %u to %u",
				ssd_name, header->write_block_size,
				ns->storage_write_block_size);
		goto Fail;
	}

	if (header->devices_n > 100) {
		cf_warning(AS_DRV_SSD, "read header: device %s don't support %u devices, corrupt read",
				ssd_name, header->devices_n);
		goto Fail;
	}

	if (strcmp(header->namespace, ns->name) != 0) {
		cf_warning(AS_DRV_SSD, "read header: device %s previous namespace %s now %s, check config or erase device",
				ssd_name, header->namespace, ns->name);
		goto Fail;
	}

	size_t h_len = header->header_length;

	cf_free(header);

	header = cf_valloc(h_len);

	if (! header) {
		goto Fail;
	}

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: errno %d (%s)", ssd_name,
				errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	sz = read(fd, (void*)header, h_len);

	if (sz != (ssize_t)header->header_length) {
		cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
				ssd_name, sz, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Fail;
	}

	cf_detail(AS_DRV_SSD, "device %s: header read success: version %d devices %d random %"PRIu64,
			ssd_name, header->version, header->devices_n, header->random);

	// In case we're bumping the version - ensure the new version gets written.
	header->version = SSD_VERSION;

	// In case we're increasing write-block-size - ensure new value is recorded.
	header->write_block_size = ns->storage_write_block_size;

	*header_r = header;

	use_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);

	return 0;

Fail:

	if (header) {
		cf_free(header);
	}

	if (fd != -1) {
		use_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);
	}

	return rv;
}


ssd_device_header *
as_storage_init_header(as_namespace *ns)
{
	ssd_device_header *h = cf_valloc(SSD_DEFAULT_HEADER_LENGTH);

	if (! h) {
		return 0;
	}

	memset(h, 0, SSD_DEFAULT_HEADER_LENGTH);

	h->magic = SSD_HEADER_MAGIC;
	h->random = 0;
	h->write_block_size = ns->storage_write_block_size;
	h->last_evict_void_time = 0;
	h->version = SSD_VERSION;
	h->devices_n = 0;
	h->header_length = SSD_DEFAULT_HEADER_LENGTH;
	memset(h->namespace, 0, sizeof(h->namespace));
	strcpy(h->namespace, ns->name);
	h->info_n = AS_PARTITIONS;
	h->info_stride = SSD_HEADER_INFO_STRIDE;

	return h;
}


bool
as_storage_empty_header(int fd, const char* device_name)
{
	void *h = cf_valloc(SSD_DEFAULT_HEADER_LENGTH);

	if (! h) {
		cf_warning(AS_DRV_SSD, "device %s: empty header: valloc failed",
				device_name);
		return false;
	}

	memset(h, 0, SSD_DEFAULT_HEADER_LENGTH);

	if (0 != lseek(fd, 0, SEEK_SET)) {
		cf_warning(AS_DRV_SSD, "device %s: empty header: seek error: %s",
				device_name, cf_strerror(errno));
		cf_free(h);
		return false;
	}

	if (SSD_DEFAULT_HEADER_LENGTH != write(fd, h, SSD_DEFAULT_HEADER_LENGTH)) {
		cf_warning(AS_DRV_SSD, "device %s: empty header: write error: %s",
				device_name, cf_strerror(errno));
		cf_free(h);
		return false;
	}

	cf_free(h);
	fsync(fd);

	return true;
}


void
as_storage_write_header(drv_ssd *ssd, ssd_device_header *header, size_t size)
{
	int fd = ssd_fd_get(ssd);

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	ssize_t sz = write(fd, (void*)header, size);

	if (sz != (ssize_t)size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->name, errno, cf_strerror(errno));
	}

	ssd_fd_put(ssd, fd);

	if (! ssd->shadow_name) {
		return;
	}

	fd = ssd_shadow_fd_get(ssd);

	if (lseek(fd, 0, SEEK_SET) != 0) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	sz = write(fd, (void*)header, size);

	if (sz != (ssize_t)size) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
				ssd->shadow_name, errno, cf_strerror(errno));
	}

	ssd_shadow_fd_put(ssd, fd);
}


//==========================================================
// Cold start utilities.
//

bool
prefer_existing_record(drv_ssd* ssd, uint32_t wblock_id, drv_ssd_block* block,
		as_index* r)
{
	// If the existing record points to the wblock we're reading, the existing
	// record must be older.
	if (wblock_id == RBLOCK_ID_TO_WBLOCK_ID(ssd, r->rblock_id)
			&& ssd->file_id == r->file_id) {
		return false;
	}

	// If both last-update-times are set and are different, later is newer.
	if (block->last_update_time != 0 && r->last_update_time != 0 &&
			block->last_update_time != r->last_update_time) {
		return block->last_update_time < r->last_update_time;
	}

	// If generations are different, bigger (considering wrap-around) is newer.
	if (block->generation != r->generation) {
		return as_gen_less_than(block->generation, r->generation);
	}

	// Finally, compare void-times. Note that defragged records will generate
	// identical copies on drive, so they'll get here and return true.
	return r->void_time == 0 ||
			(block->void_time != 0 && block->void_time <= r->void_time);
}


bool
is_set_evictable(as_namespace* ns, const as_rec_props* p_props)
{
	if (p_props->size == 0) {
		return true;
	}

	const char* set_name;

	if (as_rec_props_get_value(p_props, CL_REC_PROPS_FIELD_SET_NAME, NULL,
			(uint8_t**)&set_name) != 0) {
		return true;
	}

	as_set *p_set;

	if (cf_vmapx_get_by_name(ns->p_sets_vmap, set_name, (void**)&p_set) !=
			CF_VMAPX_OK) {
		return true;
	}

	return ! IS_SET_EVICTION_DISABLED(p_set);
}


bool
is_record_expired(as_namespace* ns, const drv_ssd_block* block,
		const as_rec_props* p_props)
{
	if (block->void_time == 0 ||
			block->void_time > ns->cold_start_threshold_void_time) {
		return false;
	}

	// If set is not evictable, may have expired but wasn't evicted.
	return block->void_time < as_record_void_time_get() ||
			is_set_evictable(ns, p_props);
}


// Add a record just read from drive to the index, if all is well.
// Return values:
//  0 - success, record added or updated
// -1 - skipped or deleted this record for a "normal" reason
// -2 - serious limit encountered, caller won't continue
// -3 - couldn't parse this record, but caller will continue
int
ssd_record_add(drv_ssds* ssds, drv_ssd* ssd, drv_ssd_block* block,
		uint64_t rblock_id, uint32_t n_rblocks)
{
	uint32_t pid = as_partition_getid(&block->keyd);

	// If this isn't a partition we're interested in, skip this record.
	if (! ssds->get_state_from_storage[pid]) {
		return -1;
	}

	as_namespace* ns = ssds->ns;

	// If eviction is necessary, evict previously added records closest to
	// expiration. (If evicting, this call will block for a long time.) This
	// call may also update the cold-start threshold void-time.
	if (! as_cold_start_evict_if_needed(ns)) {
		return -2;
	}

	// Sanity-check the record.
	if (! is_valid_record(block, ns->name)) {
		return -3;
	}

	// Don't bother with reservations - partition trees aren't going anywhere.
	as_partition* p_partition = &ns->partitions[pid];

	// Get or create the record.
	as_index_ref r_ref;
	r_ref.skip_lock = false;

	// Read LDT rec-prop.
	as_rec_props props;

	props.p_data = block->data;
	props.size = block->bins_offset;

	bool is_ldt_sub;
	bool is_ldt_parent;

	as_ldt_get_property(&props, &is_ldt_parent, &is_ldt_sub);

	if (ssd->sub_sweep) {
		if (! is_ldt_sub) {
			return -1;
		}
	}
	else {
		if (is_ldt_sub || (is_ldt_parent && ! ns->ldt_enabled)) {
			return -1;
		}
	}

	if (ssd_cold_start_is_record_truncated(ns, block, &props)) {
		return -1;
	}

	// Get/create the record from/in the appropriate index tree.
	int rv;
	int retries = 0;

	while (true) {
		if ((rv = as_record_get_create(
				is_ldt_sub ? p_partition->sub_vp : p_partition->vp,
						&block->keyd, &r_ref, ns, is_ldt_sub)) != -1) {
			break;
		}

		// rv = -1 - race can occur if versions of this record are on more than
		// one drive - wait for other thread to finish create.
		usleep(50);

		if (++retries % 20000 == 0) {
			cf_warning_digest(AS_DRV_SSD, &block->keyd, "record-add as_record_get_create() stuck ");
		}
	}

	if (rv < 0) {
		cf_warning_digest(AS_DRV_SSD, &block->keyd, "record-add as_record_get_create() failed ");
		return -1;
	}

	bool is_create = rv == 1;

	// Fix 0 generations coming off device.
	if (block->generation == 0) {
		block->generation = 1;
		cf_warning_digest(AS_DRV_SSD, &block->keyd, "record-add found generation 0 - changed to 1 ");
	}

	as_index* r = r_ref.r;
	uint32_t wblock_id = RBLOCK_ID_TO_WBLOCK_ID(ssd, rblock_id);
	// TODO - pass in wblock_id when we do boundary check in sweep.

	if (! is_create) {
		// Record already existed. Ignore this one if existing record is newer.
		if (prefer_existing_record(ssd, wblock_id, block, r)) {
			ssd_cold_start_adjust_cenotaph(ns, block, r);
			as_record_done(&r_ref, ns);
			ssd->record_add_older_counter++;
			return -1;
		}
	}
	// The record we're now reading is the latest version (so far) ...

	// Skip records that have expired. (LDT subrecords are expired via their
	// their parent record.)
	if (! is_ldt_sub && is_record_expired(ns, block, &props)) {
		as_index_delete(p_partition->vp, &block->keyd);
		as_record_done(&r_ref, ns);
		ssd->record_add_expired_counter++;
		return -1;
	}

	// We'll keep the record we're now reading ...

	// Set/reset the record's last-update-time and generation.
	r->last_update_time = block->last_update_time;
	r->generation = block->generation;

	// Set/reset the record's void-time, truncating it if beyond max-ttl.
	if (! is_ldt_sub && block->void_time > ns->cold_start_max_void_time) {
		cf_detail(AS_DRV_SSD, "record-add truncating void-time %lu > max %u",
				block->void_time, ns->cold_start_max_void_time);

		r->void_time = ns->cold_start_max_void_time;
		ssd->record_add_max_ttl_counter++;
	}
	else {
		r->void_time = block->void_time;
	}

	// Update maximum void-time.
	cf_atomic64_setmax(&p_partition->max_void_time, r->void_time);

	// If data is in memory, load bins and particles, adjust secondary index.
	if (ns->storage_data_in_memory) {
		uint8_t* block_head = (uint8_t*)block;
		drv_ssd_bin* ssd_bin = (drv_ssd_bin*)(block->data + block->bins_offset);
		as_storage_rd rd;

		if (is_create) {
			as_storage_record_create(ns, r, &rd);
		}
		else {
			as_storage_record_open(ns, r, &rd);
		}

		as_storage_rd_load_n_bins(&rd);
		as_storage_rd_load_bins(&rd, NULL);

		uint64_t bytes_memory = as_storage_record_get_n_bytes_memory(&rd);

		// Do this early since set-id is needed for the secondary index update.
		if (props.size != 0) {
			as_record_apply_properties(r, ns, &props);
		}
		else {
			as_record_clear_properties(r, ns);
		}

		uint16_t old_n_bins = rd.n_bins;

		bool has_sindex = record_has_sindex(r, ns);
		int sbins_populated = 0;

		if (has_sindex) {
			SINDEX_GRLOCK();
		}

		SINDEX_BINS_SETUP(sbins, 2 * ns->sindex_cnt);
		as_sindex* si_arr[2 * ns->sindex_cnt];
		int si_arr_index = 0;
		const char* set_name = as_index_get_set_name(r, ns);

		if (has_sindex) {
			for (uint16_t i = 0; i < old_n_bins; i++) {
				si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, rd.bins[i].id, &si_arr[si_arr_index]);
			}
		}

		int32_t delta_bins = (int32_t)block->n_bins - (int32_t)old_n_bins;

		if (rd.ns->single_bin) {
			if (delta_bins < 0) {
				as_record_clean_bins(&rd);
			}
		}
		else if (delta_bins != 0) {
			if (has_sindex && delta_bins < 0) {
				sbins_populated += as_sindex_sbins_from_rd(&rd, (uint16_t)block->n_bins, old_n_bins, sbins, AS_SINDEX_OP_DELETE);
			}

			as_bin_allocate_bin_space(r, &rd, delta_bins);
		}

		for (uint16_t i = 0; i < block->n_bins; i++) {
			as_bin* b;

			if (i < old_n_bins) {
				b = &rd.bins[i];

				if (has_sindex) {
					sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_DELETE);
				}

				as_bin_set_id_from_name(ns, b, ssd_bin->name);
			}
			else {
				// TODO - what if this fails?
				b = as_bin_create(&rd, ssd_bin->name);
			}

			// TODO - what if this fails?
			as_bin_particle_replace_from_flat(b, block_head + ssd_bin->offset,
					ssd_bin->len);

			ssd_bin = (drv_ssd_bin*)(block_head + ssd_bin->next);

			if (has_sindex) {
				si_arr_index += as_sindex_arr_lookup_by_set_binid_lockfree(ns, set_name, b->id, &si_arr[si_arr_index]);
				sbins_populated += as_sindex_sbins_from_bin(ns, set_name, b, &sbins[sbins_populated], AS_SINDEX_OP_INSERT);
			}
		}

		if (has_sindex) {
			SINDEX_GRUNLOCK();

			if (sbins_populated > 0) {
				as_sindex_update_by_sbin(ns, as_index_get_set_name(r, ns), sbins, sbins_populated, &r->keyd);
				as_sindex_sbin_freeall(sbins, sbins_populated);
			}

			as_sindex_release_arr(si_arr, si_arr_index);
		}

		as_storage_record_adjust_mem_stats(&rd, bytes_memory);
		as_storage_record_close(&rd);
	}
	else {
		if (props.size != 0) {
			as_record_apply_properties(r, ns, &props);
		}
		else {
			as_record_clear_properties(r, ns);
		}
	}

	if (is_create) {
		ssd->record_add_unique_counter++;
	}
	else if (STORAGE_RBLOCK_IS_VALID(r->rblock_id)) {
		// Replacing an existing record, undo its previous storage accounting.
		ssd_block_free(&ssds->ssds[r->file_id], r->rblock_id, r->n_rblocks,
				"record-add");
		ssd->record_add_replace_counter++;
	}
	else {
		cf_warning(AS_DRV_SSD, "replacing record with invalid rblock-id");
	}

	ssd_cold_start_transition_record(ns, block, r, is_create);

	// Update storage accounting to include this record.
	// TODO - pass in size instead of n_rblocks.
	uint32_t size = (uint32_t)RBLOCKS_TO_BYTES(n_rblocks);

	ssd->inuse_size += size;
	ssd->alloc_table->wblock_state[wblock_id].inuse_sz += size;

	// Set/reset the record's storage information.
	r->file_id = ssd->file_id;
	r->rblock_id = rblock_id;
	r->n_rblocks = n_rblocks;

	// Make sure subrecord sweep happens.
	if (is_ldt_parent) {
		ssd->has_ldt = true;
	}

	as_record_done(&r_ref, ns);

	return 0;
}


// Sweep through storage devices and rebuild the index.
//
// If there are LDT records the sweep is done twice, once for LDT parent records
// and then again for LDT subrecords.
int
ssd_load_device_sweep(drv_ssds *ssds, drv_ssd *ssd)
{
	uint8_t *buf = cf_valloc(LOAD_BUF_SIZE);

	bool read_shadow = ssd->shadow_name && ! ssd->sub_sweep;
	char *read_ssd_name = read_shadow ? ssd->shadow_name : ssd->name;
	int fd = read_shadow ? ssd_shadow_fd_get(ssd) : ssd_fd_get(ssd);
	int write_fd = read_shadow ? ssd_fd_get(ssd) : -1;

	// Seek past the header.
	off_t file_offset = ssds->header->header_length;

	if (lseek(fd, file_offset, SEEK_SET) != file_offset) {
		cf_warning(AS_DRV_SSD, "%s: seek failed: offset %ld: errno %d (%s)",
				read_ssd_name, file_offset, errno, cf_strerror(errno));
		close(fd);
		fd = -1;
		goto Finished;
	}

	if (read_shadow && lseek(write_fd, file_offset, SEEK_SET) != file_offset) {
		cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED seek: offset %ld: errno %d (%s)",
				ssd->name, file_offset, errno, cf_strerror(errno));
	}

	int error_count = 0;

	ssd->cold_start_block_counter = file_offset / LOAD_BUF_SIZE;

	// Loop over all blocks in device.
	while (true) {
		ssize_t rlen = read(fd, buf, LOAD_BUF_SIZE);

		if (rlen != LOAD_BUF_SIZE) {
			cf_warning(AS_DRV_SSD, "%s: read failed (%ld): errno %d (%s)",
					read_ssd_name, rlen, errno, cf_strerror(errno));
			close(fd);
			fd = -1;
			goto Finished;
		}

		if (read_shadow) {
			// TODO - ok to always write 1Mb blocks?
			ssize_t sz = write(write_fd, (void*)buf, LOAD_BUF_SIZE);

			if (sz != LOAD_BUF_SIZE) {
				cf_crash(AS_DRV_SSD, "%s: DEVICE FAILED write: errno %d (%s)",
						ssd->name, errno, cf_strerror(errno));
			}
		}

		size_t block_offset = 0; // current offset within the 1M block, in bytes

		while (block_offset < LOAD_BUF_SIZE) {
			drv_ssd_block *block = (drv_ssd_block*)&buf[block_offset];

			// Look for record magic.
			if (block->magic != SSD_BLOCK_MAGIC) {
				// No record found here.
				// (Includes normal case of nothing ever written here).

				block_offset += RBLOCK_SIZE;

				// We always write some at the start of a 1M block.
				if (block_offset == RBLOCK_SIZE) {
					error_count++;
					goto NextBlock;
				}
				else {
					// Otherwise check the next rblock, looking for magic.
					continue;
				}
			}

			// Note - if block->length is sane, we don't need to round up to a
			// multiple of RBLOCK_SIZE, but let's do it anyway just to be safe.
			size_t next_block_offset = block_offset +
					BYTES_TO_RBLOCK_BYTES(block->length + LENGTH_BASE);

			// Sanity-check for 1M block overruns.
			// TODO - check write_block_size boundaries!
			if (next_block_offset > LOAD_BUF_SIZE) {
				cf_warning(AS_DRV_SSD, "error: block extends over read size: foff %"PRIu64" boff %"PRIu64" blen %"PRIu64,
					file_offset, block_offset, (uint64_t)block->length);

				error_count++;
				goto NextBlock;
			}

			// Found a record - try to add it to the index.
			int add_rv = ssd_record_add(ssds, ssd, block,
					BYTES_TO_RBLOCKS(file_offset + block_offset),
					(uint32_t)BYTES_TO_RBLOCKS(next_block_offset - block_offset));

			if (add_rv == -2) {
				cf_crash(AS_DRV_SSD, "hit stop-writes limit before drive scan completed");
			}

			if (add_rv == -3) {
				error_count++;
				goto NextBlock;
			}

			error_count = 0;
			block_offset = next_block_offset;
		}

NextBlock:

		// If we encounter enough 1M blocks that have no records, assume we've
		// read all our data and we're done.
		if (error_count > 10) {
			goto Finished;
		}

		file_offset += LOAD_BUF_SIZE;
		ssd->cold_start_block_counter++;
	}

Finished:

	ssd->cold_start_block_counter = ssd->file_size / LOAD_BUF_SIZE;

	if (fd != -1) {
		read_shadow ? ssd_shadow_fd_put(ssd, fd) : ssd_fd_put(ssd, fd);
	}

	if (write_fd != -1) {
		ssd_fd_put(ssd, write_fd);
	}

	cf_free(buf);

	return 0;
}


typedef struct {
	drv_ssds *ssds;
	drv_ssd *ssd;
	cf_queue *complete_q;
	void *complete_udata;
	void *complete_rc;
} ssd_load_devices_data;

// Thread "run" function to read a device and rebuild the index.
void *
ssd_load_devices_fn(void *udata)
{
	ssd_load_devices_data *ldd = (ssd_load_devices_data*)udata;
	drv_ssd *ssd = ldd->ssd;
	drv_ssds *ssds = ldd->ssds;
	cf_queue *complete_q = ldd->complete_q;
	void *complete_udata = ldd->complete_udata;
	void *complete_rc = ldd->complete_rc;

	as_namespace* ns = ssds->ns;

	cf_free(ldd);
	ldd = 0;

	cf_info(AS_DRV_SSD, "device %s: reading device to load index", ssd->name);

	CF_ALLOC_SET_NS_ARENA(ns);

	ssd->sub_sweep	= false;
	ssd->has_ldt	= false;

	ssd_load_device_sweep(ssds, ssd);

	if (ns->ldt_enabled && ssd->has_ldt) {
		cf_info(AS_DRV_SSD, "device %s: reading device again to load subrecords",
				ssd->name);
		ssd->sub_sweep = true;
		ssd_load_device_sweep(ssds, ssd);
	}

	cf_info(AS_DRV_SSD, "device %s: read complete: UNIQUE %"PRIu64" (REPLACED %"PRIu64") (OLDER %"PRIu64") (EXPIRED %"PRIu64") (MAX-TTL %"PRIu64") records",
		ssd->name, ssd->record_add_unique_counter,
		ssd->record_add_replace_counter, ssd->record_add_older_counter,
		ssd->record_add_expired_counter, ssd->record_add_max_ttl_counter);

	if (ssd->record_add_sigfail_counter) {
		cf_warning(AS_DRV_SSD, "device %s: WARNING: %"PRIu64" elements could not be read due to signature failure. Possible hardware errors.",
			ssd->name, ssd->record_add_sigfail_counter);
	}

	if (0 == cf_rc_release(complete_rc)) {
		// All drives are done reading.

		ns->cold_start_loading = false;
		ssd_cold_start_drop_cenotaphs(ns);
		ssd_load_wblock_queues(ssds);

		pthread_mutex_destroy(&ns->cold_start_evict_lock);

		cf_queue_push(complete_q, &complete_udata);
		cf_rc_free(complete_rc);

		as_truncate_list_cenotaphs(ns);
		as_truncate_done_startup(ns); // set truncate last-update-times in sets' vmap

		ssd_start_maintenance_threads(ssds);
		ssd_start_write_worker_threads(ssds);
		ssd_start_defrag_threads(ssds);
	}

	return 0;
}


void
ssd_load_devices_load(drv_ssds *ssds, cf_queue *complete_q, void *udata)
{
	ssds->ns->cold_start_loading = true;

	void *p = cf_rc_alloc(1);

	for (int i = 1; i < ssds->n_ssds; i++) {
		cf_rc_reserve(p);
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		ssd_load_devices_data *ldd = cf_malloc(sizeof(ssd_load_devices_data));

		if (! ldd) {
			cf_crash(AS_DRV_SSD, "memory allocation in device load");
		}

		ldd->ssds = ssds;
		ldd->ssd = ssd;
		ldd->complete_q = complete_q;
		ldd->complete_udata = udata;
		ldd->complete_rc = p;

		pthread_create(&ssd->load_device_thread, 0, ssd_load_devices_fn, ldd);
	}
}


void
ssd_load_devices_init_header_length(drv_ssds *ssds)
{
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->header_size = ssds->header->header_length;
	}
}


//==========================================================
// Generic startup utilities.
//

static int
first_used_device(ssd_device_header *headers[], int n_ssds)
{
	for (int i = 0; i < n_ssds; i++) {
		if (headers[i]->random != 0) {
			return i;
		}
	}

	return -1;
}


bool
ssd_load_devices(drv_ssds *ssds, cf_queue *complete_q, void *udata)
{
	uint64_t random = cf_get_rand64();

	int n_ssds = ssds->n_ssds;
	as_namespace *ns = ssds->ns;

	ssd_device_header *headers[n_ssds];

	// Check all the headers. Pick one as the representative.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		int rvh = as_storage_read_header(ssd, ns, &headers[i]);

		if (rvh == -1) {
			cf_crash(AS_DRV_SSD, "unable to read disk header %s: %s",
					ssd->name, cf_strerror(errno));
		}

		if (rvh == -2) {
			headers[i] = as_storage_init_header(ns);
		}
	}

	int first_used = first_used_device(headers, n_ssds);

	if (first_used == -1) {
		// Shouldn't find all fresh headers here during warm restart.
		if (! ns->cold_start) {
			// There's no going back to cold start now - do so the harsh way.
			cf_crash(AS_DRV_SSD, "{%s}: found all %d devices fresh during warm restart",
					ns->name, n_ssds);
		}

		cf_info(AS_DRV_SSD, "namespace %s: found all %d devices fresh, initializing to random %"PRIu64,
				ns->name, n_ssds, random);

		ssds->header = headers[0];

		for (int i = 1; i < n_ssds; i++) {
			cf_free(headers[i]);
		}

		ssds->header->random = random;
		ssds->header->devices_n = n_ssds;
		as_storage_info_flush_ssd(ns);
		ssd_load_devices_init_header_length(ssds);

		as_truncate_list_cenotaphs(ns); // all will show as cenotaph
		as_truncate_done_startup(ns);

		return true;
	}

	// At least one device is not fresh. Check that all non-fresh devices match.

	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		// Skip fresh devices.
		if (headers[i]->random == 0) {
			ssd->started_fresh = true; // warm restart needs to know
			continue;
		}

		if (headers[first_used]->random != headers[i]->random) {
			cf_crash(AS_DRV_SSD, "namespace %s: drive set with unmatched headers - devices %s & %s have different signatures",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (headers[first_used]->devices_n != headers[i]->devices_n) {
			cf_crash(AS_DRV_SSD, "namespace %s: drive set with unmatched headers - devices %s & %s have different device counts",
					ns->name, ssds->ssds[first_used].name, ssd->name);
		}

		if (headers[first_used]->last_evict_void_time !=
				headers[i]->last_evict_void_time) {
			cf_warning(AS_DRV_SSD, "namespace %s: devices have inconsistent evict-void-times - ignoring",
					ns->name);
			headers[first_used]->last_evict_void_time = 0;
		}
	}

	// Drive set OK - fix up header set.
	ssds->header = headers[first_used];
	headers[first_used] = 0;

	for (int i = 0; i < n_ssds; i++) {
		if (headers[i]) {
			cf_free(headers[i]);
			headers[i] = 0;
		}
	}

	ssds->header->random = random;
	ssds->header->devices_n = n_ssds; // may have added fresh drives
	as_storage_info_flush_ssd(ns);
	ssd_load_devices_init_header_length(ssds);

	// Cache booleans indicating whether partitions are owned or not.
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_version version;

		as_storage_info_get(ns, pid, &version);
		ssds->get_state_from_storage[pid] =
				! as_partition_version_is_null(&version);
	}

	// Warm restart - imitate device loading by reducing resumed index.
	if (! ns->cold_start) {
		as_truncate_done_startup(ns); // set truncate last-update-times in sets' vmap
		ssd_resume_devices(ssds);

		return true;
	}

	// Initialize the cold-start eviction machinery.

	if (0 != pthread_mutex_init(&ns->cold_start_evict_lock, NULL)) {
		cf_crash(AS_DRV_SSD, "failed cold-start eviction mutex init");
	}

	uint32_t now = as_record_void_time_get();

	if (ns->cold_start_evict_ttl == 0xFFFFffff) {
		// Config file did NOT specify cold-start-evict-ttl.
		ns->cold_start_threshold_void_time = ssds->header->last_evict_void_time;

		// Check that it's not already in the past. (Note - includes 0.)
		if (ns->cold_start_threshold_void_time < now) {
			ns->cold_start_threshold_void_time = now;
		}
		else {
			cf_info(AS_DRV_SSD, "namespace %s: using saved cold start evict-ttl %u",
					ns->name, ns->cold_start_threshold_void_time - now);
		}
	}
	else {
		// Config file specified cold-start-evict-ttl. (0 is a valid value.)
		ns->cold_start_threshold_void_time = now + ns->cold_start_evict_ttl;

		cf_info(AS_DRV_SSD, "namespace %s: using config-specified cold start evict-ttl %u",
				ns->name, ns->cold_start_evict_ttl);
	}

	ns->cold_start_max_void_time = now + (uint32_t)ns->max_ttl;

	// Fire off threads to load in data - will signal completion when threads
	// are all done.
	ssd_load_devices_load(ssds, complete_q, udata);

	// Make sure caller doesn't signal completion.
	return false;
}


// Set a device's system block scheduler mode.
static int
ssd_set_scheduler_mode(const char* device_name, const char* mode)
{
	if (strncmp(device_name, "/dev/", 5)) {
		cf_warning(AS_DRV_SSD, "storage: invalid device name %s, did not set scheduler mode",
				device_name);
		return -1;
	}

	char device_tag[(strlen(device_name) - 5) + 1];

	strcpy(device_tag, device_name + 5);

	// Replace any slashes in the device tag with '!' - this is the naming
	// convention in /sys/block.
	char* p_char = device_tag;

	while (*p_char) {
		if (*p_char == '/') {
			*p_char = '!';
		}

		p_char++;
	}

	char scheduler_file_name[17 + strlen(device_tag) + 3 + 16 + 1];

	strcpy(scheduler_file_name, "/sys/class/block/");
	strcat(scheduler_file_name, device_tag);

	// Determine if this device is a partition.
	char partition_file_name[strlen(scheduler_file_name) + 10 + 1];

	strcpy(partition_file_name, scheduler_file_name);
	strcat(partition_file_name, "/partition");

	FILE* partition_file = fopen(partition_file_name, "r");

	if (partition_file) {
		fclose(partition_file);

		// This device is a partition, get parent device.
		strcat(scheduler_file_name, "/..");
	}

	strcat(scheduler_file_name, "/queue/scheduler");

	FILE* scheduler_file = fopen(scheduler_file_name, "w");

	if (! scheduler_file) {
		cf_warning(AS_DRV_SSD, "storage: couldn't open %s, did not set scheduler mode: %s",
				scheduler_file_name, cf_strerror(errno));
		return -1;
	}

	if (fwrite(mode, strlen(mode), 1, scheduler_file) != 1) {
		fclose(scheduler_file);

		cf_warning(AS_DRV_SSD, "storage: couldn't write %s to %s, did not set scheduler mode",
				mode, scheduler_file_name);
		return -1;
	}

	fclose(scheduler_file);

	cf_info(AS_DRV_SSD, "storage: set device %s scheduler mode to %s",
			device_name, mode);

	return 0;
}


static void
check_write_block_size(uint32_t write_block_size)
{
	if (write_block_size > MAX_WRITE_BLOCK_SIZE) {
		cf_crash(AS_DRV_SSD, "attempted to configure write block size in excess of %u",
				MAX_WRITE_BLOCK_SIZE);
	}

	if (LOAD_BUF_SIZE % write_block_size != 0 ||
			SSD_DEFAULT_HEADER_LENGTH % write_block_size != 0) {
		cf_crash(AS_DRV_SSD, "attempted to configure non-round write block size %u",
				write_block_size);
	}
}


static off_t
check_file_size(off_t file_size, const char *tag)
{
	if (sizeof(off_t) <= 4) {
		cf_warning(AS_DRV_SSD, "this OS supports only 32-bit (4g) files - compile with 64 bit offsets");
	}

	if (file_size > SSD_DEFAULT_HEADER_LENGTH) {
		off_t unusable_size =
				(file_size - SSD_DEFAULT_HEADER_LENGTH) % LOAD_BUF_SIZE;

		if (unusable_size != 0) {
			cf_info(AS_DRV_SSD, "%s size must be header size %u + multiple of %u, rounding down",
					tag, SSD_DEFAULT_HEADER_LENGTH, LOAD_BUF_SIZE);
			file_size -= unusable_size;
		}

		if (file_size > AS_STORAGE_MAX_DEVICE_SIZE) {
			cf_warning(AS_DRV_SSD, "%s size must be <= %"PRId64", trimming original size %"PRId64,
					tag, AS_STORAGE_MAX_DEVICE_SIZE, file_size);
			file_size = AS_STORAGE_MAX_DEVICE_SIZE;
		}
	}

	if (file_size <= SSD_DEFAULT_HEADER_LENGTH) {
		cf_crash(AS_DRV_SSD, "%s size %"PRId64" must be greater than header size %d",
				tag, file_size, SSD_DEFAULT_HEADER_LENGTH);
	}

	return file_size;
}


static uint64_t
find_io_min_size(int fd, const char *ssd_name)
{
	off_t off = lseek(fd, 0, SEEK_SET);

	if (off != 0) {
		cf_crash(AS_DRV_SSD, "%s: seek error %s", ssd_name, cf_strerror(errno));
	}

	uint8_t *buf = cf_valloc(HI_IO_MIN_SIZE);
	size_t read_sz = LO_IO_MIN_SIZE;

	while (read_sz <= HI_IO_MIN_SIZE) {
		if (read(fd, (void*)buf, read_sz) == (ssize_t)read_sz) {
			cf_free(buf);
			return read_sz;
		}

		read_sz <<= 1; // LO_IO_MIN_SIZE and HI_IO_MIN_SIZE are powers of 2
	}

	cf_crash(AS_DRV_SSD, "%s: read failed at all sizes from %u to %u bytes",
			ssd_name, LO_IO_MIN_SIZE, HI_IO_MIN_SIZE);

	return 0;
}


int
ssd_init_devices(as_namespace *ns, drv_ssds **ssds_p)
{
	int n_ssds;

	for (n_ssds = 0; n_ssds < AS_STORAGE_MAX_DEVICES; n_ssds++) {
		if (! ns->storage_devices[n_ssds]) {
			break;
		}
	}

	size_t ssds_size = sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd));
	drv_ssds *ssds = cf_malloc(ssds_size);

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "failed drv_ssds malloc");
		return -1;
	}

	memset(ssds, 0, ssds_size);
	ssds->n_ssds = n_ssds;
	ssds->ns = ns;

	// Raw device-specific initialization of drv_ssd structures.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->name = ns->storage_devices[i];

		ssd->open_flag = O_RDWR |
				(ns->storage_disable_odirect ? 0 : O_DIRECT) |
				(ns->storage_enable_osync ? O_SYNC : 0);

		int fd = open(ssd->name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open device %s: %s", ssd->name,
					cf_strerror(errno));
			return -1;
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		ssd->file_size = check_file_size((off_t)size, "usable device");
		ssd->io_min_size = find_io_min_size(fd, ssd->name);

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (! as_storage_empty_header(fd, ssd->name)) {
				close(fd);
				return -1;
			}

			cf_info(AS_DRV_SSD, "cold-start-empty - erased header of %s",
					ssd->name);
		}

		close(fd);

		ns->ssd_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "opened device %s: usable size %lu, io-min-size %lu",
				ssd->name, ssd->file_size, ssd->io_min_size);

		if (ns->storage_scheduler_mode) {
			// Set scheduler mode specified in config file.
			ssd_set_scheduler_mode(ssd->name, ns->storage_scheduler_mode);
		}
	}

	*ssds_p = ssds;

	return 0;
}


int
ssd_init_shadows(as_namespace *ns, drv_ssds *ssds)
{
	int n_shadows = 0;

	for (int n = 0; n < ssds->n_ssds; n++) {
		if (ns->storage_shadows[n]) {
			n_shadows++;
		}
	}

	if (n_shadows == 0) {
		// No shadows - a normal deployment.
		return 0;
	}

	if (n_shadows != ssds->n_ssds) {
		cf_warning(AS_DRV_SSD, "configured %d devices but only %d shadows",
				ssds->n_ssds, n_shadows);
		return -1;
	}

	// Check shadow devices.
	for (int i = 0; i < n_shadows; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->shadow_name = ns->storage_shadows[i];

		int fd = open(ssd->shadow_name, ssd->open_flag, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open shadow device %s: %s",
					ssd->shadow_name, cf_strerror(errno));
			return -1;
		}

		uint64_t size = 0;

		ioctl(fd, BLKGETSIZE64, &size); // gets the number of bytes

		if ((off_t)size < ssd->file_size) {
			cf_warning(AS_DRV_SSD, "shadow device %s is smaller than main device - %lu < %lu",
					ssd->shadow_name, size, ssd->file_size);
			close(fd);
			return -1;
		}

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (! as_storage_empty_header(fd, ssd->shadow_name)) {
				close(fd);
				return -1;
			}

			cf_info(AS_DRV_SSD, "cold-start-empty - erased header of %s",
					ssd->shadow_name);
		}

		close(fd);

		cf_info(AS_DRV_SSD, "shadow device %s is compatible with main device",
				ssd->shadow_name);

		if (ns->storage_scheduler_mode) {
			// Set scheduler mode specified in config file.
			ssd_set_scheduler_mode(ssd->shadow_name,
					ns->storage_scheduler_mode);
		}
	}

	return 0;
}


int
ssd_init_files(as_namespace *ns, drv_ssds **ssds_p)
{
	int n_ssds;

	for (n_ssds = 0; n_ssds < AS_STORAGE_MAX_FILES; n_ssds++) {
		if (! ns->storage_files[n_ssds]) {
			break;
		}
	}

	size_t ssds_size = sizeof(drv_ssds) + (n_ssds * sizeof(drv_ssd));
	drv_ssds *ssds = cf_malloc(ssds_size);

	if (! ssds) {
		cf_warning(AS_DRV_SSD, "failed drv_ssds malloc");
		return -1;
	}

	memset(ssds, 0, ssds_size);
	ssds->n_ssds = n_ssds;
	ssds->ns = ns;

	// File-specific initialization of drv_ssd structures.
	for (int i = 0; i < n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->name = ns->storage_files[i];

		if (ns->cold_start && ns->storage_cold_start_empty) {
			if (0 == remove(ssd->name)) {
				cf_info(AS_DRV_SSD, "cold-start-empty - removed %s", ssd->name);
			}
			else if (errno == ENOENT) {
				cf_info(AS_DRV_SSD, "cold-start-empty - no file %s", ssd->name);
			}
			else {
				cf_warning(AS_DRV_SSD, "failed remove: errno %d", errno);
				return -1;
			}
		}

		ssd->open_flag = O_RDWR;

		// Validate that file can be opened, create it if it doesn't exist.
		int fd = open(ssd->name, ssd->open_flag | O_CREAT, S_IRUSR | S_IWUSR);

		if (-1 == fd) {
			cf_warning(AS_DRV_SSD, "unable to open file %s: %s", ssd->name,
					cf_strerror(errno));
			return -1;
		}

		ssd->file_size = check_file_size(ns->storage_filesize, "file");
		ssd->io_min_size = LO_IO_MIN_SIZE;

		// Truncate will grow or shrink the file to the correct size.
		if (0 != ftruncate(fd, ssd->file_size)) {
			cf_warning(AS_DRV_SSD, "unable to truncate file: errno %d", errno);
			close(fd);
			return -1;
		}

		close(fd);

		ns->ssd_size += ssd->file_size; // increment total storage size

		cf_info(AS_DRV_SSD, "opened file %s: usable size %lu", ssd->name,
				ssd->file_size);
	}

	*ssds_p = ssds;

	return 0;
}


//==========================================================
// Storage API implementation: startup, shutdown, etc.
//

int
as_storage_namespace_init_ssd(as_namespace *ns, cf_queue *complete_q,
		void *udata)
{
	drv_ssds *ssds;

	if (ns->storage_devices[0]) {
		if (0 != ssd_init_devices(ns, &ssds)) {
			cf_warning(AS_DRV_SSD, "{%s} can't initialize devices", ns->name);
			return -1;
		}

		if (0 != ssd_init_shadows(ns, ssds)) {
			cf_warning(AS_DRV_SSD, "{%s} can't initialize shadows", ns->name);
			return -1;
		}
	}
	else if (ns->storage_files[0]) {
		if (0 != ssd_init_files(ns, &ssds)) {
			cf_warning(AS_DRV_SSD, "{%s} can't initialize files", ns->name);
			return -1;
		}
	}
	else {
		cf_warning(AS_DRV_SSD, "{%s} has no devices or files", ns->name);
		return -1;
	}

	// Allow defrag to go full speed during startup - restore the configured
	// settings when startup is done.
	ns->saved_defrag_sleep = ns->storage_defrag_sleep;
	ns->storage_defrag_sleep = 0;

	check_write_block_size(ns->storage_write_block_size);

	// The queue limit is more efficient to work with.
	ns->storage_max_write_q = (int)
			(ns->storage_max_write_cache / ns->storage_write_block_size);

	// Minimize how often we recalculate this.
	ns->defrag_lwm_size =
			(ns->storage_write_block_size * ns->storage_defrag_lwm_pct) / 100;

	ns->storage_private = (void*)ssds;

	char histname[HISTOGRAM_NAME_SIZE];

	snprintf(histname, sizeof(histname), "{%s}-device-read-size", ns->name);

	if (! (ns->device_read_size_hist = histogram_create(histname, HIST_SIZE))) {
		cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
	}

	snprintf(histname, sizeof(histname), "{%s}-device-write-size", ns->name);

	if (! (ns->device_write_size_hist = histogram_create(histname, HIST_SIZE))) {
		cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
	}

	// Finish initializing drv_ssd structures (non-zero-value members).
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		ssd->ns = ns;
		ssd->file_id = i;

		pthread_mutex_init(&ssd->write_lock, 0);
		pthread_mutex_init(&ssd->defrag_lock, 0);

		ssd->running = true;

		ssd->data_in_memory = ns->storage_data_in_memory;
		ssd->write_block_size = ns->storage_write_block_size;

		ssd_wblock_init(ssd);

		// Note: free_wblock_q, defrag_wblock_q created after loading devices.

		if (! (ssd->fd_q = cf_queue_create(sizeof(int), true))) {
			cf_crash(AS_DRV_SSD, "can't create fd queue");
		}

		if (ssd->shadow_name &&
				! (ssd->shadow_fd_q = cf_queue_create(sizeof(int), true))) {
			cf_crash(AS_DRV_SSD, "can't create shadow fd queue");
		}

		if (! (ssd->swb_write_q = cf_queue_create(sizeof(void*), true))) {
			cf_crash(AS_DRV_SSD, "can't create swb-write queue");
		}

		if (ssd->shadow_name &&
				! (ssd->swb_shadow_q = cf_queue_create(sizeof(void*), true))) {
			cf_crash(AS_DRV_SSD, "can't create swb-shadow queue");
		}

		if (! (ssd->swb_free_q = cf_queue_create(sizeof(void*), true))) {
			cf_crash(AS_DRV_SSD, "can't create swb-free queue");
		}

		if (! ns->storage_data_in_memory) {
			if (! (ssd->post_write_q = cf_queue_create(sizeof(void*), false))) {
				cf_crash(AS_DRV_SSD, "can't create post-write queue");
			}
		}

		snprintf(histname, sizeof(histname), "{%s}-%s-read", ns->name, ssd->name);

		if (! (ssd->hist_read = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
		}

		snprintf(histname, sizeof(histname), "{%s}-%s-large-block-read", ns->name, ssd->name);

		if (! (ssd->hist_large_block_read = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD,"cannot create histogram %s", histname);
		}

		snprintf(histname, sizeof(histname), "{%s}-%s-write", ns->name, ssd->name);

		if (! (ssd->hist_write = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
		}

		if (ssd->shadow_name) {
			snprintf(histname, sizeof(histname), "{%s}-%s-shadow-write", ns->name, ssd->name);

			if (! (ssd->hist_shadow_write = histogram_create(histname, HIST_MILLISECONDS))) {
				cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
			}
		}

		snprintf(histname, sizeof(histname), "{%s}-%s-fsync", ns->name, ssd->name);

		if (! (ssd->hist_fsync = histogram_create(histname, HIST_MILLISECONDS))) {
			cf_crash(AS_DRV_SSD, "cannot create histogram %s", histname);
		}
	}

	// Attempt to load the data.
	//
	// Return value 'false' means it's going to take a long time and will later
	// asynchronously signal completion via the complete_q, 'true' means it's
	// finished, signal here.

	if (ssd_load_devices(ssds, complete_q, udata)) {
		ssd_load_wblock_queues(ssds);

		cf_queue_push(complete_q, &udata);

		ssd_start_maintenance_threads(ssds);
		ssd_start_write_worker_threads(ssds);
		ssd_start_defrag_threads(ssds);
	}

	return 0;
}


void
as_storage_cold_start_ticker_ssd()
{
	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		if (ns->cold_start_loading) {
			char buf[2048];
			int pos = 0;
			drv_ssds *ssds = (drv_ssds*)ns->storage_private;

			for (int j = 0; j < ssds->n_ssds; j++) {
				drv_ssd *ssd = &ssds->ssds[j];
				uint32_t pct = (ssd->cold_start_block_counter * 100) /
						(ssd->file_size / LOAD_BUF_SIZE);

				pos += sprintf(buf + pos, ", %s %u%%", ssd->name, pct);
			}

			// TODO - selective on sub-records also?
			// TODO - conform with new log standard?
			if (ns->n_tombstones == 0) {
				cf_info(AS_DRV_SSD, "{%s} loaded %lu records, %lu subrecords%s",
						ns->name, ns->n_objects, ns->n_sub_objects, buf);
			}
			else {
				cf_info(AS_DRV_SSD, "{%s} loaded %lu records, %lu subrecords, %lu tombstones%s",
						ns->name, ns->n_objects, ns->n_sub_objects,
						ns->n_tombstones, buf);
			}
		}
	}
}


int
as_storage_namespace_destroy_ssd(as_namespace *ns)
{
	// This is not called - for now we don't bother unwinding.
	return 0;
}


// Note that this is *NOT* the counterpart to as_storage_record_create_ssd()!
// That would be as_storage_record_close_ssd(). This is what gets called when a
// record is destroyed, to dereference storage.
int
as_storage_record_destroy_ssd(as_namespace *ns, as_record *r)
{
	if (STORAGE_RBLOCK_IS_VALID(r->rblock_id) && r->n_rblocks != 0) {
		drv_ssds *ssds = (drv_ssds*)ns->storage_private;
		drv_ssd *ssd = &ssds->ssds[r->file_id];

		ssd_block_free(ssd, r->rblock_id, r->n_rblocks, "destroy");

		r->rblock_id = 0;
		r->n_rblocks = 0;
	}

	return 0;
}


//==========================================================
// Storage API implementation: as_storage_rd cycle.
//

int
as_storage_record_create_ssd(as_storage_rd *rd)
{
	rd->block = NULL;
	rd->must_free_block = NULL;
	rd->ssd = NULL;

	cf_assert(rd->r->rblock_id == 0, AS_DRV_SSD, "unexpected - uninitialized rblock-id");

	return 0;
}


int
as_storage_record_open_ssd(as_storage_rd *rd)
{
	drv_ssds *ssds = (drv_ssds*)rd->ns->storage_private;

	rd->block = NULL;
	rd->must_free_block = NULL;
	rd->ssd = &ssds->ssds[rd->r->file_id];

	return 0;
}


int
as_storage_record_close_ssd(as_storage_rd *rd)
{
	if (rd->must_free_block) {
		cf_free(rd->must_free_block);
		rd->must_free_block = NULL;
		rd->block = NULL;
	}

	return 0;
}


// These are near the top of this file:
//		as_storage_record_get_n_bins_ssd()
//		as_storage_record_read_ssd()
//		as_storage_particle_read_all_ssd()
//		as_storage_particle_read_and_size_all_ssd()


bool
as_storage_record_size_and_check_ssd(as_storage_rd *rd)
{
	return rd->ns->storage_write_block_size >= as_storage_record_size(rd);
}


//==========================================================
// Storage API implementation: storage capacity monitoring.
//

void
as_storage_wait_for_defrag_ssd(as_namespace *ns)
{
	if (ns->storage_defrag_startup_minimum > 0) {
		while (true) {
			int avail_pct;

			if (0 != as_storage_stats_ssd(ns, &avail_pct, 0)) {
				cf_crash(AS_DRV_SSD, "namespace %s storage stats failed",
						ns->name);
			}

			if (avail_pct >= ns->storage_defrag_startup_minimum) {
				break;
			}

			cf_info(AS_DRV_SSD, "namespace %s waiting for defrag: %d pct available, waiting for %d ...",
					ns->name, avail_pct, ns->storage_defrag_startup_minimum);

			sleep(2);
		}
	}

	// Restore configured defrag throttling values.
	ns->storage_defrag_sleep = ns->saved_defrag_sleep;
}


bool
as_storage_overloaded_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	int max_write_q = ns->storage_max_write_q;

	// TODO - would be nice to not do this loop every single write transaction!
	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		int qsz = cf_queue_sz(ssd->swb_write_q);

		if (qsz > max_write_q) {
			cf_ticker_warning(AS_DRV_SSD, "{%s} write fail: queue too deep: exceeds max %d",
					ns->name, max_write_q);
			return true;
		}

		if (ssd->shadow_name) {
			qsz = cf_queue_sz(ssd->swb_shadow_q);

			if (qsz > max_write_q) {
				cf_ticker_warning(AS_DRV_SSD, "{%s} write fail: shadow queue too deep: exceeds max %d",
						ns->name, max_write_q);
				return true;
			}
		}
	}

	return false;
}


bool
as_storage_has_space_ssd(as_namespace *ns)
{
	// Shortcut - assume we can't go from 5% to 0% in 1 ticker interval.
	if (ns->storage_last_avail_pct > 5) {
		return true;
	}
	// else - running low on available percent, check rigorously...

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		if (cf_queue_sz(ssds->ssds[i].free_wblock_q) < min_free_wblocks(ns)) {
			return false;
		}
	}

	return true;
}


void
as_storage_defrag_sweep_ssd(as_namespace *ns)
{
	cf_info(AS_DRV_SSD, "{%s} sweeping all devices for wblocks to defrag ...", ns->name);

	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		cf_atomic32_incr(&ssds->ssds[i].defrag_sweep);
	}
}


//==========================================================
// Storage API implementation: data in device headers.
//

void
as_storage_info_set_ssd(as_namespace *ns, uint32_t pid,
		const as_partition_version *version)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	info_buf *b = (info_buf*)
			(ssds->header->info_data + (SSD_HEADER_INFO_STRIDE * pid));

	b->unused = 0;
	b->version = *version;
}


void
as_storage_info_get_ssd(as_namespace *ns, uint32_t pid,
		as_partition_version *version)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;
	info_buf *b = (info_buf*)
			(ssds->header->info_data + (SSD_HEADER_INFO_STRIDE * pid));

	*version = b->version;
}


int
as_storage_info_flush_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		as_storage_write_header(ssd, ssds->header, ssds->header->header_length);
	}

	return 0;
}


void
as_storage_save_evict_void_time_ssd(as_namespace *ns, uint32_t evict_void_time)
{
	drv_ssds* ssds = (drv_ssds*)ns->storage_private;

	ssds->header->last_evict_void_time = evict_void_time;

	// Customized write instead of using as_storage_info_flush_ssd() so we can
	// write 512-4096b instead of 1Mb (and not interfere with potentially
	// concurrent writes for partition info).

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd* ssd = &ssds->ssds[i];
		size_t peek_size = BYTES_UP_TO_IO_MIN(ssd, sizeof(ssd_device_header));

		as_storage_write_header(ssd, ssds->header, peek_size);
	}
}


//==========================================================
// Storage API implementation: statistics.
//

int
as_storage_stats_ssd(as_namespace *ns, int *available_pct,
		uint64_t *used_disk_bytes)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	if (available_pct) {
		*available_pct = 100;

		// Find the device with the lowest available percent.
		for (int i = 0; i < ssds->n_ssds; i++) {
			drv_ssd *ssd = &ssds->ssds[i];
			uint64_t pct = (available_size(ssd) * 100) / ssd->file_size;

			if (pct < (uint64_t)*available_pct) {
				*available_pct = pct;
			}
		}

		// Used for shortcut in as_storage_has_space_ssd(), which is done on a
		// per-transaction basis:
		ns->storage_last_avail_pct = *available_pct;
	}

	if (used_disk_bytes) {
		uint64_t sz = 0;

		for (int i = 0; i < ssds->n_ssds; i++) {
			sz += ssds->ssds[i].inuse_size;
		}

		*used_disk_bytes = sz;
	}

	return 0;
}


int
as_storage_ticker_stats_ssd(as_namespace *ns)
{
	histogram_dump(ns->device_read_size_hist);
	histogram_dump(ns->device_write_size_hist);

	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_dump(ssd->hist_read);
		histogram_dump(ssd->hist_large_block_read);
		histogram_dump(ssd->hist_write);

		if (ssd->hist_shadow_write) {
			histogram_dump(ssd->hist_shadow_write);
		}

		histogram_dump(ssd->hist_fsync);
	}

	return 0;
}


int
as_storage_histogram_clear_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		histogram_clear(ssd->hist_read);
		histogram_clear(ssd->hist_large_block_read);
		histogram_clear(ssd->hist_write);

		if (ssd->hist_shadow_write) {
			histogram_clear(ssd->hist_shadow_write);
		}

		histogram_clear(ssd->hist_fsync);
	}

	return 0;
}


//==========================================================
// Shutdown.
//

void
as_storage_shutdown_ssd(as_namespace *ns)
{
	drv_ssds *ssds = (drv_ssds*)ns->storage_private;

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		// Stop the maintenance thread from (also) flushing the swbs.
		pthread_mutex_lock(&ssd->write_lock);
		pthread_mutex_lock(&ssd->defrag_lock);

		// Flush current swb by pushing it to write-q.
		if (ssd->current_swb) {
			// Clean the end of the buffer before pushing to write-q.
			if (ssd->write_block_size > ssd->current_swb->pos) {
				memset(&ssd->current_swb->buf[ssd->current_swb->pos], 0,
						ssd->write_block_size - ssd->current_swb->pos);
			}

			cf_queue_push(ssd->swb_write_q, &ssd->current_swb);
			ssd->current_swb = NULL;
		}

		// Flush defrag swb by pushing it to write-q.
		if (ssd->defrag_swb) {
			// Clean the end of the buffer before pushing to write-q.
			if (ssd->write_block_size > ssd->defrag_swb->pos) {
				memset(&ssd->defrag_swb->buf[ssd->defrag_swb->pos], 0,
						ssd->write_block_size - ssd->defrag_swb->pos);
			}

			cf_queue_push(ssd->swb_write_q, &ssd->defrag_swb);
			ssd->defrag_swb = NULL;
		}
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];

		while (cf_queue_sz(ssd->swb_write_q)) {
			usleep(1000);
		}

		if (ssd->shadow_name) {
			while (cf_queue_sz(ssd->swb_shadow_q)) {
				usleep(1000);
			}
		}

		ssd->running = false;
	}

	for (int i = 0; i < ssds->n_ssds; i++) {
		drv_ssd *ssd = &ssds->ssds[i];
		void *p_void;

		for (uint32_t j = 0; j < ssds->ns->storage_write_threads; j++) {
			pthread_join(ssd->write_worker_thread[j], &p_void);
		}

		if (ssd->shadow_name) {
			pthread_join(ssd->shadow_worker_thread, &p_void);
		}
	}
}
