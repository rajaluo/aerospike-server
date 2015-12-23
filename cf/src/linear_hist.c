/*
 * linear_hist.c
 *
 * Copyright (C) 2015 Aerospike, Inc.
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

#include "linear_hist.h"

#include <pthread.h>
//#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

//#include "dynbuf.h"
#include "fault.h"



//------------------------------------------------
// Create a linear histogram.
//
linear_hist*
linear_hist_create(const char *name, uint32_t start, uint32_t max_offset, uint32_t num_buckets)
{
	if (! (name && strlen(name) < LINEAR_HIST_NAME_SIZE)) {
		cf_crash(AS_INFO, "linear_hist_create - bad name %s", name ? name : "<null>");
	}

	if (num_buckets == 0) {
		cf_crash(AS_INFO, "linear_hist_create -  0 num_buckets");
	}

	linear_hist *h = cf_malloc(sizeof(linear_hist) + (sizeof(uint32_t) * num_buckets));

	if (! h) {
		cf_crash(AS_INFO, "linear_hist_create -  alloc failed");
	}

	strcpy(h->name, name);

	if (0 != pthread_mutex_init(&h->info_lock, NULL)) {
		cf_crash(AS_INFO, "linear_hist_create -  mutex init failed");
	}

	h->info_snapshot[0] = 0;

	h->num_buckets = num_buckets;
	linear_hist_clear(h, start, max_offset);

	return h;
}

//------------------------------------------------
// Destroy a linear histogram.
//
void
linear_hist_destroy(linear_hist *h)
{
	pthread_mutex_destroy(&h->info_lock);
	cf_free(h);
}

//------------------------------------------------
// Clear and (re-)scale a linear histogram.
//
void
linear_hist_clear(linear_hist *h, uint32_t start, uint32_t max_offset)
{
	h->start = start;
	h->bucket_width = (max_offset + (h->num_buckets - 1)) / h->num_buckets;

	// Only needed to protect against max_offset 0.
	if (h->bucket_width == 0) {
		h->bucket_width = 1;
	}

	memset((void *)&h->counts, 0, sizeof(uint32_t) * h->num_buckets);
}

//------------------------------------------------
// Access method for total count.
//
uint32_t
linear_hist_get_total(linear_hist *h)
{
	uint32_t total_count = 0;

	for (uint32_t i = 0; i < h->num_buckets; i++) {
		total_count += h->counts[i];
	}

	return total_count;
}

//------------------------------------------------
// Insert a data point. Points out of range will
// end up in the bucket at the appropriate end.
//
void
linear_hist_insert_data_point(linear_hist *h, uint32_t point)
{
	int32_t offset = (int32_t)(point - h->start);
	int32_t bucket = 0;

	if (offset > 0) {
		bucket = offset / h->bucket_width;

		if (bucket >= (int32_t)h->num_buckets) {
			bucket = h->num_buckets - 1;
		}
	}

	h->counts[bucket]++;
}

//------------------------------------------------
// Get the low edge of the "threshold" bucket -
// the bucket in which the specified percentage of
// total count is exceeded (accumulating from low
// bucket).
//
uint32_t
linear_hist_get_threshold_for_fraction(linear_hist *h, uint32_t tenths_pct, uint32_t *p_low)
{
	return linear_hist_get_threshold_for_subtotal(h, (linear_hist_get_total(h) * tenths_pct) / 1000, p_low);
}

//------------------------------------------------
// Get the low edge of the "threshold" bucket -
// the bucket in which the specified subtotal
// count is exceeded (accumulating from low
// bucket).
//
uint32_t
linear_hist_get_threshold_for_subtotal(linear_hist *h, uint32_t subtotal, uint32_t *p_low)
{
	uint32_t count = 0;
	uint32_t i;

	for (i = 0; i < h->num_buckets; i++) {
		count += h->counts[i];

		if (count > subtotal) {
			break;
		}
	}

	if (i == h->num_buckets) {
		// This means subtotal >= h->total_count.
		*p_low = 0xFFFFffff;
		return count;
	}

	*p_low = h->start + (i * h->bucket_width);

	// Return subtotal of everything below "threshold" bucket.
	return count - h->counts[i];
}
