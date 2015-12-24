/*
 * linear_hist.h
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

#include <pthread.h>
#include <stdint.h>
#include "dynbuf.h"

#define LINEAR_HIST_NAME_SIZE 512
#define INFO_SNAPSHOT_SIZE 2048

// DO NOT access this member data directly - use the API!
typedef struct linear_hist_s {
	char name[LINEAR_HIST_NAME_SIZE];

	pthread_mutex_t info_lock;
	char info_snapshot[INFO_SNAPSHOT_SIZE];

	uint32_t num_buckets;
	uint32_t start;
	uint32_t bucket_width;

	uint32_t counts[];
} linear_hist;

linear_hist *linear_hist_create(const char *name, uint32_t start, uint32_t max_offset, uint32_t num_buckets);
void linear_hist_destroy(linear_hist *h);
void linear_hist_clear(linear_hist *h, uint32_t start, uint32_t max_offset);

uint32_t linear_hist_get_total(linear_hist *h);
void linear_hist_merge(linear_hist *h1, linear_hist *h2);
void linear_hist_insert_data_point(linear_hist *h, uint32_t point);
uint32_t linear_hist_get_threshold_for_fraction(linear_hist *h, uint32_t tenths_pct, uint32_t *p_low);
uint32_t linear_hist_get_threshold_for_subtotal(linear_hist *h, uint32_t subtotal, uint32_t *p_low);

void linear_hist_save_info(linear_hist *h);
void linear_hist_get_info(linear_hist *h, cf_dyn_buf *db);
void linear_hist_dump(linear_hist *h);
