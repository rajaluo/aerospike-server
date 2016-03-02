/*
 * vmapx.c
 *
 * Copyright (C) 2012-2016 Aerospike, Inc.
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
// Includes
//

#include "vmapx.h"

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"

#include "util.h"


//==========================================================
// Forward Declarations
//

static bool get_index(const cf_vmapx* this, const char* name, uint32_t* p_index);


//==========================================================
// Public API
//

//------------------------------------------------
// Return persistent memory size needed. Includes
// cf_vmap struct plus values vector.
//
size_t
cf_vmapx_sizeof(uint32_t value_size, uint32_t max_count)
{
	return sizeof(cf_vmapx) + ((size_t)value_size * (size_t)max_count);
}

//------------------------------------------------
// Create a cf_vmapx object in persistent memory.
//
cf_vmapx_err
cf_vmapx_create(cf_vmapx* this, uint32_t value_size, uint32_t max_count,
		uint32_t hash_size, uint32_t max_name_size)
{
	// Value-size needs to be a multiple of 4 bytes for thread safety.
	if ((value_size & 3) || ! max_count || ! hash_size || ! max_name_size) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	this->value_size = value_size;
	this->max_count = max_count;
	this->count = 0;

	if (! (this->p_hash = vhash_create(max_name_size, hash_size))) {
		return CF_VMAPX_ERR_UNKNOWN;
	}

	this->key_size = max_name_size;

	pthread_mutex_init(&this->write_lock, 0);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Free internal resources of a cf_vmapx object.
// Don't call after failed cf_vmapx_create() or
// cf_vmapx_resume() call - those functions clean
// up on failure.
//
void
cf_vmapx_release(cf_vmapx* this)
{
	// Helps in handling bins vmap, which doesn't exist in single-bin mode.
	if (! this) {
		return;
	}

	pthread_mutex_destroy(&this->write_lock);

	vhash_destroy(this->p_hash);
}

//------------------------------------------------
// Return count.
//
uint32_t
cf_vmapx_count(const cf_vmapx* this)
{
	return cf_atomic32_get(this->count);
}

//------------------------------------------------
// Get value by index.
//
cf_vmapx_err
cf_vmapx_get_by_index(const cf_vmapx* this, uint32_t index, void** pp_value)
{
	if (index >= cf_atomic32_get(this->count)) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	*pp_value = cf_vmapx_value_ptr(this, index);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get value by name.
//
cf_vmapx_err
cf_vmapx_get_by_name(const cf_vmapx* this, const char* name, void** pp_value)
{
	uint32_t index;

	if (! get_index(this, name, &index)) {
		return CF_VMAPX_ERR_NAME_NOT_FOUND;
	}

	*pp_value = cf_vmapx_value_ptr(this, index);

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get index by name. May pass null p_index to
// just check existence.
//
cf_vmapx_err
cf_vmapx_get_index(const cf_vmapx* this, const char* name, uint32_t* p_index)
{
	return get_index(this, name, p_index) ?
			CF_VMAPX_OK : CF_VMAPX_ERR_NAME_NOT_FOUND;
}

//------------------------------------------------
// The value must begin with a null-terminated
// string which is its name. (The hash map is not
// stored in persistent memory, so names must be
// in the vector to enable us to rebuild the hash
// map on warm restart.)
//
// If name is not found, add new value and return
// newly assigned index (and CF_VMAPX_OK). If name
// is found, return index for existing name (with
// CF_VMAPX_ERR_NAME_EXISTS) but ignore new value.
// May pass null p_index.
//
cf_vmapx_err
cf_vmapx_put_unique(cf_vmapx* this, const void* p_value, uint32_t* p_index)
{
	pthread_mutex_lock(&this->write_lock);

	// If name is found, return existing name's index, ignore p_value.
	if (vhash_get_z(this->p_hash, (const char*)p_value, p_index)) {
		pthread_mutex_unlock(&this->write_lock);

		return CF_VMAPX_ERR_NAME_EXISTS;
	}

	uint32_t count = cf_atomic32_get(this->count);

	// Not allowed to add more.
	if (count >= this->max_count) {
		pthread_mutex_unlock(&this->write_lock);

		return CF_VMAPX_ERR_FULL;
	}

	// Add to vector.
	memcpy(cf_vmapx_value_ptr(this, count), p_value, this->value_size);

	// Increment count here so indexes returned by other public API calls (just
	// after adding to hash below) are guaranteed to be valid.
	cf_atomic32_incr(&this->count);

	// Add to hash.
	cf_vmapx_err rv = vhash_put_z(this->p_hash, (const char*)p_value, count);

	if (rv != CF_VMAPX_OK) {
		cf_atomic32_decr(&this->count);

		pthread_mutex_unlock(&this->write_lock);

		return rv;
	}

	pthread_mutex_unlock(&this->write_lock);

	if (p_index) {
		*p_index = count;
	}

	return CF_VMAPX_OK;
}


//==========================================================
// Private Functions
//

//------------------------------------------------
// Return value pointer at trusted index.
//
void*
cf_vmapx_value_ptr(const cf_vmapx* this, uint32_t index)
{
	return (void*)(this->values + (this->value_size * index));
}

//------------------------------------------------
// Get index by trusted name.
//
static bool
get_index(const cf_vmapx* this, const char* name, uint32_t* p_index)
{
	return vhash_get_z(this->p_hash, name, p_index);
}


//==========================================================
// vhash Class
//

// Custom hashmap for cf_vmapx usage.
// - Elements are added but never removed.
// - It's thread safe yet lockless. (Relies on cf_vmapx's write_lock.)
// - Element keys are null-terminated strings.
// - Element values are uint32_t's.

struct vhash_s {
	uint32_t key_size;
	uint32_t ele_size;
	uint32_t n_rows;
	uint8_t* table;
	uint32_t row_counts[];
};

typedef struct vhash_ele_s {
	struct vhash_ele_s* next;
	uint8_t data[]; // key_size bytes of key, 4 bytes of value
} vhash_ele;

#define VHASH_ELE_KEY_PTR(_e)		((char*)_e->data)
#define VHASH_ELE_VALUE_PTR(_h, _e)	((uint32_t*)(_e->data + _h->key_size))

//------------------------------------------------
// Create vhash with specified key size (max) and
// number or rows.
//
vhash*
vhash_create(uint32_t key_size, uint32_t n_rows)
{
	size_t row_counts_size = n_rows * sizeof(uint32_t);
	vhash* h = (vhash*)cf_malloc(sizeof(vhash) + row_counts_size);

	if (! h) {
		return NULL;
	}

	h->key_size = key_size;
	h->ele_size = sizeof(vhash_ele) + key_size + sizeof(uint32_t);
	h->n_rows = n_rows;

	size_t table_size = n_rows * h->ele_size;

	if (! (h->table = (uint8_t*)cf_malloc(table_size))) {
		cf_free(h);
		return NULL;
	}

	memset((void*)h->row_counts, 0, row_counts_size);
	memset((void*)h->table, 0, table_size);

	return h;
}

//------------------------------------------------
// Destroy vhash. (Assumes it was fully created.)
//
void
vhash_destroy(vhash* h)
{
	vhash_ele* e_table = (vhash_ele*)h->table;

	for (uint32_t i = 0; i < h->n_rows; i++) {
		if (e_table->next) {
			vhash_ele* e = e_table->next;

			while (e) {
				vhash_ele* t = e->next;

				cf_free(e);
				e = t;
			}
		}

		e_table = (vhash_ele*)((uint8_t*)e_table + h->ele_size);
	}

	cf_free(h->table);
	cf_free(h);
}

//------------------------------------------------
// Add element using null-terminated key.
//
cf_vmapx_err
vhash_put_z(vhash* h, const char* zkey, uint32_t value)
{
	size_t zkey_len = strlen(zkey);

	if (zkey_len >= h->key_size) {
		return CF_VMAPX_ERR_BAD_PARAM;
	}

	uint64_t hashed_key = cf_hash_fnv((void*)zkey, zkey_len);
	uint32_t row_i = (uint32_t)(hashed_key % h->n_rows);

	vhash_ele* e = (vhash_ele*)(h->table + (h->ele_size * row_i));

	if (h->row_counts[row_i] == 0) {
		strcpy(VHASH_ELE_KEY_PTR(e), zkey);
		*VHASH_ELE_VALUE_PTR(h, e) = value;
		h->row_counts[row_i]++;

		return CF_VMAPX_OK;
	}

	vhash_ele* e_head = e;

	// This function is always called under write lock, after get, so we'll
	// never encounter the key - don't bother checking it.
	while (e) {
		e = e->next;
	}

	e = (vhash_ele*)cf_malloc(h->ele_size);

	if (! e) {
		return CF_VMAPX_ERR_UNKNOWN;
	}

	e->next = e_head->next;
	e_head->next = e;

	strcpy(VHASH_ELE_KEY_PTR(e), zkey);
	*VHASH_ELE_VALUE_PTR(h, e) = value;
	h->row_counts[row_i]++;

	return CF_VMAPX_OK;
}

//------------------------------------------------
// Get element value using null-terminated key.
//
bool
vhash_get_z(const vhash* h, const char* zkey, uint32_t* p_value)
{
	uint64_t hashed_key = cf_hash_fnv((void*)zkey, strlen(zkey));
	uint32_t row_i = (uint32_t)(hashed_key % h->n_rows);
	uint32_t row_count = h->row_counts[row_i];

	if (row_count == 0) {
		return false;
	}

	vhash_ele* e = (vhash_ele*)(h->table + (h->ele_size * row_i));

	// Use row count instead of following pointers to the end, for thread
	// safety with concurrent put.
	for (uint32_t j = 0; j < row_count; j++) {
		if (strcmp(VHASH_ELE_KEY_PTR(e), zkey) == 0) {
			if (p_value) {
				*p_value = *VHASH_ELE_VALUE_PTR(h, e);
			}

			return true;
		}

		e = e->next;
	}

	return false;
}
