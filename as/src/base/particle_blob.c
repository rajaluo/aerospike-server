/*
 * particle_blob.c
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


#include "base/particle_blob.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_bytes.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/proto.h"


// BLOB particle interface function declarations are in particle_blob.h since
// BLOB functions are used by other particles derived from BLOB.


//==========================================================
// BLOB particle interface - vtable.
//

const as_particle_vtable blob_vtable = {
		blob_destruct,
		blob_size,

		blob_concat_size_from_wire,
		blob_append_from_wire,
		blob_prepend_from_wire,
		blob_incr_from_wire,
		blob_size_from_wire,
		blob_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		blob_to_wire,

		blob_size_from_asval,
		blob_from_asval,
		blob_to_asval,
		blob_asval_wire_size,
		blob_asval_to_wire,

		blob_size_from_msgpack,
		blob_from_msgpack,

		blob_size_from_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

typedef struct blob_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) blob_mem;

typedef struct blob_flat_s {
	uint8_t		type;
	uint32_t	size; // host order on device
	uint8_t		data[];
} __attribute__ ((__packed__)) blob_flat;


//==========================================================
// BLOB particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
blob_destruct(as_particle *p)
{
	cf_free(p);
}

uint32_t
blob_size(const as_particle *p)
{
	return (uint32_t)(sizeof(blob_mem) + ((blob_mem *)p)->sz);
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
blob_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch concat sizing blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	return (int32_t)(sizeof(blob_mem) + p_blob_mem->sz + value_size);
}

int
blob_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch appending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	memcpy(p_blob_mem->data + p_blob_mem->sz, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
blob_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	if (wire_type != p_blob_mem->type) {
		cf_warning(AS_PARTICLE, "type mismatch prepending to blob/string, %d:%d", p_blob_mem->type, wire_type);
		return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
	}

	memmove(p_blob_mem->data + value_size, p_blob_mem->data, p_blob_mem->sz);
	memcpy(p_blob_mem->data, wire_value, value_size);
	p_blob_mem->sz += value_size;

	return 0;
}

int
blob_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "unexpected increment of blob/string");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
blob_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// Wire value is same as in-memory value.
	return (int32_t)(sizeof(blob_mem) + value_size);
}

int
blob_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	p_blob_mem->type = wire_type;
	p_blob_mem->sz = value_size;
	memcpy(p_blob_mem->data, wire_value, p_blob_mem->sz);

	return 0;
}

int
blob_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	return (wire_type == p_blob_mem->type &&
			value_size == p_blob_mem->sz &&
			memcmp(wire_value, p_blob_mem->data, value_size) == 0) ? 0 : 1;
}

uint32_t
blob_wire_size(const as_particle *p)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	return p_blob_mem->sz;
}

uint32_t
blob_to_wire(const as_particle *p, uint8_t *wire)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	memcpy(wire, p_blob_mem->data, p_blob_mem->sz);

	return p_blob_mem->sz;
}

//------------------------------------------------
// Handle as_val translation.
//

uint32_t
blob_size_from_asval(const as_val *val)
{
	return (uint32_t)sizeof(blob_mem) + as_bytes_size(as_bytes_fromval(val));
}

void
blob_from_asval(const as_val *val, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	as_bytes *bytes = as_bytes_fromval(val);

	p_blob_mem->type = AS_PARTICLE_TYPE_BLOB;
	p_blob_mem->sz = as_bytes_size(bytes);
	memcpy(p_blob_mem->data, as_bytes_get(bytes), p_blob_mem->sz);
}

as_val *
blob_to_asval(const as_particle *p)
{
	blob_mem *p_blob_mem = (blob_mem *)p;

	uint8_t *value = cf_malloc(p_blob_mem->sz);

	if (! value) {
		return NULL;
	}

	memcpy(value, p_blob_mem->data, p_blob_mem->sz);

	return (as_val *)as_bytes_new_wrap(value, p_blob_mem->sz, true);
}

uint32_t
blob_asval_wire_size(const as_val *val)
{
	return as_bytes_size(as_bytes_fromval(val));
}

uint32_t
blob_asval_to_wire(const as_val *val, uint8_t *wire)
{
	as_bytes *bytes = as_bytes_fromval(val);
	uint32_t size = as_bytes_size(bytes);

	memcpy(wire, as_bytes_get(bytes), size);

	return size;
}

//------------------------------------------------
// Handle msgpack translation.
//

uint32_t
blob_size_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	// TODO - add size of unwrapped bytes!
	return (uint32_t)sizeof(blob_mem);
}

void
blob_from_msgpack(const uint8_t *packed, uint32_t packed_size, as_particle **pp)
{
	blob_mem *p_blob_mem = (blob_mem *)*pp;

	// TODO - get unwrapped bytes!
	p_blob_mem->type = as_particle_type_from_msgpack(packed, packed_size);
	p_blob_mem->sz = 0;
	memcpy(p_blob_mem->data, packed, p_blob_mem->sz);
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
blob_size_from_flat(const uint8_t *flat, uint32_t flat_size)
{
	blob_flat *p_blob_flat = (blob_flat *)flat;
	// Assume type is correct, since we got here.

	// Sanity check length.
	if (p_blob_flat->size != flat_size - sizeof(blob_flat)) {
		cf_warning(AS_PARTICLE, "unexpected flat blob/string: flat size %u, len %u",
				flat_size, p_blob_flat->size);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Flat value is same as in-memory value.
	return (int32_t)(sizeof(blob_mem) + p_blob_flat->size);
}

int
blob_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// Sizing is only a sanity check.
	int32_t mem_size = blob_size_from_flat(flat, flat_size);

	if (mem_size < 0) {
		return mem_size;
	}

	// We can do this only because the flat and in-memory formats are identical.
	*pp = (as_particle *)flat;

	return 0;
}

int
blob_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	int32_t mem_size = blob_size_from_flat(flat, flat_size);

	if (mem_size < 0) {
		return mem_size;
	}

	blob_mem *p_blob_mem = (blob_mem *)cf_malloc((size_t)mem_size);

	if (! p_blob_mem) {
		cf_warning(AS_PARTICLE, "failed malloc for blob/string (%d)", mem_size);
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	const blob_flat *p_blob_flat = (const blob_flat *)flat;

	p_blob_mem->type = p_blob_flat->type;
	p_blob_mem->sz = p_blob_flat->size;
	memcpy(p_blob_mem->data, p_blob_flat->data, p_blob_mem->sz);

	*pp = (as_particle *)p_blob_mem;

	return 0;
}

uint32_t
blob_flat_size(const as_particle *p)
{
	return (uint32_t)(sizeof(blob_flat) + ((blob_mem *)p)->sz);
}

uint32_t
blob_to_flat(const as_particle *p, uint8_t *flat)
{
	blob_mem *p_blob_mem = (blob_mem *)p;
	blob_flat *p_blob_flat = (blob_flat *)flat;

	// Already wrote the type.
	p_blob_flat->size = p_blob_mem->sz;
	memcpy(p_blob_flat->data, p_blob_mem->data, p_blob_flat->size);

	return blob_flat_size(p);
}
