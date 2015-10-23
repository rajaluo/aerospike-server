/*
 * particle_list.c
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


#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h" // TODO - eventually separate from BLOB
#include "base/proto.h"


//==========================================================
// LIST particle interface - function declarations.
//

// Destructor, etc.
void list_destruct(as_particle *p);
uint32_t list_size(const as_particle *p);

// Handle "wire" format.
int32_t list_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t list_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int list_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int list_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
uint32_t list_wire_size(const as_particle *p);
uint32_t list_to_wire(const as_particle *p, uint8_t *wire);

// Handle in-memory format.
uint32_t list_size_from_mem(as_particle_type type, const uint8_t *value, uint32_t value_size);
void list_from_mem(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp);

// Handle as_val translation.
as_val *list_to_asval(const as_particle *p);

// Handle on-device "flat" format.
int32_t list_size_from_flat(const uint8_t *flat, uint32_t flat_size);
int list_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp);
int list_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp);
uint32_t list_flat_size(const as_particle *p);
uint32_t list_to_flat(const as_particle *p, uint8_t *flat);


//==========================================================
// LIST particle interface - vtable.
//

// TODO - eventually none will use BLOB.
const as_particle_vtable list_vtable = {
		blob_destruct,
		blob_size,

		list_concat_size_from_wire,
		list_append_from_wire,
		list_prepend_from_wire,
		list_incr_from_wire,
		blob_size_from_wire,
		blob_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		blob_to_wire,

		blob_size_from_mem,
		blob_from_mem,

		list_to_asval,

		blob_size_from_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

// For now, same as related BLOB struct. TODO - eventually separate from BLOB.

typedef struct list_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) list_mem;


//==========================================================
// Forward declarations.
//


//==========================================================
// LIST particle interface - function definitions.
//

//------------------------------------------------
// Destructor, etc.
//

void
list_destruct(as_particle *p)
{
	// TODO
}

uint32_t
list_size(const as_particle *p)
{
	// TODO
	return 0;
}

//------------------------------------------------
// Handle "wire" format.
//

int32_t
list_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "concat size for list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
list_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "append to list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
list_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "prepend to list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int
list_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	cf_warning(AS_PARTICLE, "increment of list");
	return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
}

int32_t
list_size_from_wire(const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
	return -1;
}

int
list_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp)
{
	// TODO
	return -1;
}

int
list_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size)
{
	// TODO
	return -1;
}

uint32_t
list_wire_size(const as_particle *p)
{
	// TODO
	return 0;
}

uint32_t
list_to_wire(const as_particle *p, uint8_t *wire)
{
	// TODO
	return 0;
}

//------------------------------------------------
// Handle in-memory format.
//

uint32_t
list_size_from_mem(as_particle_type type, const uint8_t *value, uint32_t value_size)
{
	// TODO
	return 0;
}

void
list_from_mem(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp)
{
	// TODO
}

//------------------------------------------------
// Handle as_val translation.
//

as_val *
list_to_asval(const as_particle *p)
{
	list_mem *p_list_mem = (list_mem *)p;

	as_buffer buf;
	as_buffer_init(&buf);

	buf.data = p_list_mem->data;
	buf.capacity = p_list_mem->sz;
	buf.size = p_list_mem->sz;

	as_serializer s;
	as_msgpack_init(&s);

	as_val *val;

	as_serializer_deserialize(&s, &buf, &val);
	as_serializer_destroy(&s);

	return val;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

int32_t
list_size_from_flat(const uint8_t *flat, uint32_t flat_size)
{
	// TODO
	return -1;
}

int
list_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// TODO
	return -1;
}

int
list_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp)
{
	// TODO
	return -1;
}

uint32_t
list_flat_size(const as_particle *p)
{
	// TODO
	return 0;
}

uint32_t
list_to_flat(const as_particle *p, uint8_t *flat)
{
	// TODO
	return 0;
}


//==========================================================
// Local helpers.
//
