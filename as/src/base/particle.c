/*
 * particle.c
 *
 * Copyright (C) 2008-2015 Aerospike, Inc.
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


#include "base/particle.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_buffer.h"
#include "aerospike/as_msgpack.h"
#include "aerospike/as_serializer.h"
#include "aerospike/as_val.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "fault.h"

#include "base/datamodel.h"
#include "base/ldt.h"
#include "base/proto.h"
#include "fabric/partition.h"
#include "storage/storage.h"


//==========================================================
// Typedefs & constants.
//

extern const as_particle_vtable integer_vtable;
extern const as_particle_vtable float_vtable;
extern const as_particle_vtable string_vtable;
extern const as_particle_vtable blob_vtable;
extern const as_particle_vtable map_vtable;
extern const as_particle_vtable list_vtable;
extern const as_particle_vtable geojson_vtable;

// Array of particle vtable pointers.
const as_particle_vtable *particle_vtable[] = {
		[AS_PARTICLE_TYPE_NULL]			= NULL,
		[AS_PARTICLE_TYPE_INTEGER]		= &integer_vtable,
		[AS_PARTICLE_TYPE_FLOAT]		= &float_vtable,
		[AS_PARTICLE_TYPE_STRING]		= &string_vtable,
		[AS_PARTICLE_TYPE_BLOB]			= &blob_vtable,
		[AS_PARTICLE_TYPE_TIMESTAMP]	= &integer_vtable,
		[AS_PARTICLE_TYPE_JAVA_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_CSHARP_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_PYTHON_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_RUBY_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_PHP_BLOB]		= &blob_vtable,
		[AS_PARTICLE_TYPE_ERLANG_BLOB]	= &blob_vtable,
		[AS_PARTICLE_TYPE_MAP]			= &map_vtable,
		[AS_PARTICLE_TYPE_LIST]			= &list_vtable,
		[AS_PARTICLE_TYPE_HIDDEN_LIST]	= &list_vtable,
		[AS_PARTICLE_TYPE_HIDDEN_MAP]	= &map_vtable,
		[AS_PARTICLE_TYPE_GEOJSON]		= &geojson_vtable
};


//==========================================================
// Local utilities.
//

// Particle type check.
static inline as_particle_type
safe_particle_type(uint8_t type)
{
	switch ((as_particle_type)type) {
	case AS_PARTICLE_TYPE_INTEGER:
	case AS_PARTICLE_TYPE_FLOAT:
	case AS_PARTICLE_TYPE_STRING:
	case AS_PARTICLE_TYPE_BLOB:
	case AS_PARTICLE_TYPE_TIMESTAMP:
	case AS_PARTICLE_TYPE_JAVA_BLOB:
	case AS_PARTICLE_TYPE_CSHARP_BLOB:
	case AS_PARTICLE_TYPE_PYTHON_BLOB:
	case AS_PARTICLE_TYPE_RUBY_BLOB:
	case AS_PARTICLE_TYPE_PHP_BLOB:
	case AS_PARTICLE_TYPE_ERLANG_BLOB:
	case AS_PARTICLE_TYPE_MAP:
	case AS_PARTICLE_TYPE_LIST:
	case AS_PARTICLE_TYPE_HIDDEN_LIST:
	case AS_PARTICLE_TYPE_HIDDEN_MAP:
	case AS_PARTICLE_TYPE_GEOJSON:
		return (as_particle_type)type;
	// Note - AS_PARTICLE_TYPE_NULL is considered bad here.
	default:
		cf_warning(AS_PARTICLE, "encountered bad particle type %u", type);
		return AS_PARTICLE_TYPE_BAD;
	}
}


//==========================================================
// Particle "class static" functions.
//

as_particle_type
as_particle_type_from_asval(const as_val *val)
{
	as_val_t vtype = as_val_type(val);

	switch (vtype) {
	case AS_UNDEF: // if val was null - handle quietly
	case AS_NIL:
		return AS_PARTICLE_TYPE_NULL;
	case AS_BOOLEAN:
	case AS_INTEGER:
		return AS_PARTICLE_TYPE_INTEGER;
	case AS_DOUBLE:
		return AS_PARTICLE_TYPE_FLOAT;
	case AS_STRING:
		return AS_PARTICLE_TYPE_STRING;
	case AS_BYTES:
		return AS_PARTICLE_TYPE_BLOB;
	case AS_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case AS_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case AS_MAP:
		return AS_PARTICLE_TYPE_MAP;
	case AS_REC:
	case AS_PAIR:
	default:
		cf_warning(AS_PARTICLE, "no particle type for as_val_t %d", vtype);
		return AS_PARTICLE_TYPE_NULL;
	}
}

as_particle_type
as_particle_type_from_msgpack(const uint8_t *packed, uint32_t packed_size)
{
	as_val_t vtype = as_unpack_buf_peek_type(packed, packed_size);

	switch (vtype) {
	case AS_NIL:
		return AS_PARTICLE_TYPE_NULL;
	case AS_BOOLEAN:
	case AS_INTEGER:
		return AS_PARTICLE_TYPE_INTEGER;
	case AS_DOUBLE:
		return AS_PARTICLE_TYPE_FLOAT;
	case AS_STRING:
		return AS_PARTICLE_TYPE_STRING;
	case AS_BYTES:
		return AS_PARTICLE_TYPE_BLOB;
	case AS_GEOJSON:
		return AS_PARTICLE_TYPE_GEOJSON;
	case AS_LIST:
		return AS_PARTICLE_TYPE_LIST;
	case AS_MAP:
		return AS_PARTICLE_TYPE_MAP;
	case AS_UNDEF:
	case AS_REC:
	case AS_PAIR:
	default:
		cf_warning(AS_PARTICLE, "encountered bad as_val_t %d", vtype);
		return AS_PARTICLE_TYPE_BAD;
	}
}

// TODO - will we ever need this?
int32_t
as_particle_size_from_client(const as_msg_op *op)
{
	uint8_t type = op->particle_type;
	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);

	return particle_vtable[type]->size_from_wire_fn(value, value_size);
}

int32_t
as_particle_size_from_pickled(uint8_t **p_pickled)
{
	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	uint8_t type = safe_particle_type(*pickled++);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;

	*p_pickled = (uint8_t *)value + value_size;

	return particle_vtable[type]->size_from_wire_fn(value, value_size);
}

uint32_t
as_particle_size_from_asval(const as_val *val)
{
	as_particle_type type = as_particle_type_from_asval(val);

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just skips unmanageable as_val types.
		return 0;
	}

	return particle_vtable[type]->size_from_asval_fn(val);
}

// TODO - will we ever need this?
int32_t
as_particle_size_from_flat(const uint8_t *flat, uint32_t flat_size)
{
	uint8_t type = *flat;

	return particle_vtable[type]->size_from_flat_fn(flat, flat_size);
}

uint32_t
as_particle_asval_client_value_size(const as_val *val)
{
	as_particle_type type = as_particle_type_from_asval(val);

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just sends bin-op with NULL particle to client.
		return 0;
	}

	return particle_vtable[type]->asval_wire_size_fn(val);
}

uint32_t
as_particle_asval_to_client(const as_val *val, as_msg_op *op)
{
	as_particle_type type = as_particle_type_from_asval(val);

	op->particle_type = type;

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just sends bin-op with NULL particle to client.
		return 0;
	}

	uint8_t *value = (uint8_t *)op + sizeof(as_msg_op) + op->name_sz;
	uint32_t added_size = particle_vtable[type]->asval_to_wire_fn(val, value);

	op->op_sz += added_size;

	return added_size;
}


//==========================================================
// as_bin particle functions.
//

//------------------------------------------------
// Destructor, etc.
//

void
as_bin_particle_destroy(as_bin *b, bool free_particle)
{
	if (free_particle && as_bin_is_external_particle(b) && b->particle) {
		particle_vtable[as_bin_get_particle_type(b)]->destructor_fn(b->particle);
	}

	b->particle = NULL;
}

uint32_t
as_bin_particle_size(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		// Single-bin will get here.
		// TODO - clean up code paths so this doesn't happen?
		return 0;
	}

	return particle_vtable[as_bin_get_particle_type(b)]->size_fn(b->particle);
}

//------------------------------------------------
// Handle "wire" format.
//

// TODO - will we ever need this?
// Unlike the other "size_from" methods, this one needs the existing bin. And
// unlike the "modify" methods, this can be called with a null bin pointer.
int32_t
as_bin_particle_size_modify_from_client(as_bin *b, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	uint8_t *op_value = as_msg_op_get_value_p((as_msg_op *)op);

	// Currently all operations become creates if there's no existing particle.
	if (! (b && as_bin_inuse(b))) {
		// Memcache increment is weird - manipulate to create integer.
		if (operation == AS_MSG_OP_MC_INCR) {
			op_type = AS_PARTICLE_TYPE_INTEGER;
		}

		return particle_vtable[op_type]->size_from_wire_fn(op_value, op_value_size);
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
	case AS_MSG_OP_INCR:
		// Currently only embedded types can be incremented.
		return 0;
	case AS_MSG_OP_MC_APPEND:
	case AS_MSG_OP_APPEND:
	case AS_MSG_OP_MC_PREPEND:
	case AS_MSG_OP_PREPEND:
		return particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}
}

int
as_bin_particle_alloc_modify_from_client(as_bin *b, const as_msg_op *op)
{
	// This method does not destroy the existing particle, if any. We assume
	// there is a copy of this bin (and particle reference) elsewhere, and that
	// the copy will be responsible for the existing particle. Therefore it's
	// important on failure to leave the existing particle intact.

	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	uint8_t *op_value = as_msg_op_get_value_p((as_msg_op *)op);

	// Currently all operations become creates if there's no existing particle.
	if (! as_bin_inuse(b)) {
		// Memcache increment is weird - manipulate to create integer.
		if (operation == AS_MSG_OP_MC_INCR) {
			if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			op_type = AS_PARTICLE_TYPE_INTEGER;
			op_value_size = sizeof(uint64_t);
			op_value += sizeof(uint64_t);
		}

		int32_t mem_size = particle_vtable[op_type]->size_from_wire_fn(op_value, op_value_size);

		if (mem_size < 0) {
			return (int)mem_size;
		}

		as_particle *old_particle = b->particle;

		if (mem_size != 0) {
			b->particle = cf_malloc_ns((size_t)mem_size);

			if (! b->particle) {
				b->particle = old_particle;
				return -AS_PROTO_RESULT_FAIL_UNKNOWN;
			}
		}

		// Load the new particle into the bin.
		int result = particle_vtable[op_type]->from_wire_fn(op_type, op_value, op_value_size, &b->particle);

		// Set the bin's iparticle metadata.
		if (result == 0) {
			as_bin_state_set_from_type(b, op_type);
		}
		else {
			if (mem_size != 0) {
				cf_free(b->particle);
			}

			b->particle = old_particle;
		}

		return result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;
	as_particle *new_particle = NULL;

	as_particle *old_particle = b->particle;
	int result = 0;

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
		if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
		op_type = AS_PARTICLE_TYPE_INTEGER;
		// op_value_size of 16 will flag operation as memcache increment...
		// no break
	case AS_MSG_OP_INCR:
		result = particle_vtable[existing_type]->incr_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_APPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_APPEND:
		new_mem_size = particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return new_mem_size;
		}
		if (! (new_particle = cf_malloc_ns((size_t)new_mem_size))) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(new_particle, b->particle, particle_vtable[existing_type]->size_fn(b->particle));
		b->particle = new_particle;
		result = particle_vtable[existing_type]->append_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_PREPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_PREPEND:
		new_mem_size = particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return new_mem_size;
		}
		if (! (new_particle = cf_malloc_ns((size_t)new_mem_size))) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(new_particle, b->particle, particle_vtable[existing_type]->size_fn(b->particle));
		b->particle = new_particle;
		result = particle_vtable[existing_type]->prepend_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (result < 0) {
		if (new_mem_size != 0) {
			cf_free(b->particle);
		}

		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_stack_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op)
{
	uint8_t operation = op->op;
	as_particle_type op_type = safe_particle_type(op->particle_type);

	if (op_type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t op_value_size = as_msg_op_get_value_sz(op);
	uint8_t *op_value = as_msg_op_get_value_p((as_msg_op *)op);

	// Currently all operations become creates if there's no existing particle.
	if (! as_bin_inuse(b)) {
		// Memcache increment is weird - manipulate to create integer.
		if (operation == AS_MSG_OP_MC_INCR) {
			if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
				return -AS_PROTO_RESULT_FAIL_PARAMETER;
			}

			op_type = AS_PARTICLE_TYPE_INTEGER;
			op_value_size = sizeof(uint64_t);
			op_value += sizeof(uint64_t);
		}

		int32_t mem_size = particle_vtable[op_type]->size_from_wire_fn(op_value, op_value_size);

		if (mem_size < 0) {
			return (int)mem_size;
		}

		as_particle *old_particle = b->particle;

		// Instead of allocating, we use the stack buffer provided. (Note that
		// embedded types like integer will overwrite this with the value.)
		if (0 > cf_ll_buf_reserve(particles_llb, (size_t)mem_size, (uint8_t **)&b->particle)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}

		// Load the new particle into the bin.
		int result = particle_vtable[op_type]->from_wire_fn(op_type, op_value, op_value_size, &b->particle);

		// Set the bin's iparticle metadata.
		if (result == 0) {
			as_bin_state_set_from_type(b, op_type);
		}
		else {
			b->particle = old_particle;
		}

		return result;
	}

	// There is an existing particle, which we will modify.
	uint8_t existing_type = as_bin_get_particle_type(b);
	int32_t new_mem_size = 0;

	as_particle *old_particle = b->particle;
	int result = 0;

	switch (operation) {
	case AS_MSG_OP_MC_INCR:
		if (op_value_size != 2 * sizeof(uint64_t) || op_type != AS_PARTICLE_TYPE_BLOB) {
			return -AS_PROTO_RESULT_FAIL_PARAMETER;
		}
		op_type = AS_PARTICLE_TYPE_INTEGER;
		// op_value_size of 16 will flag operation as memcache increment...
		// no break
	case AS_MSG_OP_INCR:
		result = particle_vtable[existing_type]->incr_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_APPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_APPEND:
		new_mem_size = particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return (int)new_mem_size;
		}
		if (0 > cf_ll_buf_reserve(particles_llb, (size_t)new_mem_size, (uint8_t **)&b->particle)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(b->particle, old_particle, particle_vtable[existing_type]->size_fn(old_particle));
		result = particle_vtable[existing_type]->append_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	case AS_MSG_OP_MC_PREPEND:
		if (existing_type != AS_PARTICLE_TYPE_STRING) {
			return -AS_PROTO_RESULT_FAIL_INCOMPATIBLE_TYPE;
		}
		// no break
	case AS_MSG_OP_PREPEND:
		new_mem_size = particle_vtable[existing_type]->concat_size_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		if (new_mem_size < 0) {
			return (int)new_mem_size;
		}
		if (0 > cf_ll_buf_reserve(particles_llb, (size_t)new_mem_size, (uint8_t **)&b->particle)) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
		memcpy(b->particle, old_particle, particle_vtable[existing_type]->size_fn(old_particle));
		result = particle_vtable[existing_type]->prepend_from_wire_fn(op_type, op_value, op_value_size, &b->particle);
		break;
	default:
		// TODO - just crash?
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (result < 0) {
		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_alloc_from_client(as_bin *b, const as_msg_op *op)
{
	// This method does not destroy the existing particle, if any. We assume
	// there is a copy of this bin (and particle reference) elsewhere, and that
	// the copy will be responsible for the existing particle. Therefore it's
	// important on failure to leave the existing particle intact.

	as_particle_type type = safe_particle_type(op->particle_type);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);
	int32_t mem_size = particle_vtable[type]->size_from_wire_fn(value, value_size);

	if (mem_size < 0) {
		return (int)mem_size;
	}

	as_particle *old_particle = b->particle;

	if (mem_size != 0) {
		b->particle = cf_malloc_ns((size_t)mem_size);

		if (! b->particle) {
			b->particle = old_particle;
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	// Load the new particle into the bin.
	int result = particle_vtable[type]->from_wire_fn(type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		if (mem_size != 0) {
			cf_free(b->particle);
		}

		b->particle = old_particle;
	}

	return result;
}

int
as_bin_particle_stack_from_client(as_bin *b, cf_ll_buf *particles_llb, const as_msg_op *op)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	as_particle_type type = safe_particle_type(op->particle_type);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_PARAMETER;
	}

	uint32_t value_size = as_msg_op_get_value_sz(op);
	uint8_t *value = as_msg_op_get_value_p((as_msg_op *)op);
	int32_t mem_size = particle_vtable[type]->size_from_wire_fn(value, value_size);

	if (mem_size < 0) {
		return (int)mem_size;
	}

	as_particle *old_particle = b->particle;

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	if (0 > cf_ll_buf_reserve(particles_llb, (size_t)mem_size, (uint8_t **)&b->particle)) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	// Load the new particle into the bin.
	int result = particle_vtable[type]->from_wire_fn(type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		b->particle = old_particle;
	}

	return result;
}

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_replace_from_pickled(as_bin *b, uint8_t **p_pickled)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	uint32_t old_mem_size = as_bin_inuse(b) ? particle_vtable[old_type]->size_fn(b->particle) : 0;

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type new_type = safe_particle_type(*pickled++);
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t new_value_size = cf_swap_from_be32(*p32++);
	const uint8_t *new_value = (const uint8_t *)p32;

	*p_pickled = (uint8_t *)new_value + new_value_size;

	if (new_type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	int32_t new_mem_size = particle_vtable[new_type]->size_from_wire_fn(new_value, new_value_size);

	if (new_mem_size < 0) {
		// Leave existing particle intact.
		return (int)new_mem_size;
	}

	if ((uint32_t)new_mem_size != old_mem_size) {
		if (as_bin_inuse(b)) {
			// Destroy the old particle.
			particle_vtable[old_type]->destructor_fn(b->particle);
		}

		b->particle = NULL;
	}

	if (new_mem_size != 0 && ! b->particle) {
		b->particle = cf_malloc_ns((size_t)new_mem_size);

		if (! b->particle) {
			as_bin_set_empty(b);
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	// Load the new particle into the bin.
	int result = particle_vtable[new_type]->from_wire_fn(new_type, new_value, new_value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, new_type);
	}
	else {
		if (as_bin_inuse(b)) {
			// Destroy the old particle.
			particle_vtable[old_type]->destructor_fn(b->particle);
		}

		b->particle = NULL;
		as_bin_set_empty(b);
	}

	return result;
}

// TODO - re-do to leave original intact on failure.
int32_t
as_bin_particle_stack_from_pickled(as_bin *b, uint8_t *stack, uint8_t **p_pickled)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type type = safe_particle_type(*pickled++);
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;

	*p_pickled = (uint8_t *)value + value_size;

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	int32_t mem_size = particle_vtable[type]->size_from_wire_fn(value, value_size);

	if (mem_size < 0) {
		// Leave existing particle intact.
		return mem_size;
	}

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	b->particle = (as_particle *)stack;

	// Load the new particle into the bin.
	int result = particle_vtable[type]->from_wire_fn(type, value, value_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		b->particle = NULL;
		as_bin_set_empty(b);
	}

	return result == 0 ? mem_size : (int32_t)result;
}

int
as_bin_particle_compare_from_pickled(const as_bin *b, uint8_t **p_pickled)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "comparing to unused bin");
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	const uint8_t *pickled = (const uint8_t *)*p_pickled;
	as_particle_type type = safe_particle_type(*pickled++);
	const uint32_t *p32 = (const uint32_t *)pickled;
	uint32_t value_size = cf_swap_from_be32(*p32++);
	const uint8_t *value = (const uint8_t *)p32;

	*p_pickled = (uint8_t *)value + value_size;

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	return particle_vtable[as_bin_get_particle_type(b)]->compare_from_wire_fn(b->particle, type, value, value_size);
}

uint32_t
as_bin_particle_client_value_size(const as_bin *b)
{
	if (! as_bin_inuse(b)) {
		// UDF result bin (bin name "SUCCESS" or "FAILURE") will get here.
		return 0;
	}

	if (as_bin_is_hidden(b)) {
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	return particle_vtable[type]->wire_size_fn(b->particle);
}

uint32_t
as_bin_particle_to_client(const as_bin *b, as_msg_op *op)
{
	if (! (b && as_bin_inuse(b))) {
		// UDF result bin (bin name "SUCCESS" or "FAILURE") will get here.
		// Ordered ops that find no bin will get here.
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	if (as_bin_is_hidden(b)) {
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	op->particle_type = type;

	uint8_t *value = (uint8_t *)op + sizeof(as_msg_op) + op->name_sz;
	uint32_t added_size = particle_vtable[type]->to_wire_fn(b->particle, value);

	op->op_sz += added_size;

	return added_size;
}

uint32_t
as_bin_particle_pickled_size(const as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	// Always a type byte and a 32-bit size.
	return 1 + 4 + particle_vtable[type]->wire_size_fn(b->particle);
}

uint32_t
as_bin_particle_to_pickled(const as_bin *b, uint8_t *pickled)
{
	uint8_t type = as_bin_get_particle_type(b);

	*pickled++ = type;

	uint32_t *p_size = (uint32_t *)pickled;
	uint8_t *value = (uint8_t *)(p_size + 1);
	uint32_t size = particle_vtable[type]->to_wire_fn(b->particle, value);

	*p_size = cf_swap_to_be32(size);

	return 1 + 4 + size;
}

//------------------------------------------------
// Handle as_val translation.
//

int
as_bin_particle_replace_from_asval(as_bin *b, const as_val *val)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	as_particle_type new_type = as_particle_type_from_asval(val);

	if (new_type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just skips unmanageable as_val types.
		return 0;
	}

	uint32_t new_mem_size = particle_vtable[new_type]->size_from_asval_fn(val);
	// TODO - could this ever fail?

	as_particle *old_particle = b->particle;

	if (new_mem_size != 0) {
		b->particle = cf_malloc_ns(new_mem_size);

		if (! b->particle) {
			b->particle = old_particle;
			return -1;
		}
	}

	// Load the new particle into the bin.
	particle_vtable[new_type]->from_asval_fn(val, &b->particle);
	// TODO - could this ever fail?

	if (as_bin_inuse(b)) {
		// Destroy the old particle.
		particle_vtable[old_type]->destructor_fn(old_particle);
	}

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, new_type);

	return 0;
}

void
as_bin_particle_stack_from_asval(as_bin *b, uint8_t* stack, const as_val *val)
{
	// We assume that if we're using stack particles, the old particle is either
	// nonexistent or also a stack particle - either way, don't destroy.

	as_particle_type type = as_particle_type_from_asval(val);

	if (type == AS_PARTICLE_TYPE_NULL) {
		// Currently UDF code just skips unmanageable as_val types.
		return;
	}

	// Instead of allocating, we use the stack buffer provided. (Note that
	// embedded types like integer will overwrite this with the value.)
	b->particle = (as_particle *)stack;

	// Load the new particle into the bin.
	particle_vtable[type]->from_asval_fn(val, &b->particle);
	// TODO - could this ever fail?

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, type);

	// TODO - we don't bother returning size written, since nothing yet needs
	// it and it's very expensive for CDTs to do an extra size_from_asval_fn()
	// call. Perhaps we could have from_asval_fn() return the size if needed?
}

as_val *
as_bin_particle_to_asval(const as_bin *b)
{
	uint8_t type = as_bin_get_particle_type(b);

	// Caller is responsible for freeing as_val returned here.
	return particle_vtable[type]->to_asval_fn(b->particle);
}

//------------------------------------------------
// Handle msgpack translation.
//

int
as_bin_particle_alloc_from_msgpack(as_bin *b, const uint8_t *packed, uint32_t packed_size)
{
	// We assume the bin is empty.

	as_particle_type type = as_particle_type_from_msgpack(packed, packed_size);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -AS_PROTO_RESULT_FAIL_UNKNOWN;
	}

	if (type == AS_PARTICLE_TYPE_NULL) {
		return 0;
	}

	uint32_t mem_size = particle_vtable[type]->size_from_msgpack_fn(packed, packed_size);

	if (mem_size != 0) {
		b->particle = cf_malloc(mem_size); // response, so not cf_malloc_ns()

		if (! b->particle) {
			return -AS_PROTO_RESULT_FAIL_UNKNOWN;
		}
	}

	particle_vtable[type]->from_msgpack_fn(packed, packed_size, &b->particle);

	// Set the bin's iparticle metadata.
	as_bin_state_set_from_type(b, type);

	return 0;
}

//------------------------------------------------
// Handle on-device "flat" format.
//

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_cast_from_flat(as_bin *b, uint8_t *flat, uint32_t flat_size)
{
	if (as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "cast from flat into used bin");
		return -1;
	}

	as_particle_type type = safe_particle_type(*flat);

	if (type == AS_PARTICLE_TYPE_BAD) {
		return -1;
	}

	// Cast the new particle into the bin.
	int result = particle_vtable[type]->cast_from_flat_fn(flat, flat_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, type);
	}
	else {
		as_bin_set_empty(b);
	}

	return result;
}

// TODO - re-do to leave original intact on failure.
int
as_bin_particle_replace_from_flat(as_bin *b, const uint8_t *flat, uint32_t flat_size)
{
	uint8_t old_type = as_bin_get_particle_type(b);
	as_particle_type new_type = safe_particle_type(*flat);

	if (new_type == AS_PARTICLE_TYPE_BAD) {
		return -1;
	}

	// Just destroy the old particle, if any - we're replacing it.
	if (as_bin_inuse(b)) {
		particle_vtable[old_type]->destructor_fn(b->particle);
	}

	// Load the new particle into the bin.
	int result = particle_vtable[new_type]->from_flat_fn(flat, flat_size, &b->particle);

	// Set the bin's iparticle metadata.
	if (result == 0) {
		as_bin_state_set_from_type(b, new_type);
	}
	else {
		as_bin_set_empty(b);
	}

	return result;
}

uint32_t
as_bin_particle_flat_size(as_bin *b)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "flat sizing unused bin");
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	return particle_vtable[type]->flat_size_fn(b->particle);
}

uint32_t
as_bin_particle_to_flat(const as_bin *b, uint8_t *flat)
{
	if (! as_bin_inuse(b)) {
		// TODO - just crash?
		cf_warning(AS_PARTICLE, "flattening unused bin");
		return 0;
	}

	uint8_t type = as_bin_get_particle_type(b);

	*flat = type;

	return particle_vtable[type]->to_flat_fn(b->particle, flat);
}


//==========================================================
// Functions specific to LDTs.
//

//------------------------------------------------
// Handle "wire" format.
//

uint32_t
as_ldt_particle_client_value_size(as_storage_rd *rd, as_bin *b, as_val **p_val)
{
	*p_val = as_llist_scan(rd->ns, rd->ns->partitions[as_partition_getid(&rd->r->keyd)].sub_vp, rd, b);

	if (! *p_val) {
		return 0;
	}

	as_serializer s;
	as_msgpack_init(&s);

	uint32_t added_size = as_serializer_serialize_getsize(&s, *p_val);

	as_serializer_destroy(&s);

	return added_size;
}

uint32_t
as_ldt_particle_to_client(as_val *val, as_msg_op *op)
{
	if (! val) {
		op->particle_type = AS_PARTICLE_TYPE_NULL;
		return 0;
	}

	op->particle_type = AS_PARTICLE_TYPE_HIDDEN_LIST;

	uint8_t *value = (uint8_t *)op + sizeof(as_msg_op) + op->name_sz;

	as_serializer s;
	as_msgpack_init(&s);

	uint32_t added_size = as_serializer_serialize_presized(&s, val, value);

	as_serializer_destroy(&s);
	as_val_destroy(val);

	op->op_sz += added_size;

	return added_size;
}


//==========================================================
// as_bin particle functions specific to CDTs.
//

//------------------------------------------------
// Handle "wire" format.
//

int
as_bin_cdt_read_from_client(const as_bin *b, as_msg_op *op, as_bin *result)
{
	return as_bin_cdt_packed_read(b, op, result);
}

int
as_bin_cdt_alloc_modify_from_client(as_bin *b, as_msg_op *op, as_bin *result)
{
	return as_bin_cdt_packed_modify(b, op, result, NULL);
}

int
as_bin_cdt_stack_modify_from_client(as_bin *b, cf_ll_buf *particles_llb, as_msg_op *op, as_bin *result)
{
	return as_bin_cdt_packed_modify(b, op, result, particles_llb);
}
