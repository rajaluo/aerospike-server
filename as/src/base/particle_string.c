/*
 * particle_string.c
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

#include "aerospike/as_string.h"
#include "aerospike/as_val.h"

#include "fault.h"

#include "base/datamodel.h"
#include "base/particle.h"
#include "base/particle_blob.h"


//==========================================================
// STRING particle interface - function declarations.
//

// Most STRING particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

// Handle as_val translation.
as_val *string_to_asval(const as_particle *p);


//==========================================================
// STRING particle interface - vtable.
//

const as_particle_vtable string_vtable = {
		blob_destruct,
		blob_size,
		blob_ptr,

		blob_concat_size_from_wire,
		blob_append_from_wire,
		blob_prepend_from_wire,
		blob_incr_from_wire,
		blob_size_from_wire,
		blob_from_wire,
		blob_compare_from_wire,
		blob_wire_size,
		blob_to_wire,

		blob_size_from_mem,
		blob_from_mem,
		blob_mem_size,
		blob_to_mem,

		string_to_asval,

		blob_size_from_flat,
		blob_cast_from_flat,
		blob_from_flat,
		blob_flat_size,
		blob_to_flat
};


//==========================================================
// Typedefs & constants.
//

// Same as related BLOB struct. TODO - just expose BLOB structs?

typedef struct string_mem_s {
	uint8_t		type;
	uint32_t	sz;
	uint8_t		data[];
} __attribute__ ((__packed__)) string_mem;


//==========================================================
// STRING particle interface - function definitions.
//

// Most STRING particle table functions just use the equivalent BLOB particle
// functions. Here are the differences...

//------------------------------------------------
// Handle as_val translation.
//

as_val *
string_to_asval(const as_particle *p)
{
	string_mem *p_string_mem = (string_mem *)p;

	uint8_t *value = cf_malloc(p_string_mem->sz + 1);

	if (! value) {
		return NULL;
	}

	memcpy(value, p_string_mem->data, p_string_mem->sz);
	value[p_string_mem->sz] = 0;

	return (as_val *)as_string_new_wlen((char *)value, p_string_mem->sz, true);
}
