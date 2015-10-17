/*
 * particle_integer.h
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

#pragma once

#include <stdint.h>
#include "base/datamodel.h"

// The INTEGER particle interface function declarations are in this header file
// since INTEGER functions are used by other particles derived from INTEGER.

// Destructor, etc.
void integer_destruct(as_particle *p);
uint32_t integer_size(const as_particle *p);
// Note - no 'ptr' function - it's irrelevant.

// Handle "wire" format.
int32_t integer_concat_size_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int integer_append_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int integer_prepend_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int integer_incr_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int32_t integer_size_from_wire(const uint8_t *wire_value, uint32_t value_size);
int integer_from_wire(as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size, as_particle **pp);
int integer_compare_from_wire(const as_particle *p, as_particle_type wire_type, const uint8_t *wire_value, uint32_t value_size);
uint32_t integer_wire_size(const as_particle *p);
uint32_t integer_to_wire(const as_particle *p, uint8_t *wire);

// Handle in-memory format.
uint32_t integer_size_from_mem(as_particle_type type, const uint8_t *value, uint32_t value_size);
void integer_from_mem(as_particle_type type, const uint8_t *mem_value, uint32_t value_size, as_particle **pp);
uint32_t integer_mem_size(const as_particle *p);
uint32_t integer_to_mem(const as_particle *p, uint8_t *value);

// Handle on-device "flat" format.
int32_t integer_size_from_flat(const uint8_t *flat, uint32_t flat_size);
int integer_cast_from_flat(uint8_t *flat, uint32_t flat_size, as_particle **pp);
int integer_from_flat(const uint8_t *flat, uint32_t flat_size, as_particle **pp);
uint32_t integer_flat_size(const as_particle *p);
uint32_t integer_to_flat(const as_particle *p, uint8_t *flat);
