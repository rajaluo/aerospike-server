/*
 * util.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include <stddef.h>
#include <stdint.h>

// Position of most significant bit, 0 ... 63 from low to high. -1 for value 0.
static inline int
cf_msb(uint64_t value)
{
	int n = -1;

	while (value != 0) {
		value >>= 1;
		n++;
	}

	return n;
}

/* cf_hash_fnv
 * The 64-bit Fowler-Noll-Vo hash function (FNV-1a) */
static inline uint64_t
cf_hash_fnv(const void *buf, size_t bufsz)
{
    uint64_t hash = 0xcbf29ce484222325ULL;
    const uint8_t *bufp = (const uint8_t *) buf;
    const uint8_t *bufe = bufp + bufsz;

    while (bufp < bufe) {
        /* XOR the current byte into the bottom of the hash */
        hash ^= (uint64_t)*bufp++;

        /* Multiply by the 64-bit FNV magic prime */
        hash *= 0x100000001b3ULL;
    }

    return(hash);
}

/* cf_hash_oneatatime
 * The 64-bit One-at-a-Time hash function */
static inline uint64_t
cf_hash_oneatatime(void *buf, size_t bufsz)
{
    size_t i;
    uint64_t hash = 0;
    uint8_t *b = (uint8_t *)buf;

    for (i = 0; i < bufsz; i++) {
        hash += b[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);

    return(hash);
}
