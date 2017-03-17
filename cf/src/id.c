/*
 * id.c
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

#include "fault.h"
#include "util.h" // We don't have our own header file.

#include "citrusleaf/alloc.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

/*
** This has nowhere else to go.
*/

const cf_digest cf_digest_zero = { .digest = { 0 } };

/*
** Node IDs are great things to use as keys in the hash table.
*/

uint32_t
cf_nodeid_shash_fn(const void *value)
{
	cf_node id = *(const cf_node *)value;
	return (uint32_t)(id >> 32) | (uint32_t)id;
}

uint32_t
cf_nodeid_rchash_fn(const void *value, uint32_t len)
{
	(void)len;
	return cf_nodeid_shash_fn(value);
}

char *
cf_node_name(void)
{
	char buffer[1000];
	int32_t res = gethostname(buffer, sizeof(buffer));

	if (res == sizeof(buffer) || (res < 0 && errno == ENAMETOOLONG)) {
		cf_crash(CF_MISC, "Host name too long");
	}

	if (res < 0) {
		cf_warning(CF_MISC, "Error while determining host name: %d (%s)",
				errno, cf_strerror(errno));
		buffer[0] = 0;
	}

	return cf_strdup(buffer);
}
