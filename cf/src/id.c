/*
 * id.c
 *
 * Copyright (C) 2008-2014 Aerospike, Inc.
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

#include "util.h" // we don't have our own header file

#include <errno.h>
#include <ifaddrs.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_types.h>
#include <citrusleaf/alloc.h>

#include "fault.h"

/*
** need a spot for this
*/

cf_digest cf_digest_zero = { { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 } };

/*
** nodeids are great things to use as keys in the hash table
*/

uint32_t
cf_nodeid_shash_fn(void *value)
{
	uint32_t *b = value;
	uint32_t acc = 0;
	for (int i=0;i<sizeof(cf_node);i++) {
		acc += *b;
	}
	return(acc);
}

uint32_t
cf_nodeid_rchash_fn(void *value, uint32_t value_len)
{
	uint32_t *b = value;
	uint32_t acc = 0;
	for (int i=0;i<sizeof(cf_node);i++) {
		acc += *b;
	}
	return(acc);
}
