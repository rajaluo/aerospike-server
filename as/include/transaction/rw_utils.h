/*
 * rw_utils.h
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

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "msg.h"

#include "base/datamodel.h"
#include "base/transaction.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"


//==========================================================
// Public API.
//

void send_rw_messages(rw_request* rw);
int set_set_from_msg(as_record* r, as_namespace* ns, as_msg* m);
bool check_msg_key(as_msg* m, as_storage_rd* rd);
bool get_msg_key(as_transaction* tr, as_storage_rd* rd);
void update_metadata_in_index(as_transaction* tr, bool increment_generation, as_record* r);
bool pickle_all(as_storage_rd* rd, rw_request* rw);
void delete_adjust_sindex(as_storage_rd* rd);


static inline void
destroy_stack_bins(as_bin* stack_bins, uint32_t n_bins)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		as_bin_particle_destroy(&stack_bins[i], true);
	}
}


// Not a nice way to specify a read-all op - dictated by backward compatibility.
// Note - must check this before checking for normal read op!
static inline bool
op_is_read_all(as_msg_op* op, as_msg* m)
{
	return op->name_sz == 0 && op->op == AS_MSG_OP_READ &&
			(m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
}


static inline bool
is_valid_ttl(as_namespace* ns, uint32_t ttl)
{
	// Note - TTL 0 means "use namespace default", -1 means "never expire".
	return ttl <= ns->max_ttl || ttl == 0xFFFFffff;
}
