/* migrate_ce.c
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


//==========================================================
// Includes.
//

#include "fabric/migrate.h"

#include <stdbool.h>
#include <stddef.h>

#include "fault.h"
#include "msg.h"
#include "util.h"

#include "base/datamodel.h"
#include "fabric/fabric.h"


//==========================================================
// Constants.
//

const uint32_t MY_MIG_FEATURES = 0;


//==========================================================
// Community Edition API.
//

bool
should_emigrate_record(emigration *emig, as_index_ref *r_ref)
{
	return true;
}

emig_meta_q *
emig_meta_q_create()
{
	return NULL;
}

void
emig_meta_q_destroy(emig_meta_q *emq)
{
}

void
emig_meta_q_push_batch(emig_meta_q *emq, const meta_batch *batch)
{
}

void
emigration_handle_meta_batch_request(cf_node src, msg *m)
{
	cf_warning(AS_MIGRATE, "CE node received meta-batch request - unexpected");
	as_fabric_msg_put(m);
}

void
immigration_handle_meta_batch_ack(cf_node src, msg *m)
{
	cf_warning(AS_MIGRATE, "CE node received meta-batch ack - unexpected");
	as_fabric_msg_put(m);
}

bool
immigration_start_meta_sender(immigration *immig, uint32_t emig_features,
		uint32_t emig_partition_sz)
{
	return false;
}

void
immig_meta_q_init(immig_meta_q *imq)
{
}

void
immig_meta_q_destroy(immig_meta_q *imq)
{
}
