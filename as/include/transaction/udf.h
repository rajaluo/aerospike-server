/*
 * udf.h
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

#include <stdint.h>

#include "aerospike/as_aerospike.h"

#include "base/proto.h"
#include "base/transaction.h"


//==========================================================
// Typedefs and constants.
//

typedef enum {
	UDF_OPTYPE_NONE,
	UDF_OPTYPE_READ,
	UDF_OPTYPE_WRITE,
	UDF_OPTYPE_DELETE,
	UDF_OPTYPE_LDT_READ,
	UDF_OPTYPE_LDT_WRITE,
	UDF_OPTYPE_LDT_DELETE
} udf_optype;

#define UDF_OP_IS_DELETE(op) \
	(((op) == UDF_OPTYPE_DELETE) || ((op) == UDF_OPTYPE_LDT_DELETE))

#define UDF_OP_IS_READ(op) \
	(((op) == UDF_OPTYPE_READ) || ((op) == UDF_OPTYPE_LDT_READ))

#define UDF_OP_IS_WRITE(op) \
	(((op) == UDF_OPTYPE_WRITE) || ((op) == UDF_OPTYPE_LDT_WRITE))

#define UDF_OP_IS_LDT(op) \
	(((op) == UDF_OPTYPE_LDT_READ) || \
			((op) == UDF_OPTYPE_LDT_WRITE) || \
			((op) == UDF_OPTYPE_LDT_DELETE))

#define UDF_MAX_STRING_SZ 128

typedef struct udf_def_s {
	char			filename[UDF_MAX_STRING_SZ];
	char			function[UDF_MAX_STRING_SZ];
	as_msg_field*	arglist;
	uint8_t			type;
} udf_def;

typedef int (*iudf_cb)(void* udata, int retcode);

typedef struct iudf_origin_s {
	udf_def	def;
	iudf_cb	cb;
	void*	udata;
} iudf_origin;


//==========================================================
// Public API.
//

void as_udf_init();
udf_def* udf_def_init_from_msg(udf_def* def, const as_transaction* tr);

transaction_status as_udf_start(as_transaction* tr);

extern as_aerospike g_as_aerospike;
