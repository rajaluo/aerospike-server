/*
 * security.h
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

#pragma once

#include <stdint.h>
#include "base/transaction.h"


//==========================================================
// Typedefs & constants.
//

// Security privileges.
typedef enum {
	// Data operations.
	PRIV_READ			= 0x0001,
	PRIV_SCAN			= 0x0002,
	PRIV_QUERY			= 0x0004,
	PRIV_WRITE			= 0x0008,
	PRIV_UDF_APPLY		= 0x0010,
	PRIV_UDF_SCAN		= 0x0020,
	PRIV_UDF_QUERY		= 0x0040,
	PRIV_INDEX_MANAGE	= 0x0080,
	PRIV_UDF_MANAGE		= 0x0100,
	// ... 7 unused bits ...

	// Deployment management operations.
	PRIV_GET_STATS		= 0x00010000,
	PRIV_GET_CONFIG		= 0x00020000,
	PRIV_SET_CONFIG		= 0x00040000,

	// Database users and permissions management.
	PRIV_USER_ADMIN		= 0x10000000
} as_sec_priv;

// Current security message version.
#define AS_SEC_MSG_SCHEME 0

// Security protocol message container.
typedef struct as_sec_msg_s {
	uint8_t		scheme;		// security scheme/version
	uint8_t		result;		// result code (only for responses, except MORE)
	uint8_t		command;	// security command (only for requests)
	uint8_t		n_fields;	// number of fields in this message

	uint8_t		unused[12];	// reserved bytes round as_sec_msg size to 16 bytes

	uint8_t		fields[];	// the fields (name/value pairs)
} __attribute__ ((__packed__)) as_sec_msg;


//==========================================================
// Public API.
//

uint8_t as_security_allowed(as_sec_priv operation, const as_file_handle* fd_h);
void* as_security_filter_create();
void as_security_filter_destroy(void* pv_filter);
void as_security_refresh(as_file_handle* fd_h);
void as_security_transact(as_transaction *tr);
