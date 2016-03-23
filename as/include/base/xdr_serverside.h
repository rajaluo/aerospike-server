/*
 * xdr_serverside.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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

/*
 * runtime structures and definitions for serverside of XDR
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"

#include "dynbuf.h"
#include "util.h"
#include "xdr_config.h"

#include "base/datamodel.h"
#include "base/transaction.h"

int as_xdr_init();
void xdr_conf_init(const char *config_file);
void as_xdr_start();
int as_xdr_shutdown();
void xdr_sig_handler(int signum);

void xdr_broadcast_lastshipinfo(uint64_t val[]);
void xdr_clmap_update(int changetype, cf_node succession[], int listsize);
void xdr_write(as_namespace *ns, cf_digest keyd, as_generation generation, cf_node masternode, bool is_delete, uint16_t set_id);
void as_xdr_handle_txn(as_transaction *txn);

void as_xdr_info_init(void);
int32_t as_xdr_info_port(void);
int as_info_command_xdr(char *name, char *params, cf_dyn_buf *db);
void xdr_handle_failednodeprocessingdone(cf_node);
void as_xdr_get_stats(char *name, cf_dyn_buf *db);
void as_xdr_get_config(cf_dyn_buf *db);
void as_xdr_set_config(char *params, cf_dyn_buf *db);
int32_t as_xdr_set_config_ns(char *ns_name, char *params);

bool is_xdr_delete_shipping_enabled();
bool is_xdr_nsup_deletes_enabled();
bool is_xdr_forwarding_enabled();
