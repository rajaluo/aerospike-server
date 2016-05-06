/*
 * xdr_serverside_stubs.c
 *
 * Copyright (C) 2014-2016 Aerospike, Inc.
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

#include "base/xdr_serverside.h"

xdr_state g_xdr_state = XDR_DOWN;

int as_xdr_init()
{
	return -1;
}

void xdr_conf_init(const char *config_file)
{
}

void as_xdr_start()
{
}

int as_xdr_shutdown()
{
	return -1;
}

void xdr_sig_handler(int signum)
{
}

void xdr_broadcast_lastshipinfo(uint64_t val[])
{
}

void xdr_clmap_update(int changetype, cf_node succession[], int listsize)
{
}

void xdr_clear_dirty_bins(xdr_dirty_bins *dirty)
{
}

void xdr_fill_dirty_bins(xdr_dirty_bins *dirty)
{
}

void xdr_copy_dirty_bins(xdr_dirty_bins *from, xdr_dirty_bins *to)
{
}

void xdr_add_dirty_bin(as_namespace *ns, xdr_dirty_bins *dirty, const char *name, size_t name_len)
{
}

void xdr_write(as_namespace *ns, cf_digest keyd, as_generation generation, cf_node masternode, bool is_delete, uint16_t set_id, xdr_dirty_bins *dirty)
{
}

void as_xdr_handle_txn(as_transaction *txn)
{
}

void as_xdr_info_init(void)
{
}

int32_t as_xdr_info_port(void)
{
	return 0;
}

int as_info_command_xdr(char *name, char *params, cf_dyn_buf *db)
{
	return -1;
}

void xdr_handle_failednodeprocessingdone(cf_node nodeid)
{
}

void as_xdr_get_stats(char *name, cf_dyn_buf *db)
{
	// No XDR stats. Remove the last ';' that will be added by the caller
	cf_dyn_buf_chomp(db);
}

void as_xdr_get_config(cf_dyn_buf *db)
{
	// No XDR config. Remove the last ';' that will be added by the caller
	cf_dyn_buf_chomp(db);
}

void as_xdr_set_config(char *params, cf_dyn_buf *db)
{
	cf_dyn_buf_append_string(db, "error");
}

int32_t as_xdr_set_config_ns(char *ns_name, char *params)
{
	return -1;
}

bool is_xdr_delete_shipping_enabled()
{
	return false;
}

bool is_xdr_nsup_deletes_enabled()
{
	return false;
}

bool is_xdr_forwarding_enabled()
{
	return false;
}
