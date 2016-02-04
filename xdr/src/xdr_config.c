/*
 * xdr_config.c
 *
 * Copyright (C) 2011-2014 Aerospike, Inc.
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
 *  Configuration file-related routines shared between the server and XDR.
 */

#include "xdr_config.h"

const xdr_cfg_opt XDR_GLOBAL_OPTS[] = {
		{ "namespace",						XDR_CASE_NAMESPACE_BEGIN },
		{ "xdr",							XDR_CASE_XDR_BEGIN }
};

const xdr_cfg_opt XDR_NS_OPTS[] = {
		{ "enable-xdr",						XDR_CASE_NS_ENABLE_XDR },
		{ "xdr-remote-datacenter",			XDR_CASE_NS_XDR_REMOTE_DATACENTER },
		{ "default-ttl",					XDR_CASE_NS_DEFAULT_TTL },
		{ "max-ttl",						XDR_CASE_NS_MAX_TTL },
		{ "storage-engine",					XDR_CASE_NS_STORAGE_ENGINE_BEGIN },
		{ "set",							XDR_CASE_NS_SET_BEGIN },
		{ "}",								XDR_CASE_CONTEXT_END }
};

const xdr_cfg_opt XDR_NS_STORAGE_OPTS[] = {
		{ "memory",							XDR_CASE_NS_STORAGE_MEMORY },
		{ "ssd",							XDR_CASE_NS_STORAGE_SSD },
		{ "device",							XDR_CASE_NS_STORAGE_DEVICE },
		{ "kv",								XDR_CASE_NS_STORAGE_KV },
		{ "}",								XDR_CASE_CONTEXT_END }
};

const xdr_cfg_opt XDR_NS_SET_OPTS[] = {
		{ "}",								XDR_CASE_CONTEXT_END }	// only thing that is of interest for
																	// XDR to skip over the set subsection
};

const xdr_cfg_opt XDR_OPTS[] = {
		{ "{",								XDR_CASE_CONTEXT_BEGIN },
		{ "enable-xdr",						XDR_CASE_ENABLE_XDR },
		{ "xdr-namedpipe-path",				XDR_CASE_NAMEDPIPE_PATH },
		{ "xdr-digestlog-path",				XDR_CASE_DIGESTLOG_PATH },
		{ "xdr-errorlog-path",				XDR_CASE_ERRORLOG_PATH },
		{ "xdr-info-port",					XDR_CASE_INFO_PORT },
		{ "datacenter",						XDR_CASE_DATACENTER_BEGIN },
		{ "xdr-max-recs-inflight",			XDR_CASE_MAX_RECS_INFLIGHT },
		{ "forward-xdr-writes",				XDR_CASE_FORWARD_XDR_WRITES },
		{ "xdr-threads",					XDR_CASE_THREADS },
		{ "timeout",						XDR_CASE_TIMEOUT }, // not exposed to users
		{ "stop-writes-noxdr",				XDR_CASE_STOP_WRITES_NOXDR },
		{ "enable-xdr-delete-shipping",		XDR_CASE_XDR_DELETE_SHIPPING_ENABLED },
		{ "xdr-forward-with-gencheck",		XDR_CASE_XDR_FORWARD_WITH_GENCHECK },
		{ "xdr-replace-record",				XDR_CASE_XDR_REPLACE_RECORD },
		{ "xdr-hotkey-maxskip",				XDR_CASE_XDR_HOTKEY_MAXSKIP },
		{ "xdr-nsup-deletes-enabled",		XDR_CASE_XDR_NSUP_DELETES_ENABLED },
		{ "xdr-shipping-enabled",			XDR_CASE_XDR_SHIPPING_ENABLED },
		{ "xdr-info-timeout",				XDR_CASE_XDR_INFO_TIMEOUT },
		{ "xdr-compression-threshold",		XDR_CASE_XDR_COMPRESSION_THRESHOLD },
		{ "xdr-read-batch-size",			XDR_CASE_XDR_READ_BATCH_SIZE },
		{ "xdr-ship-delay",					XDR_CASE_XDR_SHIP_DELAY },
		{ "xdr-check-data-before-delete",	XDR_CASE_XDR_CHECK_DATA_BEFORE_DELETE },
		{ "xdr-pidfile",					XDR_CASE_XDR_PIDFILE },
		{ "xdr-ship-threads",				XDR_CASE_XDR_SHIP_THREADS}, // xdr in asd
		{ "xdr-ship-slab-size",				XDR_CASE_XDR_SHIP_SLAB_SIZE}, // xdr in asd
		{ "}",								XDR_CASE_CONTEXT_END }
};

const xdr_cfg_opt XDR_DC_OPTS[] = {
		{ "{",								XDR_CASE_CONTEXT_BEGIN },
		{ "dc-node-address-port",			XDR_CASE_DC_NODE_ADDRESS_PORT },
		{ "dc-int-ext-ipmap",				XDR_CASE_DC_INT_EXT_IPMAP },
		{ "dc-security-config-file",		XDR_CASE_DC_SECURITY_CONFIG_FILE },
		{ "dc-use-alternate-services",		XDR_CASE_DC_USE_ALTERNATE_SERVICES },
		{ "}",								XDR_CASE_CONTEXT_END }
};

const int NUM_XDR_GLOBAL_OPTS		= sizeof(XDR_GLOBAL_OPTS) / sizeof(xdr_cfg_opt);
const int NUM_XDR_NS_OPTS			= sizeof(XDR_NS_OPTS) / sizeof(xdr_cfg_opt);
const int NUM_XDR_NS_STORAGE_OPTS	= sizeof(XDR_NS_STORAGE_OPTS) / sizeof(xdr_cfg_opt);
const int NUM_XDR_NS_SET_OPTS		= sizeof(XDR_NS_SET_OPTS) / sizeof(xdr_cfg_opt);
const int NUM_XDR_OPTS				= sizeof(XDR_OPTS) / sizeof(xdr_cfg_opt);
const int NUM_XDR_DC_OPTS			= sizeof(XDR_DC_OPTS) / sizeof(xdr_cfg_opt);

// Security related configs
const xdr_cfg_opt XDR_SEC_GLOBAL_OPTS[] = {
		{ "credentials",					XDR_CASE_SEC_CREDENTIALS_BEGIN }
};

const xdr_cfg_opt XDR_SEC_CRED_OPTS[] = {
		{ "{",								XDR_CASE_CONTEXT_BEGIN },
		{ "username",						XDR_CASE_SEC_CRED_USERNAME },
		{ "password",						XDR_CASE_SEC_CRED_PASSWORD },
		{ "}",								XDR_CASE_CONTEXT_END }
};

const int NUM_XDR_SEC_GLOBAL_OPTS	= sizeof(XDR_SEC_GLOBAL_OPTS) / sizeof(xdr_cfg_opt);
const int NUM_XDR_SEC_CRED_OPTS		= sizeof(XDR_SEC_CRED_OPTS) / sizeof(xdr_cfg_opt);

// N.B.:  Default to false.
const bool g_xdr_supported;

void xdr_config_defaults(xdr_config *c)
{
	int index;

	c->xdr_global_enabled = false;	//This config option overrides the enable-xdr setting of the namespace(s)
	c->xdr_digestpipe_path = NULL;	//The user has to specify a named pipe used to communicate the digests
	c->xdr_digestpipe_readfd = -1;	//Once the named pipe reader is open, the file descriptor will be stored here
	c->xdr_digestpipe_writefd = -1;	//Once the named pipe writer is open, the file descriptor will be stored here

	for (index = 0; index < XDR_MAX_DGLOG_FILES ; index++) {
		c->xdr_digestlog_path[index] = NULL;	//Path where the digest information is written to the disk
	}
	c->xdr_num_digestlog_paths = 0; //Number of rlog files 0 is default
	c->xdr_digestlog_overwrite = true;
	c->xdr_digestlog_persist = true;
	c->xdr_info_port = 0;

	c->xdr_local_port = 0;		//Port of the remote node
	c->xdr_max_recs_inflight = 0; // Max number of digests shipped that can be in the async queue at any given point
								  // The default will be determined based on remote DC's pipelining capabilties
	c->xdr_read_batch_size = 500;   // Number of digests read from the digest log and processed in one go
	c->xdr_ship_slab_size = 500;    // Number of digests processed by one shipper thread.
	c->xdr_ship_threads = 8;        // Number of XDR shipper threads.
	c->xdr_timeout = 30000;		// Timeout for each element that is shipped. default is 30000 ms
								// asd side connection times out at 15 seconds
	c->xdr_threads = 3;		//Number of receiver threads to spawn
	c->xdr_forward_xdrwrites = false;	//If the writes due to xdr should be forwarded
	c->xdr_nsup_deletes_enabled = false;		// Shall XDR ship deletes of evictions or expiration
	c->xdr_stop_writes_noxdr = false;	//If the normal writes should be stopped if there is no xdr
	c->xdr_internal_shipping_delay = 0; //Default sleep between shipping each batch is 0 second
	c->xdr_conf_change_flag = 0;
	c->xdr_shipping_enabled = true;
	c->xdr_delete_shipping_enabled = true;
	c->xdr_check_data_before_delete = false;
	c->xdr_hotkey_maxskip = 5;
	c->xdr_fwd_with_gencheck = false;
	c->xdr_replace_record = true;
	c->xdr_info_request_timeout_ms = 500;
	c->xdr_compression_threshold = 0; //0 = Disabled compressed shipping, > 0 minimum size of packet for compression
	c->xdr_pidfile = NULL;
}
