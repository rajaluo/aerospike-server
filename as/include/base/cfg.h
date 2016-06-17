/*
 * cfg.h
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

/*
 * configuration structure
 */

#pragma once

#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <stdbool.h>
#include <stdint.h>

#include "xdr_config.h"

#include "aerospike/mod_lua_config.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_queue.h"

#include "hist.h"
#include "hist_track.h"
#include "olock.h"
#include "socket.h"
#include "util.h"

#include "base/cluster_config.h"
#include "base/datamodel.h"
#include "base/security_config.h"
#include "base/system_metadata.h"
#include "fabric/paxos.h"


#define MAX_TRANSACTION_QUEUES 128
#define MAX_DEMARSHAL_THREADS  256	// maximum number of demarshal worker threads
#define MAX_FABRIC_WORKERS 128		// maximum fabric worker threads
#define MAX_BATCH_THREADS 64		// maximum batch worker threads

struct as_namespace_s;


typedef struct as_config_s {

	//--------------------------------------------
	// service context.
	//

	uid_t				uid;
	gid_t				gid;
	uint32_t			paxos_single_replica_limit; // cluster size at which, and below, the cluster will run with replication factor 1
	char*				pidfile;
	int					n_service_threads;
	int					n_transaction_queues;
	int					n_transaction_threads_per_queue;
	int					n_proto_fd_max;

	bool				svc_benchmarks_active;
	bool				info_hist_active;
	bool				allow_inline_transactions;
	int					n_batch_threads;
	uint32_t			batch_max_buffers_per_queue; // maximum number of buffers allowed in a buffer queue at any one time, fail batch if full
	uint32_t			batch_max_requests; // maximum count of database requests in a single batch
	uint32_t			batch_max_unused_buffers; // maximum number of buffers allowed in buffer pool at any one time
	uint32_t			batch_priority; // number of records between an enforced context switch, used by old batch only
	int					n_batch_index_threads;
	int					n_fabric_workers;
	bool				generation_disable;
	uint32_t			hist_track_back; // total time span in seconds over which to cache data
	uint32_t			hist_track_slice; // period in seconds at which to cache histogram data
	char*				hist_track_thresholds; // comma-separated bucket (ms) values to track
	int					n_info_threads;
	bool				ldt_benchmarks;
	// Note - log-local-time affects a global in cf_fault.c, so can't be here.
	int					migrate_max_num_incoming;
	int					migrate_rx_lifetime_ms; // for debouncing re-tansmitted migrate start messages
	int					n_migrate_threads;
	uint32_t			nsup_delete_sleep; // sleep this many microseconds between generating delete transactions, default 0
	uint32_t			nsup_period;
	bool				nsup_startup_evict;
	uint64_t			paxos_max_cluster_size;
	paxos_protocol_enum	paxos_protocol;
	paxos_recovery_policy_enum paxos_recovery_policy;
	uint32_t			paxos_retransmit_period;
	int					proto_fd_idle_ms; // after this many milliseconds, connections are aborted unless transaction is in progress
	int					proto_slow_netio_sleep_ms; // dynamic only
	uint32_t			query_bsize;
	uint64_t			query_buf_size; // dynamic only
	uint32_t			query_bufpool_size;
	bool				query_in_transaction_thr;
	uint32_t			query_long_q_max_size;
	bool				query_enable_histogram;
	bool                partitions_pre_reserved; // query will reserve all partitions up front
	uint32_t			query_priority;
	uint64_t			query_sleep_us;
	uint64_t			query_rec_count_bound;
	bool				query_req_in_query_thread;
	uint32_t			query_req_max_inflight;
	uint32_t			query_short_q_max_size;
	uint32_t			query_threads;
	uint32_t			query_threshold;
	uint64_t			query_untracked_time_ms;
	uint32_t			query_worker_threads;
	bool				respond_client_on_master_completion;
	bool				run_as_daemon;
	uint32_t			scan_max_active; // maximum number of active scans allowed
	uint32_t			scan_max_done; // maximum number of finished scans kept for monitoring
	uint32_t			scan_max_udf_transactions; // maximum number of active transactions per UDF background scan
	uint32_t			scan_threads; // size of scan thread pool
	uint32_t			sindex_builder_threads; // secondary index builder thread pool size
	uint64_t			sindex_data_max_memory; // maximum memory for secondary index trees
	bool				sindex_gc_enable_histogram; // dynamic only
	bool				snub_nodes;
	uint32_t			ticker_interval;
	uint64_t			transaction_max_ns;
	uint32_t			transaction_pending_limit; // 0 means no limit
	bool				transaction_repeatable_read;
	uint32_t			transaction_retry_ms;
	uint64_t			udf_runtime_max_gmemory; // maximum runtime memory allowed for all UDF - TODO - used?
	uint64_t			udf_runtime_max_memory; // maximum runtime memory allowed for per UDF - TODO - used?
	bool				use_queue_per_device;
	char*				work_directory;
	bool				write_duplicate_resolution_disable;

	bool				asmalloc_enabled; // whether ASMalloc integration is enabled
	bool				fabric_dump_msgs; // whether to log information about existing "msg" objects and queues
	int64_t				max_msgs_per_type; // maximum number of "msg" objects permitted per type
	bool				memory_accounting; // whether memory accounting is enabled
	uint32_t			prole_extra_ttl; // seconds beyond expiry time after which we garbage collect, 0 for no garbage collection
	bool				non_master_sets_delete;	// dynamic only - locally delete non-master records in sets that are being emptied

	//--------------------------------------------
	// network::service context.
	//

	cf_socket_cfg		socket;
	cf_socket_cfg		localhost_socket; // for listener on 127.0.0.1, only opened if main socket not already listening on 0.0.0.0 or 127.0.0.1
	char*				external_address; // host name that clients will connect on
	bool				is_external_address_virtual;
	char*				alternate_address; // alternate service address (could be DNS)
	char*				network_interface_name; // network_interface_name to use on this machine for generating the IP addresses
	bool				socket_reuse_addr; // whether or not a socket can be reused (SO_REUSEADDR)

	//--------------------------------------------
	// network::heartbeat context.
	//

	hb_mode_enum		hb_mode;
	char*				hb_addr;
	int					hb_port;
	char* 				hb_init_addr;
	int					hb_init_port;
	char*				hb_mesh_seed_addrs[AS_CLUSTER_SZ];
	int					hb_mesh_seed_ports[AS_CLUSTER_SZ];
	uint32_t			hb_interval;
	uint32_t			hb_timeout;
	char*				hb_tx_addr;
	uint8_t				hb_mcast_ttl;
	uint32_t			hb_mesh_rw_retry_timeout;
	hb_protocol_enum	hb_protocol;

	//--------------------------------------------
	// network::fabric context.
	//

	int					fabric_port;

	bool				fabric_keepalive_enabled;
	int					fabric_keepalive_time;
	int					fabric_keepalive_intvl;
	int					fabric_keepalive_probes;

	//--------------------------------------------
	// network::info context.
	//

	int					info_port;

	//--------------------------------------------
	// Configuration sub-containers.
	//

	mod_lua_config		mod_lua;
	cluster_config_t	cluster;
	as_sec_config		sec_cfg;

	//--------------------------------------------
	// Namespaces. (These are configured...)
	//

	struct as_namespace_s* namespaces[AS_NAMESPACE_SZ];
	uint32_t			n_namespaces;

	// To speed up transaction enqueue's determination of data-in-memory:
	uint32_t			n_namespaces_in_memory;
	uint32_t			n_namespaces_not_in_memory;


	//======================================================
	// Not (directly) configuration. Many should probably be
	// relocated...
	//

	// Address advertised for receiving [mesh only] heartbeats: computed
	// starting with "heartbeat.address" (g_config.hb_addr), and set to a real
	// IP address (g_config.node_ip) if that is "any", and finally overriden by
	// "heartbeat.interface-address" (g_config.hb_tx_addr), if set.
	char*				hb_addr_to_use;

	// heartbeat: takes the lock and fills in this structure
	// paxos: read only uses it for detecting changes
	cf_node				hb_paxos_succ_list_index[AS_CLUSTER_SZ];
	cf_node				hb_paxos_succ_list[AS_CLUSTER_SZ][AS_CLUSTER_SZ];
	pthread_mutex_t		hb_paxos_lock;

	// Cluster-config related.
	cf_node				self_node; // unique instance ID either HW inspired or cluster group/node ID
	uint16_t			cluster_mode;
	cf_node				hw_self_node; // cache the HW value self-node value, for various uses
	char*				node_ip;

	// Global object pointers that just shouldn't be here.
	as_paxos*			paxos;
	as_smd_t*			smd;
	olock*				record_locks;

	// Global variables that just shouldn't be here.
	cf_atomic_int		migrate_num_incoming; // for receiver-side migration flow control
	cf_atomic_int		partition_generation; // global counter to signal clients that partition map changed
	uint64_t			start_ms; // start time of the server
	cf_queue*			transactionq_a[MAX_TRANSACTION_QUEUES];
	uint32_t			transactionq_current;
	cf_socket_cfg		xdr_socket; // the port to listen on for XDR compatibility, typically 3004
	cf_node				xdr_clmap[AS_CLUSTER_SZ]; // cluster map as known to XDR
	xdr_lastship_s		xdr_lastship[AS_CLUSTER_SZ]; // last XDR shipping info of other nodes
	uint64_t			xdr_self_lastshiptime[DC_MAX_NUM]; // last XDR shipping by this node

	cf_atomic64	    	sindex_data_memory_used;  // TODO - used?
	cf_atomic_int		udf_runtime_gmemory_used; // TODO - used?

} as_config;

/* Configuration function declarations */
extern as_config *as_config_init(const char *config_file);
extern void as_config_post_process(as_config *c, const char *config_file);

/* Declare an instance of the configuration structure in global scope */
extern as_config g_config;
extern xdr_config g_xcfg;
