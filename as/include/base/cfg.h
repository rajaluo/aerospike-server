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

/* as_config
 * Runtime configuration */
typedef struct as_config_s {

	/* Global service configuration */
	uid_t				uid;
	gid_t				gid;
	char				*pidfile;
	bool				run_as_daemon;

	/* A unique instance ID: Either HW inspired, or Cluster Group/Node ID */
	cf_node				self_node;
	uint16_t			cluster_mode;
	cf_node				hw_self_node; // Cache the HW value self-node value, for various uses.

	/* IP address */
	char				*node_ip;

	/* Heartbeat system */
	hb_mode_enum		hb_mode;
	hb_protocol_enum	hb_protocol;
	char				*hb_addr;
	char 				*hb_init_addr;
	int					hb_port;
	int					hb_init_port;
	char				*hb_mesh_seed_addrs[AS_CLUSTER_SZ];
	int					hb_mesh_seed_ports[AS_CLUSTER_SZ];
	char				*hb_tx_addr;
	// Address advertised for receiving [mesh only] heartbeats:
	// Computed starting with "heartbeat.address" (g_config.hb_addr),
	// and set to a real IP address (g_config.node_ip) if that is "any",
	// and finally overriden by "heartbeat.interface-address" (g_config.hb_tx_addr), if set.
	char				*hb_addr_to_use;
	uint32_t			hb_interval;
	uint32_t			hb_timeout;
	unsigned char		hb_mcast_ttl;
	uint32_t			hb_mesh_rw_retry_timeout;

	uint64_t			start_ms; // filled with the start time of the server

	/* tuning parameters */
	int					n_migrate_threads;
	int					n_info_threads;
	int					n_batch_index_threads;
	int					n_batch_threads;

	/* Query tunables */
	uint32_t			query_threads;
	uint32_t			query_worker_threads;
	uint32_t			query_priority;
	uint64_t			query_sleep_us;
	uint32_t			query_bsize;
	bool				query_in_transaction_thr;
	uint64_t			query_buf_size;
	uint32_t			query_threshold;
	uint64_t			query_rec_count_bound;
	bool				query_req_in_query_thread;
	uint32_t			query_req_max_inflight;
	uint32_t			query_bufpool_size;
	uint32_t			query_short_q_max_size;
	uint32_t			query_long_q_max_size;
	uint64_t			query_untracked_time_ms;

	int					n_transaction_queues;
	int					n_transaction_threads_per_queue;
	int					n_service_threads;
	int					n_fabric_workers;
	bool				use_queue_per_device;
	bool				allow_inline_transactions;

	/* max client file descriptors */
	int					n_proto_fd_max;

	/* after this many milliseconds, connections are aborted unless transaction is in progress */
	int					proto_fd_idle_ms;

	/* sleep this many millisecond before retrying for all the blocked query */
	int					proto_slow_netio_sleep_ms;

	/* The TCP port for the fabric */
	int					fabric_port;

	/* Fabric TCP socket keepalive parameters */
	bool				fabric_keepalive_enabled;
	int					fabric_keepalive_time;
	int					fabric_keepalive_intvl;
	int					fabric_keepalive_probes;

	/* The TCP port for the info socket */
	int					info_port;

	/* The TCP socket for the listener */
	cf_socket_cfg		socket;

	/* The TCP socket for the listener on 127.0.0.1 */
	/* (Only opened if the main service socket is not already listening on 0.0.0.0 or 127.0.0.1.) */
	cf_socket_cfg		localhost_socket;

	/* The port to listen on for XDR compatibility, typically 3004. */
	cf_socket_cfg		xdr_socket;

	char				*external_address; // hostname that clients will connect on
	bool				is_external_address_virtual;
	char				*alternate_address; // alternate service address (could be DNS)
	char				*network_interface_name; // network_interface_name to use on this machine for generating the IP addresses

	/* Whether or not a socket can be reused (SO_REUSEADDR) */
	bool				socket_reuse_addr;

	/* Consensus algorithm runtime data */
	as_paxos			*paxos;

	/*
	 * heartbeat: takes the lock and fills in this structure
	 * paxos: read only uses it for detecting changes
	 */
	cf_node				hb_paxos_succ_list_index[AS_CLUSTER_SZ];
	cf_node				hb_paxos_succ_list[AS_CLUSTER_SZ][AS_CLUSTER_SZ];
	pthread_mutex_t		hb_paxos_lock;

	/* System Metadata module state */
	as_smd_t			*smd;

	/* a global generation count on all partition state changes */
	cf_atomic_int		partition_generation;

	/* The transaction queues */
	uint32_t			transactionq_current;
	cf_queue			*transactionq_a[MAX_TRANSACTION_QUEUES];

	/* object lock structure */
	olock				*record_locks;

	/* global configuration for how often to print 'ticker' info to the log - 0 is no ticker */
	uint32_t			ticker_interval;
	
	// whether to collect ldt benchmarks
	bool				ldt_benchmarks;

	// whether memory accounting is enabled
	bool				memory_accounting;

	// whether ASMalloc integration is enabled
	bool				asmalloc_enabled;

	// whether to log information about existing "msg" objects and queues
	bool				fabric_dump_msgs;

	// maximum number of "msg" objects permitted per type
	int64_t				max_msgs_per_type;

	// the common work directory cache
	char				*work_directory;

	/*
	**  TUNING PARAMETERS
	*/

	/* global timeout configuration */
	uint32_t			transaction_retry_ms;
	// max time (ns) in the system before we kick the request out forever
	uint64_t			transaction_max_ns;
	// transaction pending limit - number of pending transactions ON A SINGLE RECORD (0 means no limit)
	uint32_t			transaction_pending_limit;
	/* transaction_repeatable_read flag defines whether a read should attempt to get all duplicate values before returning */
	bool				transaction_repeatable_read;
	/* disable generation checking */
	bool				generation_disable;
	bool				write_duplicate_resolution_disable;
	/* respond client on master completion */
	bool				respond_client_on_master_completion;
	/* enables node snubbing - this code caused a Paxos issue in the past */
	bool				snub_nodes;

	uint32_t			scan_max_active;			// maximum number of active scans allowed
	uint32_t			scan_max_done;				// maximum number of finished scans kept for monitoring
	uint32_t			scan_max_udf_transactions;	// maximum number of active transactions per UDF background scan
	uint32_t			scan_threads;				// size of scan thread pool

	// maximum count of database requests in a single batch
	uint32_t			batch_max_requests;
	// maximum number of buffers allowed in a buffer queue at any one time.  Fail batch if full.
	uint32_t			batch_max_buffers_per_queue;
	// maximum number of buffers allowed in buffer pool at any one time.
	uint32_t			batch_max_unused_buffers;
	// number of records between an enforced context switch - thus 1 is very low priority, 1000000 would be very high
	uint32_t			batch_priority;  // Used by old batch functionality only.

	// nsup (expiration and eviction) tuning parameters
	uint32_t			nsup_delete_sleep; // sleep this many microseconds between generating delete transactions, default 0
	uint32_t			nsup_period;
	bool				nsup_startup_evict;

	/* tuning parameter for how often to run retransmit checks for paxos */
	uint32_t			paxos_retransmit_period;
	/* parameter that let the cluster run under lower replication factor for less than 1 */
	uint32_t			paxos_single_replica_limit; // cluster size at which, and below, the cluster will run with repl factor 1
	/* Maximum size of cluster allowed to be formed. */
	uint64_t			paxos_max_cluster_size;
	/* Currently-active Paxos protocol version. */
	paxos_protocol_enum	paxos_protocol;
	/* Currently-active Paxos recovery policy. */
	paxos_recovery_policy_enum	paxos_recovery_policy;

	// For receiver-side migration flow control:
	int					migrate_max_num_incoming;
	cf_atomic_int		migrate_num_incoming;
	// For debouncing re-tansmitted migrate start messages:
	int					migrate_rx_lifetime_ms;

	// Temporary dangling prole garbage collection.
	uint32_t			prole_extra_ttl;	// seconds beyond expiry time after which we garbage collect, 0 for no garbage collection
	bool				non_master_sets_delete;	// locally delete non-master records in sets that are being emptied

	xdr_lastship_s		xdr_lastship[AS_CLUSTER_SZ];		// last XDR shipping info of other nodes
	cf_node				xdr_clmap[AS_CLUSTER_SZ];			// cluster map as known to XDR
	uint64_t			xdr_self_lastshiptime[DC_MAX_NUM];	// last XDR shipping by this node

	// configuration to put cap on amount of memory
	// all secondary index put together can take
	// this is to protect cluster. This override the
	// per namespace configured value
	uint32_t		sindex_builder_threads;   // Secondary index builder thread pool size
	uint64_t		sindex_data_max_memory;   // Maximum memory for secondary index trees
	cf_atomic64	    sindex_data_memory_used;  // Maximum memory for secondary index trees
	cf_atomic_int   sindex_gc_timedout;           // Number of time sindex gc iteration timed out waiting for partition lock
	uint64_t        sindex_gc_inactivity_dur;     // Cumulative sum of sindex GC thread inactivity.
	uint64_t        sindex_gc_activity_dur;       // Cumulative sum of sindex gc thread activity.
	uint64_t        sindex_gc_list_creation_time; // Cumulative sum of list creation phase in sindex GC
	uint64_t        sindex_gc_list_deletion_time; // Cumulative sum of list deletion phase in sindex GC
	uint64_t        sindex_gc_garbage_found;      // Amount of garbage found during list creation phase
	uint64_t        sindex_gc_garbage_cleaned;    // Amount of garbage deleted during list deletion phase
	uint64_t        sindex_gc_objects_validated;  // Cumulative sum of sindex objects validated
	bool            sindex_gc_enable_histogram;
	histogram      *_sindex_gc_validate_obj_hist; // Histogram to track time taken to validate sindex object
	histogram      *_sindex_gc_delete_obj_hist;   // Histogram to track time taken to delete sindex object by GC
	histogram      *_sindex_gc_pimd_rlock_hist;   // HIstogram to track time spent under pimd rlock by sindex GC
	histogram      *_sindex_gc_pimd_wlock_hist;   // Histogram to track time spent under pimd wlock by sindex GC

	bool                partitions_pre_reserved;  // If true query will reserve all the partitions upfront 
												  // before processing query. Default - FALSE

	cf_atomic64			query_false_positives;
	bool				query_enable_histogram;

	uint64_t			udf_runtime_max_memory; // Maximum runtime memory allowed for per UDF
	uint64_t			udf_runtime_max_gmemory; // maximum runtime memory alloed for all UDF
	cf_atomic_int		udf_runtime_gmemory_used; // Current runtime memory reserve by per UDF - BUG if global should be 64?

	// Geospatial stats
	cf_atomic_int		geo_region_query_count;		// Number of region queries
	cf_atomic_int		geo_region_query_cells;		// Number of cells used by region queries
	cf_atomic_int		geo_region_query_points;	// Number of valid points found
	cf_atomic_int		geo_region_query_falsepos;	// Number of false positives found

	/*
	** STATISTICS
	*/
	cf_atomic_int		fabric_msgs_sent;
	cf_atomic_int		fabric_msgs_rcvd;
	cf_atomic_int		fabric_msgs_selfsend;  // not included in prev send + receive
	cf_atomic_int		fabric_write_short;
	cf_atomic_int		fabric_write_medium;
	cf_atomic_int		fabric_write_long;
	cf_atomic_int		fabric_read_short;
	cf_atomic_int		fabric_read_medium;
	cf_atomic_int		fabric_read_long;
	cf_atomic_int		proto_transactions;
	cf_atomic_int		proxy_initiate; // initiated
	cf_atomic_int		ldt_proxy_initiate; // initiated
	cf_atomic_int		proxy_action;   // did it
	cf_atomic_int		proxy_retry;    // retried it
	cf_atomic_int		ldt_proxy_timeout;    // retried it
	cf_atomic_int		proxy_retry_q_full;
	cf_atomic_int		proxy_unproxy;
	cf_atomic_int		proxy_retry_same_dest;
	cf_atomic_int		proxy_retry_new_dest;
	cf_atomic_int		proto_connections_opened;
	cf_atomic_int		proto_connections_closed;
	cf_atomic_int		fabric_connections_opened;
	cf_atomic_int		fabric_connections_closed;
	cf_atomic_int		heartbeat_connections_opened;
	cf_atomic_int		heartbeat_connections_closed;
	cf_atomic_int		heartbeat_received_self;
	cf_atomic_int		heartbeat_received_foreign;
	cf_atomic_int		info_connections_opened;
	cf_atomic_int		info_connections_closed;
	cf_atomic_int		global_record_ref_count;
	cf_atomic_int		reaper_count;

	cf_atomic64			n_demarshal_error;
	cf_atomic64			n_tsvc_client_error;
	cf_atomic64			n_tsvc_batch_sub_error;
	cf_atomic64			n_tsvc_udf_sub_error;

	cf_atomic_int		batch_index_initiate; // not (just) a statistic

	cf_atomic_int		batch_index_complete;
	cf_atomic_int		batch_index_timeout;
	cf_atomic_int		batch_index_errors;

	cf_atomic_int		batch_index_huge_buffers;
	cf_atomic_int		batch_index_created_buffers;
	cf_atomic_int		batch_index_destroyed_buffers;

	// "Old" batch.
	cf_atomic_int		batch_initiate;
	cf_atomic_int		batch_timeout;
	cf_atomic_int		batch_errors;

	// For now all tracked histograms are namespace scoped, but these controls
	// are still global:
	uint32_t			hist_track_back; // total time span in seconds over which to cache data
	uint32_t			hist_track_slice; // period in seconds at which to cache histogram data
	char *				hist_track_thresholds; // comma-separated bucket (ms) values to track

	histogram *			batch_index_hist;
	bool				batch_index_hist_active;

	histogram *			info_hist;
	bool				info_hist_active;

	histogram *			svc_demarshal_hist;
	histogram *			svc_queue_hist;
	bool				svc_benchmarks_active;

	// LDT related histogram
	histogram *			ldt_multiop_prole_hist;   // histogram that tracks LDT multi op replication performance (in fabric)
	histogram *			ldt_update_record_cnt_hist; // histogram that tracks number of records written (write/update)
                                             // by LDT UDF execluding parent record
	histogram *			ldt_io_record_cnt_hist; // histogram that tracks number of records opened (write/update)
                                             // by LDT UDF execluding parent record
	histogram *			ldt_update_io_bytes_hist;   // histogram that tracks number bytes written by LDT every transaction
	histogram * 		ldt_hist;            // histogram that tracks ldt performance

	cf_atomic_int		err_storage_queue_full;
	cf_atomic_int		err_storage_defrag_corrupt_record;

	// For Lua Garbage Collection, we want to track three things:
	// (1) The number of times we were below the GC threshold
	// (2) The number of times we performed "Light GC" (step-wise gc)
	// (3) The number of times we performed "Heavy GC" (full gc)
	// Currently, however, there is no direct connection between the g_config
	// object and the mod-lua world, so we will need to use some other
	// mechanism to fill in these stats.  They are inactive for now.
	// (May 19, 2014 tjl)
	// cf_atomic_int	stat_lua_gc_delay;
	// cf_atomic_int	stat_lua_gc_step;
	// cf_atomic_int	stat_lua_gc_full;

	/* Namespaces */
	uint32_t			n_namespaces;
	struct as_namespace_s * namespaces[AS_NAMESPACE_SZ];

	// To speed up transaction enqueue's determination of data-in-memory:
	uint32_t			n_namespaces_in_memory;
	uint32_t			n_namespaces_not_in_memory;

	// MOD_LUA Config
	mod_lua_config		mod_lua;

	// Cluster Config Info
	cluster_config_t	cluster;

	// Security configuration info.
	as_sec_config		sec_cfg;

} as_config;

/* Configuration function declarations */
extern as_config *as_config_init(const char *config_file);
extern void as_config_post_process(as_config *c, const char *config_file);

/* Declare an instance of the configuration structure in global scope */
extern as_config g_config;
extern xdr_config g_xcfg;
