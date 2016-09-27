/*
 * hb.h
 *
 * Copyright (C) 2008-2016 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_vector.h"

#include "msg.h"
#include "socket.h"

#include "fabric/hlc.h"

/**
 * Maximum number of nodes in a cluster.
 */
#define AS_CLUSTER_SZ 128

/**
 * Minimum heartbeat interval.
 */
#define AS_HB_TX_INTERVAL_MS_MIN 50

/**
 * Maximum heartbeat interval. (10 mins)
 */
#define AS_HB_TX_INTERVAL_MS_MAX 600000

/**
 * Heartbeat modes.
 */
typedef enum as_hb_mode_enum {
	AS_HB_MODE_UNDEF,
	AS_HB_MODE_MULTICAST,
	AS_HB_MODE_MESH
} as_hb_mode;

/**
 * Heart protocol versions.
 */
typedef enum as_hb_protocol_enum {
	AS_HB_PROTOCOL_UNDEF,
	AS_HB_PROTOCOL_NONE,
	AS_HB_PROTOCOL_RESET,
	AS_HB_PROTOCOL_V1,
	AS_HB_PROTOCOL_V2,
	AS_HB_PROTOCOL_V3
} as_hb_protocol;

/**
 * Heartbeat subsystem configuration.
 */
typedef struct as_hb_config_s
{
	/**
	 * Mode of operation. Mesh or Multicast for now.
	 */
	as_hb_mode mode;

	/**
	 * Binding interface config.
	 */
	cf_serv_cfg bind_cfg;

	/**
	 * Multicast mode only config for multicast groups.
	 */
	cf_mserv_cfg multicast_group_cfg;

	/**
	 * The interval at which heartbeat pulse messages are sent in
	 * milliseconds.
	 */
	uint32_t tx_interval;

	/**
	 * Max number of missed heartbeat intervals after which a node is
	 * considered expired.
	 */
	uint32_t max_intervals_missed;

	/**
	 * Set multiple of 'hb max intervals missed' during which if no fabric
	 * messages arrive from a node, the node is considered fabric expired.
	 * Set to -1 for infinite grace period.
	 */
	int fabric_grace_factor;

	/**
	 * The ttl for multicast packets. Set to zero for default TTL.
	 */
	uint8_t multicast_ttl;

	/**
	 * HB protocol to use.
	 */
	as_hb_protocol protocol;

	/**
	 * Set to a value > 0 to override the MTU read from the network
	 * interface.
	 */
	uint32_t override_mtu;

	/**
	 * Mesh seeds from config file.
	 * Only used for during config parsing and initialization.
	 */
	char* mesh_seed_addrs[AS_CLUSTER_SZ];
	int mesh_seed_ports[AS_CLUSTER_SZ];

} as_hb_config;

/**
 * Errors encountered by the heartbeat subsystem.
 */
typedef enum as_hb_err_type_e {
	AS_HB_ERR_NO_SRC_NODE,
	AS_HB_ERR_NO_TYPE,
	AS_HB_ERR_NO_ID,
	AS_HB_ERR_HEARTBEAT_PROTOCOL_MISMATCH,
	AS_HB_ERR_NO_ENDPOINT,
	AS_HB_ERR_NO_SEND_TS,
	AS_HB_ERR_NO_NODE_REQ,
	AS_HB_ERR_NO_NODE_REPLY,
	AS_HB_ERR_NO_ANV_LENGTH,
	AS_HB_ERR_MAX_CLUSTER_SIZE_MISMATCH,
	AS_HB_ERR_SEND_INFO_REQ_FAIL,
	AS_HB_ERR_SEND_INFO_REPLY_FAIL,
	AS_HB_ERR_SEND_BROADCAST_FAIL,
	AS_HB_ERR_EXPIRE_HB,
	AS_HB_ERR_EXPIRE_FAB_DEAD,
	AS_HB_ERR_EXPIRE_FAB_ALIVE,
	AS_HB_ERR_UNPARSABLE_MSG,
	AS_HB_ERR_MESH_CONNECT_FAIL,
	AS_HB_ERR_REMOTE_CLOSE,
	AS_HB_ERR_MTU_BREACH,
	AS_HB_ERR_MAX_TYPE
} as_hb_err_type;

/**
 * Events published by the heartbeat subsystem.
 */
typedef enum {
	AS_HB_NODE_ARRIVE,
	AS_HB_NODE_DEPART,
	AS_HB_AUTO_RESET
} as_hb_event_type;

/**
 * Heartbeat published event structure.
 */
typedef struct as_hb_event_node_s
{
	/**
	 * The type of the event.
	 */
	as_hb_event_type evt;

	/**
	 * The event nodeid.
	 */
	cf_node nodeid;

	/**
	 * The monotonic timestamp when this event happened.
	 */
	cf_clock event_time;

	/**
	 * The monotonic timestamp when this event was detected. Will differ
	 * from event_time for node depart events.
	 */
	cf_clock event_detected_time;
} as_hb_event_node;

/**
 * A plugin that is publishing and receiving data via the heartbeat subsystem.
 * The
 * heartbeat outgoing message buffer will be populated and parsed in the order
 * of this enum.
 */
typedef enum {
	/**
	 * The heartbeat subsystem itself.
	 */
	AS_HB_PLUGIN_HB,
	/**
	 * The older clustering subsystem.
	 * TODO: Use only one plugin id and register differently based on the
	 * clustering version.
	 */
	AS_HB_PLUGIN_FABRIC,
	/**
	 * The older clustering subsystem.
	 * TODO: Use only one plugin id and register differently based on the
	 * clustering version.
	 */
	AS_HB_PLUGIN_PAXOS,
	/**
	 * The clustering subsystem.
	 */
	AS_HB_PLUGIN_CLUSTERING,
	/**
	 * Dummy sentinel enum value. Should be the last.
	 */
	AS_HB_PLUGIN_SENTINEL
} as_hb_plugin_id;

/**
 * A hook to allow plugin to publish its data as a part of the heartbeat
 * message.
 */
typedef void (*as_hb_plugin_set_data_fn)(msg* hb_message);

/**
 * Data stored for an adjacent node for a plugin.
 */
typedef struct as_hb_plugin_node_data_s
{
	/**
	 * Heap allocated node specific data blob for this plugin.
	 */
	void* data;

	/**
	 * The size of the stored data.
	 */
	size_t data_size;

	/**
	 * The capacity of the allocated data structure.
	 */
	size_t data_capacity;
} as_hb_plugin_node_data;

/**
 * A function to parse plugin data for a node into an in memory object. Should
 * be fast and never acquire locks.
 *
 * The parameter plugin_data->data will always be pointer to a previously
 * allocated memory location. plugin_data->data_capacity will indicate the
 * capacity of this memory. Implementations should reuse this previously
 * allocated data blob to avoid the overhead of heap  allocations. If current
 * data capacity is greater than the new data size please
 * invoke cf_realloc and get a new block for current data and update and
 * plugin_data->data and plugin_data->data_capacity accordingly.
 *
 * This function should always data_size correctly before returning. Set
 * plugin_data->data_size = 0 for no plugin data.
 *
 * @param hb_message the heartbeat message.
 * @param source the source node.
 * @param plugin_data (output) plugin data structure to output parsed data.
 */
typedef void (*as_hb_plugin_parse_data_fn)(msg* hb_message, cf_node source,
					   as_hb_plugin_node_data* plugin_data);

/**
 * A listener for detecting changes to this plugin's data for a particular node.
 * Does not supply old and new values of the data, because does not seem to be
 * required currently and to keep implementation simple.
 *
 * @param node the node whose plugin data changed.
 */
typedef void (*as_hb_plugin_data_changed_fn)(cf_node nodeid);

/**
 * A plugin allows a module to pushing and read
 * data with heartbeat pulse messages.
 */
typedef struct as_hb_plugin_s
{
	/**
	 * The plugin id.
	 */
	as_hb_plugin_id id;

	/**
	 * Fixed plugin data size on wire.
	 */
	size_t wire_size_fixed;

	/**
	 * Fixed plugin data size on wire.
	 */
	size_t wire_size_per_node;

	/**
	 * The function which adds this plugin's data to the pulse message. Can
	 * be NULL. This function can hold the plugin module's locks.
	 */
	as_hb_plugin_set_data_fn set_fn;

	/**
	 * A function will parses and reads this plugins data from an incoming
	 * message. Can be NULL. This function SHOULD NOT hold the plugin
	 * module's locks to prevent deadlocks.
	 */
	as_hb_plugin_parse_data_fn parse_fn;

	/**
	 * A function invoked when plugin data for a particular node changed.
	 * Can be NULL. This function can hold the plugin module's locks.
	 */
	as_hb_plugin_data_changed_fn change_listener;
} as_hb_plugin;

/**
 * The fields in the heartbeat message. V2 protocol is frozen. Should never
 * change.
 */
typedef enum {
	AS_HB_V2_MSG_ID = 0,
	AS_HB_V2_MSG_TYPE = 1,
	AS_HB_V2_MSG_NODE = 2,
	AS_HB_V2_MSG_ADDR = 3,
	AS_HB_V2_MSG_PORT = 4,

	/**
	 * For pulse messages contains the node ids of the succession list.
	 */
	AS_HB_V2_MSG_ANV = 5,

	/**
	 * Legacy max cluster size and hence the length of the succession list
	 * as well.
	 */
	AS_HB_V2_MSG_ANV_LENGTH = 6,

	/*---- Internal fields not send on the wire but present to make the a v2
	   message look more like a v3 message as the code is v3 oriented. ----*/
	/**
	 * Compatibility as_endpoint_list field never send out on the wire. Used
	 * to make the a v2 message work seamlessly with v3 oriented code.
	 */
	AS_HB_V2_MSG_COMPAT_ENDPOINTS = 7,

	/**
	 * Compatibility as_endpoint_list field never send out on the wire. Used
	 * to make the a v2 message work seamlessly with v3 oriented code.
	 */
	AS_HB_V2_MSG_COMPAT_INFO_REQUEST = 8,

	/**
	 * Compatibility as_endpoint_list field never send out on the wire. Used
	 * to make the a v2 message work seamlessly with v3 oriented code.
	 */
	AS_HB_V2_MSG_COMPAT_INFO_REPLY = 9,

	/**
	 * Sentinel value. Should be the last in the enum.
	 */
	AS_HB_V2_MSG_SENTINEL = 10

} as_hb_v2_msg_fields;

/**
 * The fields in the heartbeat message.
 * New field additions only at the end.
 */
typedef enum {
	/*---- Same meaning and order as v2 fields. ----*/
	AS_HB_MSG_ID,
	AS_HB_MSG_TYPE,
	AS_HB_MSG_NODE,

	/*---- Fields specific to v3 but compulsory ----*/
	/**
	 * Cluster Name.
	 */
	AS_HB_MSG_CLUSTER_NAME,

	/**
	 * HLC timestamp.
	 */
	AS_HB_MSG_HLC_TIMESTAMP,

	/**
	 * Heartbeats endpoints advertised by this node.
	 */
	AS_HB_MSG_ENDPOINTS,

	/**
	 * Payload for compressed messages.
	 */
	AS_HB_MSG_COMPRESSED_PAYLOAD,

	/**
	 * Mesh info request.
	 */
	AS_HB_MSG_INFO_REQUEST,

	/**
	 * Mesh info reply.
	 */
	AS_HB_MSG_INFO_REPLY,

	/*---- Plugin data fields. Potentially extensible  ----*/
	/**
	 * Fabric  data advertised by this node. Placed close to hb endpoints to
	 * help compression, because it would most likely match with hb
	 * endpoints.
	 */
	AS_HB_MSG_FABRIC_DATA,

	/**
	 * Valid only for pulse messages, has adjacency list and clusterid.
	 */
	AS_HB_MSG_HB_DATA,

	/**
	 * Contains the cluster key and succession list.
	 */
	AS_HB_MSG_PAXOS_DATA
} as_hb_msg_fields;

/*-----------------------------------------------------------------
 * HB subsystem public API
 *-----------------------------------------------------------------*/
/**
 * Initialize the heartbeat subsystem.
 */
void as_hb_init();

/**
 * Start the heartbeat subsystem.
 * @param config heartbeat subsystem configuration.
 */
void as_hb_start();

/**
 * Shut down the heartbeat subsystem.
 */
void as_hb_shutdown();

/**
 * Indicates if a node is present in the heartbeat subsystem's adjacency list.
 */
bool as_hb_node_is_adjacent(cf_node nodeid);

/**
 * Get the ip address of a node given its node id. Only there to support fabric
 * getting hold of a remote node's heartbeat message, with legacy heartbeat
 * protocol.
 *
 * @param node the node to get ip address of.
 * @param addr the output ip address on success, undefined on failure.
 *
 * @return 0 if the node's ip address is found. -1 on failure.
 */
int as_hb_getaddr(cf_node node, cf_ip_addr* addr);

/**
 * Heartbeat event listener callback function.
 */
typedef void (*as_hb_event_fn)(int nevents, as_hb_event_node* events,
			       void* udata);

/**
 * Register a heartbeat node event listener.
 */
void as_hb_register_listener(as_hb_event_fn event_callback, void* udata);

/**
 * Generate events required to transform the input  succession list to a list
 * that would be consistent with the heart beat adjacency list. This means nodes
 * that are in the adjacency list but missing from the succession list will
 * generate an NODE_ARRIVE event. Nodes in the succession list but missing from
 * the adjacency list will generate a NODE_DEPART event.
 *
 * @param succession_list the succession list to correct. This should be large
 * enough to hold g_config.paxos_max_cluster_size events.
 * @param succession_size the size of the succession list.
 * @param events the output events. This should be large enough to hold
 * g_config.paxos_max_cluster_size events.
 * @param max_events the maximum number of events to generate, should be the
 * allocated size of events array.
 * @return the number of corrective events generated.
 */
int as_hb_get_corrective_events(cf_node* succession, size_t succession_size,
				as_hb_event_node* events, size_t max_events);

/**
 * Return a string summarizing the number of heartbeat-related errors of each
 * type.
 * @param verbose use long format messages if "verbose" is true, otherwise use
 * short format messages.
 * @return NULL terminated string containing the stats.
 */
char* as_hb_stats_get(bool verbose);

/**
 *  Log the state of the heartbeat module.
 */
void as_hb_dump(bool verbose);

/**
 * Set heartbeat protocol version.
 */
as_hb_protocol as_hb_protocol_get();

/**
 * Set heartbeat protocol version.
 */
int as_hb_protocol_set(as_hb_protocol protocol);

/**
 * Get the timeout interval to consider a node dead / expired in milliseconds.
 */
uint32_t as_hb_node_timeout_get();

/**
 * Override the computed MTU for the network interface used by heartbeat.
 */
void as_hb_override_mtu_set(int mtu);

/**
 * Get the heartbeat pulse transmit interval.
 */
uint32_t as_hb_tx_interval_get();

/**
 * Set the heartbeat pulse transmit interval.
 */
int as_hb_tx_interval_set(uint32_t new_interval);

/**
 * Set the maximum number of missed heartbeat intervals after which a node is
 * considered expired.
 */
void as_hb_max_intervals_missed_set(uint32_t new_max);

/**
 * Set multiple of 'hb max intervals missed' during which if no fabric messages
 * arrive from a node, the node is considered fabric expired. Set to -1 for
 * infinite grace period.
 */
void as_hb_fabric_grace_factor_set(int new_factor);

/**
 * Get the timeout interval to consider a node dead / expired in milliseconds if
 * no heartbeat pulse messages are received.
 */
uint32_t as_hb_node_timeout_get();

/**
 * Indicates if the input max cluster size is valid based on hb state.Transient
 * API to help with deciding to apply new max cluster size.
 */
bool as_hb_max_cluster_size_isvalid(uint32_t max_cluster_size);

/*-----------------------------------------------------------------
 * HB plugin subsystem public API.
 *-----------------------------------------------------------------*/

/**
 * Register the setter and parser functions for a plugin.
 */
void as_hb_plugin_register(as_hb_plugin* plugin);

/**
 * Indicates if a node is alive.
 */
bool as_hb_is_alive(cf_node nodeid);

/**
 * Validate heart beat config.
 */
void as_hb_config_validate();

/**
 * Compute the nodes to evict from the input nodes so that remaining nodes form
 * a clique, based on adjacency lists.
 *
 * @param nodes input cf_node vector.
 * @param nodes_to_evict output cf_node clique array, that is initialized.
 */
void as_hb_maximal_clique_evict(cf_vector* nodes, cf_vector* nodes_to_evict);

/**
 * Read the plugin data for a node in the adjacency list. The plugin_data->data
 * input param should be pre allocated and plugin_data->data_capacity should
 * indicate its capacity.
 *
 * @param nodeid the node id
 * @param pluginid the plugin identifier.
 * @param plugin_data (input/output) on success plugin_data->data will be the
 * plugin's data for the node and plugin_data->data_size will be the data size.
 * node. NULL if there is no plugin data.
 * @praram msg_hlc_ts  (output) if not NULL will be filled with the timestamp of
 * when the hb message for this data was received.
 * @param recv_monotonic_ts (output) if not NULL will be filled with monotonic
 * wall clock receive timestamp for this plugin data.
 * @return 0 on success and -1 on error, where errno will be set to  ENOENT if
 * there is no entry for this node and ENOMEM if the input plugin data's
 * capacity is less than plugin's data. In ENOMEM case plugin_data->data_size
 * will be set to the required capacity.
 */
int as_hb_plugin_data_get(cf_node nodeid, as_hb_plugin_id plugin,
			  as_hb_plugin_node_data* plugin_data,
			  as_hlc_msg_timestamp* msg_hlc_ts,
			  cf_clock* recv_monotonic_ts);

/**
 * Iterate function for iterating over plugin data.
 * @param node the node iterated.
 * @param plugin_data a pointer to the saved plugin data for the node.
 * @param data_size the size of the plugin data.
 * @param recv_monotonic_ts the monotonic wall clock receive timestamp for this
 * plugin data.
 * @praram msg_hlc_ts  the timestamp of when the hb message for this
 * data was received. Will be NULL if there is not plugin data.
 * @param udata udata passed through from the invoker of the iterate function.
 * NULL if there is no plugin data.
 */
typedef void (*as_hb_plugin_data_iterate_fn)(cf_node nodeid, void* plugin_data,
					     size_t plugin_data_size,
					     cf_clock recv_monotonic_ts,
					     as_hlc_msg_timestamp* msg_hlc_ts,
					     void* udata);

/**
 * Call the iterate method on plugin data for all nodes in the input vector. The
 * iterate function will be invoked for all nodes in the input vector even if
 * they are not in the adjacency list of have no plugin data. Plugin data will
 * be NULL with size zero in such cases.
 *
 * @param nodes the iterate on.
 * @param plugin the plugin identifier.
 * @param iterate_fn the iterate function invoked for plugin data for every
 * node.
 * @param udata passed as is to the iterate function. Useful for getting results
 * out of the iteration.
 * NULL if there is no plugin data.
 * @return the size of the plugin data. 0 if there is no plugin data.
 */
void as_hb_plugin_data_iterate(cf_vector* nodes, as_hb_plugin_id plugin,
			       as_hb_plugin_data_iterate_fn iterate_fn,
			       void* udata);

/**
 * Call the iterate method on all nodes in current adjacency list. Note plugin
 * data can still be NULL if the plugin data failed to parse the plugin data.
 *
 * @param plugin the plugin identifier.
 * @param iterate_fn the iterate function invoked for plugin data for every
 * node.
 * @param udata passed as is to the iterate function. Useful for getting results
 * out of the iteration.
 * NULL if there is no plugin data.
 * @return the size of the plugin data. 0 if there is no plugin data.
 */
void as_hb_plugin_data_iterate_all(as_hb_plugin_id plugin,
				   as_hb_plugin_data_iterate_fn iterate_fn,
				   void* udata);

/*-----------------------------------------------------------------
 * Info public API
 *-----------------------------------------------------------------*/
/**
 * Populate the buffer with heartbeat configuration.
 */
void as_hb_info_config_get(cf_dyn_buf* db);

/**
 * Generate a string for listening address and port in format
 * ip_address:port along with the heartbeat mode.
 *
 * @param mode (output) current heartbeat subsystem mode.
 * @param addr_port (output) listening ip address and port formatted as
 * ip_address:port
 * @param addr_port_capacity the capacity of the addr_port input.
 */
void as_hb_info_listen_addr_get(as_hb_mode* mode, char* addr_port,
				size_t addr_port_capacity);

/*-----------------------------------------------------------------
 * Mesh mode public API
 *-----------------------------------------------------------------*/
/**
 * Add an aerospike instance from the mesh seed list.
 */
int as_hb_mesh_tip(char* host, int port);

/**
 * Remove an aerospike instance from the mesh list.
 */
int as_hb_mesh_tip_clear(char* host, int port);

/**
 * Clear the entire mesh list.
 */
int as_hb_mesh_tip_clear_all();

/**
 * Validate heart beat config.
 */
void as_hb_config_validate();
