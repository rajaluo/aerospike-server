/*
 * paxos.c
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

/*
 *  Paxos consensus algorithm
 *
 */

#include "fabric/paxos.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h> // For MAX() and MIN().
#include <time.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_queue_priority.h"
#include "citrusleaf/cf_random.h"

#include "fault.h"
#include "msg.h"
#include "node.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/system_metadata.h"
#include "base/thr_info.h"
#include "fabric/clustering.h"
#include "fabric/exchange.h"
#include "fabric/fabric.h"
#include "fabric/hb.h"
#include "fabric/hlc.h"
#include "fabric/migrate.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "storage/storage.h"


/* SYNOPSIS
 * Paxos
 *
 * Generations
 * A Paxos generation consists of two unsigned 32-bit fields: a sequence
 * number, which tracks each separate Paxos action. A sequence
 * number of zero is invalid and used for internal record keeping.
 *
 * Pending transaction list
 * We maintain a list of pending transactions.  This list holds the last
 * AS_PAXOS_ALPHA transactions we've seen.
 *
 * code flow
 * _event, _spark, _thr
 * weird state structure in _thr!!!
 */


// TEMPORARY - reach into new clustering module.
extern void as_clustering_set_integrity(bool has_integrity);
extern void as_exchange_cluster_key_set(uint64_t cluster_key);
extern cf_node* as_exchange_succession();
extern void as_exchange_succession_set(cf_node* succession, uint32_t cluster_size);
extern void as_clustering_switchover(uint64_t cluster_key,	uint32_t cluster_size,
				cf_node* succession, uint32_t sequence_number);


/* Function forward references: */

bool as_paxos_succession_ismember(cf_node n);
cf_node as_paxos_succession_getprincipal();
void as_paxos_current_init(as_paxos* p);
static bool as_paxos_are_proto_compatible(uint32_t protocol1, uint32_t protocol2);
static void as_paxos_hb_get_succession_list(cf_node nodeid,
					    cf_node* succession);
static cf_node as_paxos_hb_get_principal(cf_node nodeid);


/* AS_PAXOS_PROTOCOL_IDENTIFIER
 * Select the appropriate message identifier for the active Paxos protocol. */
#define AS_PAXOS_PROTOCOL_IDENTIFIER() (AS_PAXOS_PROTOCOL_V1 == g_config.paxos_protocol ? AS_PAXOS_MSG_V1_IDENTIFIER : \
										(AS_PAXOS_PROTOCOL_V2 == g_config.paxos_protocol ? AS_PAXOS_MSG_V2_IDENTIFIER : \
										 (AS_PAXOS_PROTOCOL_V3 == g_config.paxos_protocol ? AS_PAXOS_MSG_V3_IDENTIFIER : \
										  AS_PAXOS_MSG_V4_IDENTIFIER)))

/* AS_PAXOS_PROTOCOL_IS_V
 * Is the current Paxos protocol version the given version number? */
#define AS_PAXOS_PROTOCOL_IS_V(n) (AS_PAXOS_PROTOCOL_V ## n == g_config.paxos_protocol)

/* AS_PAXOS_PROTOCOL_IS_AT_LEAST_V
 * Is the current Paxos protocol version greater than or equal to the given version number? */
#define AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(n) ((((int)(g_config.paxos_protocol) - AS_PAXOS_PROTOCOL_V ## n)) >= 0)

/* AS_PAXOS_PROTOCOL_VERSION_NUMBER
 * Return the version number for the given Paxos protocol identifier. */
#define AS_PAXOS_PROTOCOL_VERSION_NUMBER(n) ((n) - AS_PAXOS_PROTOCOL_NONE)

/* AS_PMC_USE
 * USE paxos_max_cluster_size? (For backward compatibility)
 */
#define AS_PMC_USE() (as_hb_protocol_get() != AS_HB_PROTOCOL_V3)

/* AS_PAXOS_ENABLED
 * Is this node sending out and receiving Paxos messages? */
#define AS_PAXOS_ENABLED() (AS_PAXOS_PROTOCOL_NONE != g_config.paxos_protocol)

/*
 * Maximum time, in millis, auto reset waits for a paxos transaction to finish.
 */
#define AS_PAXOS_AUTO_RESET_MAX_WAIT 50000

/*
 * Maximum number of attempts for a sync message before trying a full recovery.
 * Should necessarily be >= 1 to ensure that at leat one attempt at sync is
 * allowed to go through.
 */
#define AS_PAXOS_SYNC_ATTEMPTS_MAX 2

/*
 * Block size for allocating node plugin data. Ensure the allocation is in
 * multiples of 128 bytes, allowing expansion to 16 nodes without reallocating.
 */
#define HB_PLUGIN_DATA_BLOCK_SIZE 128

/*
 * The singleton paxos object. TODO - why a pointer, is struct too big?
 */
as_paxos *g_paxos = NULL;

// These threads are joinable:
pthread_t g_thr_id;
pthread_t g_sup_thr_id;

/* as_paxos_state_next
 * This is just a little bit of syntactic sugar around the transitions in
 * the state machine; it saves us from some nasty if/else constructions.
 * These #defines are here to avoid cluttering up the global namespace */
#define ACK 0
#define NACK 1
int
as_paxos_state_next(int s, int next)
{
	const int states[15][2] = {
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* UNDEF */
		{ AS_PAXOS_MSG_COMMAND_PREPARE_ACK, AS_PAXOS_MSG_COMMAND_PREPARE_NACK },  /* PREPARE */
		{ AS_PAXOS_MSG_COMMAND_COMMIT, AS_PAXOS_MSG_COMMAND_UNDEF },              /* PREPARE_ACK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* PREPARE_NACK */
		{ AS_PAXOS_MSG_COMMAND_COMMIT_ACK, AS_PAXOS_MSG_COMMAND_COMMIT_NACK },    /* COMMIT */
		{ AS_PAXOS_MSG_COMMAND_CONFIRM, AS_PAXOS_MSG_COMMAND_UNDEF },             /* COMMIT_ACK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* COMMIT_NACK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* CONFIRM */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* SYNC_REQUEST */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* SYNC */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* PARTITION_SYNC_REQUEST */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* PARTITION_SYNC */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* HEARTBEAT_EVENT */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF },               /* RETRANSMIT_CHECK */
		{ AS_PAXOS_MSG_COMMAND_UNDEF, AS_PAXOS_MSG_COMMAND_UNDEF }                /* SET_SUCC_LIST */
	};

	return(states[s][next]);
}

/*
 * Names of the Paxos command messages.
 * (NB:  Must match order and number of "AS_PAXOS_MSG_COMMAND_*" definitions in "paxos.h".)
 */
static char *as_paxos_cmd_name[] = {
	"UNDEF",
	"PREPARE",
	"PREPARE_ACK",
	"PREPARE_NACK",
	"COMMIT",
	"COMMIT_ACK",
	"COMMIT_NACK",
	"CONFIRM",
	"SYNC_REQUEST",
	"SYNC",
	"PARTITION_SYNC_REQUEST",
	"PARTITION_SYNC",
	"HEARTBEAT_EVENT",
	"RETRANSMIT_CHECK",
	"SET_SUCC_LIST"
};

/**
 * Log the succession list.
 *
 * @param msg the log record prefix. Cannot be NULL.
 * @param slist the succession list to log.
 * @param list_max_length the maximum length of the list.
 */
void as_paxos_log_succession_list(char *msg, cf_node slist[], int list_max_length)
{

	// Each byte of node id requires two bytes in hex, plus space for trailing
	// comma
	int print_buff_capacity = list_max_length * ((sizeof(cf_node) * 2) + 1);

	// For closing and opening parens.
	print_buff_capacity += 2;

	// For the message and the space separator
	print_buff_capacity += strnlen(msg, 100) + 1;

	// For NULL terminator
	print_buff_capacity += 1;

	char buff[print_buff_capacity];

	int used = 0;
	used += snprintf(buff, print_buff_capacity, "%s [", msg);

	int num_printed = 0;
	for (int i = 0; i < list_max_length && used < print_buff_capacity; i++) {
		if (slist[i] == (cf_node)0) {
			// End of list.
			break;
		}
		used += snprintf(buff + used, print_buff_capacity - used,
				 "%" PRIx64 ",", slist[i]);
		num_printed++;
	}

	if (num_printed){
		// Trim comma after the last node
		if (used - 1 < print_buff_capacity) {
			snprintf(buff + used - 1, print_buff_capacity - used + 1, "]");
		}
	} else {
		snprintf(buff + used, print_buff_capacity - used, "]");
	}

	// Force terminate the buffer in case sprintf has overflown.
	buff[print_buff_capacity - 1] = 0;

	cf_info(AS_PAXOS, "%s", buff);
}

void
dump_partition_state()
{
	/*
	 * Print out the data loss statistics
	 */
	char printbuf[100];
	int pos = 0; // location to print from
	printbuf[0] = '\0';

	cf_debug(AS_PAXOS, " Partition State Dump");

	for (int index = 0; index < AS_CLUSTER_SZ; index++) {
		if (g_paxos->succession[index] == (cf_node) 0) {
			break;
		}

		cf_debug(AS_PAXOS, " Node %"PRIx64"", g_paxos->succession[index]);
		for (int i = 0; i < g_config.n_namespaces; i++) {
			as_namespace *ns = g_config.namespaces[i];
			cf_debug(AS_PAXOS, " Name Space: %s", ns->name);
			int k = 0;
			as_partition_vinfo *parts = ns->cluster_vinfo[index];
			if (NULL == parts) {
				cf_debug(AS_PAXOS, " STATE is EMPTY");
				continue;
			}
			for (int j = 0; j < AS_PARTITIONS; j++) {
				int bytes = sprintf((char *) (printbuf + pos), " %"PRIx64"", parts[j].iid);
				if (bytes <= 0) {
					cf_debug(AS_PAXOS, "printing error. Bailing ...");
					return;
				}
				pos += bytes;
				if (k % 2 == 1) {
					cf_detail(AS_PAXOS, "%s", (char *) printbuf);
					pos = 0;
					printbuf[0] = '\0';
				}
				k++;
			}
		}
		if (pos > 0) {
			cf_debug(AS_PAXOS, "%s", (char *) printbuf);
			pos = 0;
			printbuf[0] = '\0';
		}
	}
} // end dump_partition_state()

void
as_paxos_print_cluster_key(const char *message)
{
	cf_debug(AS_PAXOS, "%s: cluster key %"PRIx64"", message, as_exchange_cluster_key());
}

/* as_paxos_sync_generate
 * Generate a Paxos synchronization message; returns a pointer to the message,
 * or NULL on error */
msg *
as_paxos_sync_msg_generate(uint64_t cluster_key)
{
	as_paxos *p = g_paxos;
	msg *m = NULL;
	int e = 0;

	cf_debug(AS_PAXOS, "SYNC sending cluster key %"PRIx64"", cluster_key);

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get fabric message");
		return(NULL);
	}

	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_SYNC);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, p->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, 0); // not used


	size_t cluster_limit = AS_CLUSTER_SZ;

	/* Include the succession list length in all Paxos protocol v2 or greater messages. Except for heartbeat version v3.*/
	if (AS_PMC_USE() && !AS_PAXOS_PROTOCOL_IS_V(1)) {
		cluster_limit = g_config.paxos_max_cluster_size;
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, cluster_limit);
	}

	e += msg_set_buf(m, AS_PAXOS_MSG_SUCCESSION, (uint8_t *)p->succession, cluster_limit * sizeof(cf_node), MSG_SET_COPY);
	e += msg_set_uint64(m, AS_PAXOS_MSG_CLUSTER_KEY, cluster_key);
	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to generate sync message");
		return(NULL);
	}

	return(m);
}

/* as_paxos_sync_msg_apply
 * Apply a synchronization message; returns 0 on success */
int
as_paxos_sync_msg_apply(msg *m)
{
	as_paxos *p = g_paxos;

	uint8_t *bufp = NULL;
	size_t bufsz = 0;

	uint64_t cluster_key = 0;

	int e = 0;
	cf_node self = g_config.self_node;

	cf_assert(m, AS_PAXOS, "invalid argument");

	as_paxos_generation gen;
	memset(&gen, 0, sizeof(as_paxos_generation));

	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &gen.sequence);
	// Older versions handled unused AS_PAXOS_MSG_GENERATION_PROPOSAL here.

	e += msg_get_buf(m, AS_PAXOS_MSG_SUCCESSION, &bufp, &bufsz, MSG_GET_DIRECT);
	e += msg_get_uint64(m, AS_PAXOS_MSG_CLUSTER_KEY, &cluster_key);

	if ((0 > e) || (NULL == bufp)) {
		cf_warning(AS_PAXOS, "unpacking sync message failed");
		return(-1);
	}

	cf_node succession[AS_CLUSTER_SZ];
	memset(succession, 0, sizeof(succession));
	memcpy(succession, bufp, bufsz);

	if (!as_hb_is_alive(succession[0])) {
		// This could happen is a new principal comes up, however this
		// node does not add it to its adjacency list because of a max
		// cluster size breach.
		cf_warning(AS_PAXOS,
			   "Sync message ignored from dead principal %" PRIx64,
			   succession[0]);
		return -1;
	}

	/* Check if we need to ignore this message */
	if (succession[0] == p->succession[0]) { // current succession list has same principal as local succession list
		if (!p->alive[0]) { // Let this through
			cf_info(AS_PAXOS, "Sync message received from a principal %"PRIx64" that is back from the dead?", p->succession[0]);
		}
		/* compare generations */
		if (gen.sequence < p->gen.sequence) {
			cf_warning(AS_PAXOS, "Sync message ignored from %"PRIx64" - [%d]@%"PRIx64" is arriving after [%d]@%"PRIx64,
					succession[0], gen.sequence, succession[0], p->gen.sequence, p->succession[0]);
			return -1;
		}
	}

	/* Apply the sync msg to the current state */
	p->gen.sequence = gen.sequence;

	memset(p->succession, 0, sizeof(p->succession));
	memcpy(p->succession, bufp, bufsz);

	cf_debug(AS_PAXOS, "SYNC getting cluster key %"PRIx64"", cluster_key);

	// Disallow migration requests into this node until we complete partition
	// rebalancing.
	as_partition_balance_disallow_migrations();

	// AER-4645 Important that setting cluster key follows disallow_migrations.
	as_exchange_cluster_key_set(cluster_key);
	cf_info(AS_PAXOS, "cluster key set to %lx", cluster_key);

	as_partition_balance_synchronize_migrations();

	/* Fix up the auxiliary state around the succession table and destroy
	 * any pending transactions */
	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		p->alive[i] = (0 != p->succession[i]) ? true : false;
		if (p->alive[i]) {
			cf_debug(AS_PAXOS, "setting succession[%d] = %"PRIx64" to alive", i, p->succession[i]);
		}
	}

	memset(p->pending, 0, sizeof(p->pending));

	as_paxos_current_init(p);

	/* If this succession list doesn't include ourselves, then fail */
	if (false == as_paxos_succession_ismember(self)) {
		cf_warning(AS_PAXOS, " sync message falied - succession list does not contain self");
		return(-1);
	}
	return(0);
}

/* as_paxos_partition_sync_request_msg_generate
 * Generate a Paxos partition synchronization request message; returns a pointer to the message,
 * or NULL on error */
msg *
as_paxos_partition_sync_request_msg_generate()
{
	as_paxos *p = g_paxos;
	msg *m = NULL;
	int e = 0;

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get fabric message");
		return(NULL);
	}

	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, p->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, 0); // not used

	/* Include the succession list length in all Paxos protocol v2 or greater messages. Except for heartbeat version v3.*/
	if (AS_PMC_USE() && !AS_PAXOS_PROTOCOL_IS_V(1)) {
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);
	}

	/*
	 * Normally partition locks need to be held for accessing partition vinfo
	 * In this case, however, migrates are disallowed when we are doing the copy
	 * and the partition_vinfo should be consistent
	 */
	size_t array_size = g_config.n_namespaces;
	cf_debug(AS_PAXOS, "Partition Sync request Array Size = %zu ", array_size);
	size_t elem_size = sizeof(as_partition_vinfo) * AS_PARTITIONS;

	if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITION, array_size, elem_size)) {
		cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
		return(NULL);
	}

	size_t n_elem = 0;
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_partition_vinfo vi[AS_PARTITIONS];
		for (int j = 0; j < AS_PARTITIONS; j++)
			memcpy(&vi[j], &g_config.namespaces[i]->partitions[j].version_info, sizeof(as_partition_vinfo));
		e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITION, n_elem, (uint8_t *)vi, elem_size);
		cf_debug(AS_PAXOS, "writing element %zu", n_elem);
		n_elem++;
	}

	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to generate sync message");
		return(NULL);
	}

	/* Include the partition sizes array in all Paxos protocol v3 or greater PARTITION_SYNC_REQUEST messages. */
	if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
		array_size = g_config.n_namespaces;
		cf_debug(AS_PAXOS, "Partitionsz Sync request Array Size = %zu", array_size);
		elem_size = sizeof(uint64_t) * AS_PARTITIONS;

		if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITIONSZ, array_size, elem_size)) {
			cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
			return(NULL);
		}

		n_elem = 0;
		for (int i = 0; i < g_config.n_namespaces; i++) {
			uint64_t partitionsz[AS_PARTITIONS] = { 0 };

			e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITIONSZ, n_elem, (uint8_t *)partitionsz, elem_size);
			cf_debug(AS_PAXOS, "writing element %zu", n_elem);
			n_elem++;
		}

		if (0 > e) {
			cf_warning(AS_PAXOS, "unable to generate sync message");
			return(NULL);
		}
	}

	return(m);
}

/* as_paxos_partition_sync_request_msg_apply
 * Apply a partition sync request  message; returns 0 on success */
int
as_paxos_partition_sync_request_msg_apply(msg *m, int n_pos)
{
	as_paxos *p = g_paxos;
	int e = 0;

	cf_assert(m, AS_PAXOS, "invalid argument");

	as_paxos_generation gen;
	memset(&gen, 0, sizeof(as_paxos_generation));

	/* We trust this state absolutely */
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &gen.sequence);
	// Older versions handled unused AS_PAXOS_MSG_GENERATION_PROPOSAL here.

	if (gen.sequence != p->gen.sequence) {
		cf_warning(AS_PAXOS, "sequence does not match (%"PRIu32", %"PRIu32") - partition sync request not applied",
				gen.sequence, p->gen.sequence);
		return -1;
	}

	size_t array_size = g_config.n_namespaces;

	int size;
	if (0 != msg_get_buf_array_size(m, AS_PAXOS_MSG_PARTITION, &size)) {
		cf_warning(AS_PAXOS, "Unable to read partition sync message");
		return(-1);
	}
	if (size != array_size) {
		cf_warning(AS_PAXOS, "Different number of namespaces (expected: %zu, received in partition sync message: %d) between nodes in same cluster ~~ Please check node configurations", array_size, size);
		return(-1);
	}

	/*
	 * reset the values of this node's partition version in the global list
	 */
	size_t elem = 0;
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		memset(ns->cluster_vinfo[n_pos], 0, sizeof(as_partition_vinfo) * AS_PARTITIONS);
		uint8_t *bufp = NULL;
		size_t bufsz = sizeof(as_partition_vinfo) * AS_PARTITIONS;
		e += msg_get_buf_array(m, AS_PAXOS_MSG_PARTITION, elem, &bufp, &bufsz, MSG_GET_DIRECT);
		elem++;
		if ((0 > e) || (NULL == bufp)) {
			cf_warning(AS_PAXOS, "unpacking partition sync request message failed");
			return(-1);
		}
		memcpy(ns->cluster_vinfo[n_pos], bufp, sizeof(as_partition_vinfo) * AS_PARTITIONS);
	}

	return(0);
}

/* as_paxos_partition_sync_msg_generate
 * Generate a Paxos partition synchronization message; returns a pointer to the message,
 * or NULL on error */
msg *
as_paxos_partition_sync_msg_generate()
{
	as_paxos *p = g_paxos;
	msg *m = NULL;
	int e = 0;

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get fabric message");
		return(NULL);
	}

	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_PARTITION_SYNC);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, p->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, 0); // not used


	size_t cluster_limit = AS_CLUSTER_SZ;
	/* Include the succession list length in all Paxos protocol v2 or greater messages. Except for heartbeat version v3.*/
	if (AS_PMC_USE() && !AS_PAXOS_PROTOCOL_IS_V(1)) {
		cluster_limit = g_config.paxos_max_cluster_size;
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, cluster_limit);
	}

	e += msg_set_buf(m, AS_PAXOS_MSG_SUCCESSION, (uint8_t *)p->succession, cluster_limit * sizeof(cf_node), MSG_SET_COPY);

	/*
	 * Create a message with the global partition version info
	 */
	/*
	 * find cluster size
	 */
	size_t cluster_size = 0;
	for (int j = 0; j < AS_CLUSTER_SZ; j++) {
		if (p->succession[j] != (cf_node)0) {
			cluster_size++;
		} else {
			break;
		}
	}

	if (2 > cluster_size) {
		cf_warning(AS_PAXOS, "Cluster size is wrong %zu. unable to set fabric message", cluster_size);
		return(NULL);
	}

	size_t array_size = cluster_size * g_config.n_namespaces;
	cf_debug(AS_PAXOS, "Array Size = %zu", array_size);
	size_t elem_size = sizeof(as_partition_vinfo) * AS_PARTITIONS;

	if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITION, array_size, elem_size)) {
		cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
		return(NULL);
	}

	size_t n_elem = 0;
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		for (int j = 0; j < cluster_size; j++) {
			cf_debug(AS_PAXOS, "writing element %zu", n_elem);
			e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITION, n_elem, (uint8_t *)ns->cluster_vinfo[j], elem_size);
			if (0 > e) {
				cf_warning(AS_PAXOS, "unable to generate sync message");
				return(NULL);
			}
			n_elem++;
		}
	}
	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to generate sync message");
		return(NULL);
	}

	/* Include the partition sizes array in all Paxos protocol v3 or greater PARTITION_SYNC messages. */
	if (AS_PAXOS_PROTOCOL_IS_AT_LEAST_V(3)) {
		array_size = cluster_size * g_config.n_namespaces;
		elem_size = sizeof(uint64_t) * AS_PARTITIONS;

		if (0 != msg_set_buf_array_size(m, AS_PAXOS_MSG_PARTITIONSZ, array_size, elem_size)) {
			cf_warning(AS_PAXOS, "Cannot allocate array buffer. unable to set fabric message");
			return(NULL);
		}

		n_elem = 0;
		uint64_t partitionsz[AS_PARTITIONS] = { 0 };

		for (int i = 0; i < g_config.n_namespaces; i++) {
			for (int j = 0; j < cluster_size; j++) {
				e += msg_set_buf_array(m, AS_PAXOS_MSG_PARTITIONSZ, n_elem, (uint8_t *)partitionsz, elem_size);

				if (0 > e) {
					cf_warning(AS_PAXOS, "unable to generate sync message");
					return(NULL);
				}
				n_elem++;
			}
		}

		if (0 > e) {
			cf_warning(AS_PAXOS, "unable to generate sync message");
			return(NULL);
		}
	}

	if (cf_context_at_severity(AS_PAXOS, CF_DEBUG)) {
		dump_partition_state();
	}

	return(m);
}

/* as_paxos_partition_sync_msg_apply
 * Apply a partition synchronization message; returns 0 on success */
int
as_paxos_partition_sync_msg_apply(msg *m)
{
	as_paxos *p = g_paxos;
	uint8_t *bufp = NULL;
	size_t bufsz = 0;
	int e = 0;

	cf_assert(m, AS_PAXOS, "invalid argument");

	as_paxos_generation gen;
	memset(&gen, 0, sizeof(as_paxos_generation));

	/* We trust this state absolutely */
	e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &gen.sequence);
	// Older versions handled unused AS_PAXOS_MSG_GENERATION_PROPOSAL here.
	if (gen.sequence != p->gen.sequence) {
		cf_detail(AS_PAXOS, "sequence do not match. partition sync message not applied");
		return -1;
	}

	e += msg_get_buf(m, AS_PAXOS_MSG_SUCCESSION, &bufp, &bufsz, MSG_GET_DIRECT);

	if ((0 > e) || (NULL == bufp)) {
		cf_warning(AS_PAXOS, "unpacking succession list from partition sync message failed");
		return(-1);
	}

	/*
	 * Make sure that the bits are identical
	 */
	if (0 != memcmp(p->succession, bufp, bufsz)) {
		cf_warning(AS_PAXOS, "succession lists mismatch from partition sync message");
		return(-1);
	}

	/*
	 * find cluster size
	 */
	size_t cluster_size = 0;
	for (int j = 0; j < AS_CLUSTER_SZ; j++) {
		if (p->succession[j] != (cf_node)0) {
			cluster_size++;
		} else {
			break;
		}
	}

	if (2 > cluster_size) {
		cf_warning(AS_PAXOS, "Cluster size is wrong %zu. unable to apply partition sync message", cluster_size);
		return(-1);
	}

	/*
	 * Check if the state of this node is correct for applying a partition sync message
	 */
	if (as_partition_balance_are_migrations_allowed() == true) {
		cf_info(AS_PAXOS, "Node allows migrations. Ignoring duplicate partition sync message.");
		return(-1);
	}

	size_t array_size = cluster_size * g_config.n_namespaces;

	int size;
	if (0 != msg_get_buf_array_size(m, AS_PAXOS_MSG_PARTITION, &size)) {
		cf_warning(AS_PAXOS, "Unable to read partition sync message");
		return(-1);
	}
	if (size != array_size) {
		cf_warning(AS_PAXOS, "Expected array size %zu, received %d, Unable to read partition sync message", array_size, size);
		return(-1);
	}

	/*
	 * reset the values of this node's partition version in the global list
	 */
	size_t elem = 0;
	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		for (int j = 0; j < cluster_size; j++) {
			memset(ns->cluster_vinfo[j], 0, sizeof(as_partition_vinfo) * AS_PARTITIONS);
			uint8_t *bufp = NULL;
			bufsz = sizeof(as_partition_vinfo) * AS_PARTITIONS;
			e += msg_get_buf_array(m, AS_PAXOS_MSG_PARTITION, elem, &bufp, &bufsz, MSG_GET_DIRECT);
			elem++;
			if ((0 > e) || (NULL == bufp)) {
				cf_warning(AS_PAXOS, "unpacking partition sync message failed");
				return(-1);
			}
			memcpy(ns->cluster_vinfo[j], bufp, sizeof(as_partition_vinfo) * AS_PARTITIONS);
		}
	}

	if (cf_context_at_severity(AS_PAXOS, CF_DEBUG)) {
		dump_partition_state();
	}

	return(0);
}

/* as_paxos_succession_insert
 * Insert a node into the succession list, maintaining descending sorted order; return 0 on success */
int
as_paxos_succession_insert(cf_node n)
{
	as_paxos *p = g_paxos;
	int i;

	for (i = 0; i < AS_CLUSTER_SZ; i++) {
		// Node already exists - May happen in some rare error cases.
		if (n == p->succession[i]) {
			cf_warning(AS_PAXOS, "New node %"PRIx64" already found in Paxos succession list", n);
			p->alive[i] = true;
			break;
		}
		// Found the end of the list - Insert node here.
		if (0 == p->succession[i]) {
			p->succession[i] = n;
			p->alive[i] = true;
			break;
		}
		// Found where this node belongs - Shift the other nodes down and insert node here.
		if (n > p->succession[i]) {
			// We can only shift N-i-1 elements without overflowing memmory
			memmove(&p->succession[i + 1], &p->succession[i], (AS_CLUSTER_SZ - i - 1) * sizeof(cf_node));
			memmove(&p->alive[i + 1], &p->alive[i], (AS_CLUSTER_SZ - i - 1) * sizeof(bool));
			p->succession[i] = n;
			p->alive[i] = true;
			break;
		}
	}

	if (AS_CLUSTER_SZ == i) {
		return(-1);
	} else {
		if (p->succession[AS_CLUSTER_SZ - 1] != 0) {
			cf_debug(AS_PAXOS, "Lost zero sentinal element in paxos succession list");
		}
		return(0);
	}
}

/* as_paxos_succession_remove
 * Remove a node from the succession list; return 0 on success */
int
as_paxos_succession_remove(cf_node n)
{
	as_paxos *p = g_paxos;
	int i;

	bool found = false;
	/* Find the offset into the succession list of the failed node */
	for (i = 0; i < AS_CLUSTER_SZ; i++) {
		if (p->succession[i] == 0) {
			break;
		}
		if (n == p->succession[i]) {
			found = true;
			break;
		}
	}
	if (found == false) {
		cf_info(AS_PAXOS, "Departed node %"PRIx64" is not found in paxos succession list", n);
		return(0);
	}

	/* Remove the node from the succession, applying a little bit of
	 * optimization to avoid unnecessary memmove()s */
	if (((AS_CLUSTER_SZ) - 1 == i) || (0 == p->succession[i + 1])) {
		p->succession[i] = 0;
		p->alive[i] = false;
	} else {
		memmove(&p->succession[i], &p->succession[i + 1], ((AS_CLUSTER_SZ - i) - 1) * sizeof(cf_node));
		memmove(&p->alive[i], &p->alive[i + 1], ((AS_CLUSTER_SZ - i) - 1) * sizeof(bool));
		// zero-out the element at the end of the array which will be old value
		p->succession[AS_CLUSTER_SZ-1] = 0;
		p->alive[AS_CLUSTER_SZ-1] = false;
	}

	/* Fix up any votes in progress, since vote-keeping is indexed on
	 * position within the succession list */
	for (int j = 0; j < AS_PAXOS_ALPHA; j++) {
		if ((0 == p->pending[j].gen.sequence) || (p->pending[j].confirmed))
			continue;

		if ((AS_CLUSTER_SZ - 1 == i) || (0 == p->succession[i + 1]))
			p->pending[j].votes[i] = false;
		else
			memmove(&p->pending[j].votes[i], &p->pending[j].votes[i + 1], ((AS_CLUSTER_SZ - i) - 1) * sizeof(bool));
	}

	return(0);
}

/* as_paxos_succession_getprincipal
 * Get the head of the Paxos succession list, or zero if there is none */
cf_node
as_paxos_succession_getprincipal()
{
	as_paxos *p = g_paxos;

	if (!p) {
		cf_warning(AS_PAXOS, "Paxos is not yet initialized ~~ returning NULL principal");
		return 0;
	}

	/* Find the first living node in the succession */
	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if (p->alive[i]) {
			return(p->succession[i]);
		}
	}

	return 0;
}

/* as_paxos_succession_ismember
 * Returns true if the specified node is in the succession and alive */
bool
as_paxos_succession_ismember(cf_node n)
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if ((n == p->succession[i]) && p->alive[i]) {
			return(true);
		}
	}

	return(false);
}

/* as_paxos_set_protocol
 * Set the Paxos protocol version.
 * Returns 0 if successful, else returns -1.  */
int
as_paxos_set_protocol(paxos_protocol_enum protocol)
{
	if (protocol != AS_PAXOS_PROTOCOL_V5) {
		cf_warning(AS_PAXOS, "can't set paxos protocol to anything but v5");
		return -1;
	}

	if (g_config.paxos_protocol == AS_PAXOS_PROTOCOL_V5) {
		cf_info(AS_PAXOS, "paxos protocol is already v5");
		return -1;
	}

	if (as_hb_protocol_get() != AS_HB_PROTOCOL_V3) {
		cf_warning(AS_PAXOS,
				"paxos protocol v5 requires heartbeat protocol v3");
		return -1;
	}

	if (!as_partition_balance_are_migrations_allowed()) {
		cf_warning(AS_PAXOS,
				"switch to paxos protocol v5 requires stable cluster");
		return -1;
	}

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];

		int64_t initial_tx = (int64_t)ns->migrate_tx_partitions_initial;
		int64_t initial_rx = (int64_t)ns->migrate_rx_partitions_initial;
		int64_t remaining_tx = (int64_t)ns->migrate_tx_partitions_remaining;
		int64_t remaining_rx = (int64_t)ns->migrate_rx_partitions_remaining;
		int64_t initial = initial_tx + initial_rx;
		int64_t remaining = remaining_tx + remaining_rx;

		if (initial > 0 && remaining > 0) {
			cf_warning(AS_PAXOS,
					"switch to paxos protocol v5 requires migrations to be complete for namespace %s",
					ns->name);
			return -1;
		}
	}

	// Switch to new world!
	g_config.paxos_protocol = AS_PAXOS_PROTOCOL_V5;

	cf_info(AS_PAXOS, "stopping old paxos module ...");

	pthread_join(g_thr_id, NULL);
	pthread_join(g_sup_thr_id, NULL);

	cf_info(AS_PAXOS,
			"old paxos stopped - starting new exchange and clustering modules ...");

	as_partition_balance_jump_versions();

	as_exchange_start();
	as_clustering_switchover(as_exchange_cluster_key(),
			as_exchange_cluster_size(), as_exchange_succession(),
			g_paxos->gen.sequence);

	as_smd_convert_sindex_module();

	return 0;
}

/**
 * @return true if this node is in a single node cluster with self as the principal.
 */
bool as_paxos_is_single_node_cluster()
{
	as_paxos *p = g_paxos;

	// For a single node cluster this node should be the first / principal in
	// the succession list and the max cluster size shoulf be 1 or then the
	// succession list sould have only one element.
	return (p->succession[0] == g_config.self_node) && (p->succession[1] == 0);
}

/* as_paxos_partition_sync_states_all
 * Returns true if all nodes currently in the cluster (i.e., where "alive" is true)
 * have already sent in a PARTITION_SYNC_REQUEST message. */
bool
as_paxos_partition_sync_states_all()
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		// It is important to check the p->alive[] flag here. If a node has just departed, we return
		// true so that this paxos reconfiguration can complete
		if (p->alive[i] && (false == p->partition_sync_state[i])) {
			return(false);
		}
		if ((p->alive[i] == false) && ((cf_node)0 != p->succession[i])) {
			cf_debug(AS_PAXOS, "Node %"PRIx64" appears to have departed during a paxos vote", p->succession[i]);
		}
	}

	return(true);
}

/* as_paxos_set_partition_sync_state
 * Returns true if the specified node is in the succession and alive
 * Sets the partition sync state to be true */
bool
as_paxos_set_partition_sync_state(cf_node n)
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if ((n == p->succession[i]) && p->alive[i]) {
			p->partition_sync_state[i] = true;
			return(true);
		}
	}

	return(false);
}

/* as_paxos_get_succession_index
 * Returns the position of this node in the succession list */
int
as_paxos_get_succession_index(cf_node n)
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if ((n == p->succession[i]) && p->alive[i]) {
			return(i);
		}
	}

	return(-1);
}

/* as_paxos_succession_setdeceased
 * Mark a node in the succession list as deceased */
void
as_paxos_succession_setdeceased(cf_node n)
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if ((n == p->succession[i]) && p->alive[i]) {
			p->alive[i] = false;
			break;
		}
	}
}

/* as_paxos_succession_setrevived
 * Mark a node in the succession list as alive */
void
as_paxos_succession_setrevived(cf_node n)
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if (n == p->succession[i]) {
			if (! p->alive[i]) {
				cf_info(AS_PAXOS, "Node %"PRIx64" revived", n);
				p->alive[i] = true;
			}
			break;
		}
	}
}

/* as_paxos_succession_quorum
 * Return true if a quorum of the nodes in the succession list are alive */
bool
as_paxos_succession_quorum()
{
	as_paxos *p = g_paxos;
	int a = 0, c = 0;
	bool r;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i]) {
			break;
		}
		if (0 != p->succession[i]) {
			c++;
			if (p->alive[i])
				a++;
		}
	}

	r = (a >= ((c >> 1) + 1)) ? true : false;
	return(r);
}

void
as_paxos_current_init(as_paxos *p)
{
	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		p->current[i] = NULL;
	}

	p->current[0] = &p->pending[0];
	p->current[0]->gen.sequence = p->gen.sequence;
}

// Gets the transaction candidate with the largest principal.
// Currently only used in an ignored NACK message.
as_paxos_transaction *
as_paxos_current_get()
{
	as_paxos *p = g_paxos;

	as_paxos_transaction *max = p->current[0];

	for (int i = 1; i < AS_CLUSTER_SZ; i++) {
		if (NULL == p->current[i])
			return(max);

		if (p->current[i]->c.p_node > max->c.p_node)
			max = p->current[i];
	}

	return(max);
}

// Get the sequence number to use for the next transaction.
uint32_t
as_paxos_sequence_getnext()
{
	as_paxos* p = g_paxos;

	as_hlc_timestamp now = as_hlc_timestamp_now();
	cf_clock hlc_physical_ts = as_hlc_physical_ts_get(now);
	uint32_t time_seconds = (uint32_t)(hlc_physical_ts / 1000);

	// Use the max of the last sequence number + 1 and the clock, to prevent
	// the sequence number for sliding back just in case.
	return MAX(p->gen.sequence + 1, time_seconds);
}

// Checks to see if this transaction has the highest sequence for a
// transaction with this principal.
bool
as_paxos_current_is_candidate(as_paxos_transaction t)
{
	if (!as_hb_is_alive(t.c.p_node)) {
		// This could happen is a new principal comes up, however this
		// node does not add it to its adjacency list because of a max
		// cluster size breach.
		cf_info(AS_PAXOS, "Rejecting prepare from node %"PRIx64" not in adjacency list.",
			   t.c.p_node);
		return false;
	}

	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (NULL == p->current[i])
			return(true);

		// if lesser sequence for same master, reject
		if (t.c.p_node == p->current[i]->c.p_node
				&& t.gen.sequence <= p->current[i]->gen.sequence)
			return(false);
	}

	return(true);
}

// Add a transaction with a new principal to the current array,
// or update the transaction for a principal already in the array.
void
as_paxos_current_update(as_paxos_transaction *t)
{
	as_paxos *p = g_paxos;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (NULL == p->current[i] || t->c.p_node == p->current[i]->c.p_node) {
			p->current[i] = t;
			return;
		}
	}
}

/* as_paxos_transaction_search
 * Search the pending transaction table for a transaction with a specific
 * generation; return NULL if no corresponding transaction is found */
as_paxos_transaction *
as_paxos_transaction_search(as_paxos_transaction s)
{
	as_paxos *p = g_paxos;
	as_paxos_transaction *t = NULL;

	/* Scan through the list until we find a transaction with a matching
	 * generation */
	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (p->pending[i].c.p_node == s.c.p_node && p->pending[i].gen.sequence == s.gen.sequence) {
			t = &p->pending[i];
			break;
		}
	}

	return(t);
}

/* as_paxos_transaction_compare
 * Compare two transactions; return 0 if they are equal, nonzero otherwise */
int
as_paxos_transaction_compare(as_paxos_transaction *t1, as_paxos_transaction *t2)
{
	cf_assert(t1, AS_PAXOS, "invalid transaction (t1)");
	cf_assert(t2, AS_PAXOS, "invalid transaction (t2)");

	if (t1->gen.sequence == t2->gen.sequence &&
			0 == memcmp(&t1->c, &t2->c, sizeof(as_paxos_change))) {
		return 0;
	}
	else {
		return -1;
	}
}

/* as_paxos_transaction_update
 * Update a pending transaction with the information from a successor */
void
as_paxos_transaction_update(as_paxos_transaction *oldt, as_paxos_transaction *newt)
{
	cf_assert(oldt, AS_PAXOS, "invalid transaction (oldt)");
	cf_assert(newt, AS_PAXOS, "invalid transaction (newt)");

	oldt->gen.sequence = newt->gen.sequence;
	memcpy(&oldt->c, &newt->c, sizeof(oldt->c));
}

/* as_paxos_transaction_establish
 * Establish a new transaction in the first available slot in the
 * pending transaction table; return NULL on internal error */
as_paxos_transaction *
as_paxos_transaction_establish(as_paxos_transaction *s)
{
	as_paxos *p = g_paxos;
	as_paxos_transaction *t = NULL, *oldest = NULL;

	cf_assert(s, AS_PAXOS, "invalid transaction");

	/* Scan through the list to find an empty slot; if none was found, reuse
	 * the oldest retired transaction */
	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (0 == p->pending[i].gen.sequence) {
			t = &p->pending[i];
			break;
		}

		/* Keep a pointer to the oldest transaction we've seen (retired take priority over non-retired) */
		if (!oldest
			|| (p->pending[i].retired && !oldest->retired)
			|| (p->pending[i].retired == oldest->retired
				&& (p->pending[i].gen.sequence < oldest->gen.sequence))) {
			oldest = &p->pending[i];
		}
	}

	/* Sanity checking to make sure we found a slot, reusing the one
	 * occupied by the oldest transaction if necessary */
	if (!t) {
		cf_debug(AS_PAXOS, "reusing oldest transaction slot");
		t = oldest;
	}

	/* Copy the transaction information into the slot; update the current
	 * pointer */
	memcpy(t, s, sizeof(as_paxos_transaction));
	t->retired = false;
	t->confirmed = false;
	t->establish_time = cf_getms();
	memset(t->votes, 0, sizeof(t->votes));
	t->election_cycle = AS_PAXOS_MSG_COMMAND_PREPARE;
	as_paxos_current_update(t);

	return(t);
}

/* as_paxos_transaction_confirm
 * Mark a transaction as confirmed */
void
as_paxos_transaction_confirm(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, "invalid argument");

	t->confirmed = true;

	as_paxos *p = g_paxos;
	// We also have to confirm transactions that have sequence numbers less than this transaction.
	// It is possible that messages have been lost
	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if ((p->pending[i].gen.sequence != 0)
			&& (p->pending[i].gen.sequence < t->gen.sequence)
			&& !(p->pending[i].confirmed)) {
			p->pending[i].confirmed = true;
			break;
		}
	}
}

/* as_paxos_transaction_retire
 * Mark a transaction as retired */
void
as_paxos_transaction_retire(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, "invalid argument");

	t->retired = true;
}

/* as_paxos_transaction_destroy
 * Destroy the contents of a pointer to a pending transaction */
void
as_paxos_transaction_destroy(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, "invalid argument");
	memset(t, 0, sizeof(as_paxos_transaction));
}

/* as_paxos_transaction_vote
 * Count a vote for a pending transaction.  s is a pointer to the
 * corresponding entry in the pending transaction table, n is the cf_node
 * whose vote we are counting, and t is a pointer to the transaction they
 * think they are voting for.  Return as follows:
 *    ..REJECT if the vote has been rejected
 *    ..ACCEPT if the vote has been accepted, but a quorum hasn't been reached
 *    ..QUORUM if the vote has been accepted and a quorum has been reached
 */
as_paxos_transaction_vote_result
as_paxos_transaction_vote(as_paxos_transaction *s, cf_node n, as_paxos_transaction *t)
{
	as_paxos *p = g_paxos;
	as_paxos_transaction_vote_result r;
	int c = 0, v = 0;

	cf_assert(s, AS_PAXOS, "invalid transaction (s)");
	cf_assert(t, AS_PAXOS, "invalid transaction (t)");

	/* Make sure that the transaction we're voting on is the same one we
	 * have in the pending transaction table */
	if (0 != as_paxos_transaction_compare(s, t))
		return(AS_PAXOS_TRANSACTION_VOTE_REJECT);

	/* Record the vote, counting the number of living nodes, c, and the
	 * number of votes, v, as we go */
	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (0 == p->succession[i])
			break;

		if (p->alive[i])
			c++;

		if (n == p->succession[i]) {
			s->votes[i] = true;
		}

		if (s->votes[i])
			v++;
	}

	//r = ((v >= ((c >> 1) + 1)) || ((v == c) && (v == 1))) ? AS_PAXOS_TRANSACTION_VOTE_QUORUM : AS_PAXOS_TRANSACTION_VOTE_ACCEPT;
	r = (v == ((c >> 1) + 1)) ? AS_PAXOS_TRANSACTION_VOTE_QUORUM : AS_PAXOS_TRANSACTION_VOTE_ACCEPT;
	return(r);
}

/* as_paxos_transaction_vote_reset
 * Reset the count of votes for a transaction */
void
as_paxos_transaction_vote_reset(as_paxos_transaction *t)
{
	cf_assert(t, AS_PAXOS, "invalid argument");

	for (int i = 0; i < AS_CLUSTER_SZ; i++)
		t->votes[i] = false;
}

/* as_paxos_transaction_getnext
 * Get the next applicable transaction */
as_paxos_transaction *
as_paxos_transaction_getnext(cf_node master_node)
{
	as_paxos *p = g_paxos;
	as_paxos_transaction *t = NULL;

	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (p->pending[i].c.p_node == master_node && p->pending[i].confirmed &&
				(p->pending[i].gen.sequence > p->gen.sequence)) {
			t = &p->pending[i];
			break;
		}
	}

	return(t);
}

void
as_paxos_send_sync_messages() {
	as_paxos *p = g_paxos;
	uint64_t cluster_key = as_exchange_cluster_key();
	msg *reply = NULL;

	if (NULL == (reply = as_paxos_sync_msg_generate(cluster_key))) {
		cf_warning(AS_PAXOS, "unable to construct sync message");
		return;
	}

	char sbuf[(AS_CLUSTER_SZ * 17) + 49];
	snprintf(sbuf, 49, "SUCCESSION [%d]@%"PRIx64": ", p->gen.sequence, g_config.self_node);
	snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", p->succession[0]);

	/*
	 * Note the fact that we have send a sync message.
	 */
	p->num_sync_attempts++;

	for (int i = 1; i < AS_CLUSTER_SZ; i++) {
		if (p->succession[i] == 0) {
			break;
		}
		snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", p->succession[i]);
		if (p->partition_sync_state[i] == false) {
			msg_incr_ref(reply);
			cf_info(AS_PAXOS, "sending sync message to %"PRIx64"", p->succession[i]);
			if (0 != as_fabric_send(p->succession[i], reply, AS_FABRIC_CHANNEL_CTRL)) {
				cf_warning(AS_PAXOS, "sync message to %"PRIx64" lost in fabric", p->succession[i]);
				as_fabric_msg_put(reply);
			}
		}
	}

	as_fabric_msg_put(reply);
	cf_info(AS_PAXOS, "%s", sbuf);
	as_paxos_print_cluster_key("COMMAND CONFIRM");
}

void as_paxos_start_second_phase()
{
	as_paxos *p = g_paxos;
	cf_node self = g_config.self_node;

	/*
	 * Generate one uuid and use this for the cluster key
	 */
	uint64_t cluster_key = 0;

	// Generate a non-zero cluster key that fits in 7 bytes.
	while ((cluster_key = (cf_get_rand64() >> 8)) == 0) {
		;
	}

	// Disallow migration requests into this node until we complete partition
	// rebalancing.
	as_partition_balance_disallow_migrations();

	// AER-4645 Important that setting cluster key follows disallow_migrations.
	as_exchange_cluster_key_set(cluster_key);
	cf_info(AS_PAXOS, "cluster key set to %lx", cluster_key);

	as_partition_balance_synchronize_migrations();

	/* Earlier code used to synchronize only new members. However, this code is changed to
	 * send sync messages to all members of the cluster. On receiving a sync, all nodes are expected to send
	 * the partition state back to the cluster master (principal) */
	if (p->succession[0] != self)
		cf_warning(AS_PAXOS, "Principal is not first in the succession list!");

	/* Principal's state is always local. Copy its current partition
	 * version information into global table and set state flag.
	 * Note that the index for the principal is 0 */

	for (int i = 0; i < g_config.n_namespaces; i++) {
		as_namespace *ns = g_config.namespaces[i];
		memset(ns->cluster_vinfo[0], 0, sizeof(as_partition_vinfo) * AS_PARTITIONS);
		for (int j = 0; j < AS_PARTITIONS; j++) {
			ns->cluster_vinfo[0][j] = ns->partitions[j].version_info;
		}
	}
	p->partition_sync_state[0] = true; /* Principal's state is always local */
	for (int i = 1; i < AS_CLUSTER_SZ; i++) {
		p->partition_sync_state[i] = false;
	}

	as_paxos_send_sync_messages();
}

/* as_paxos_transaction_apply
 * Apply all confirmed pending transactions */
void
as_paxos_transaction_apply(cf_node from_id)
{
	as_paxos *p = g_paxos;
	as_paxos_transaction *t = NULL;
	cf_node self = g_config.self_node;
	int n_xact = 0;

	/*
	 * Apply all the changes that are part of this transaction but only if the node is principal
	 * Note that all the changes need to be applied before any synchronization messages are sent out
	 * The non principals will have their changes applied when they receive sync messages
	 * Non-principal transactions are also retired here to reclaim the space in the transaction table
	 */
	if (self != as_paxos_succession_getprincipal()) {
		while (NULL != (t = as_paxos_transaction_getnext(from_id))) {
			/* Update the generation count in anticipation of the sync message*/
			p->gen.sequence = t->gen.sequence;
			n_xact++;
			cf_debug(AS_PAXOS, "Non-principal retiring transaction %p (nc %d) from %"PRIx64, t, t->c.n_change, from_id);
			as_paxos_transaction_retire(t);
		}

		cf_debug(AS_PAXOS, "{%d} non-principal retired %d transactions",
				p->gen.sequence, n_xact);

		return;
	}

	while (NULL != (t = as_paxos_transaction_getnext(self))) {
		cf_debug(AS_PAXOS, "{%d} applying transaction %p",
				p->gen.sequence, t);

		if ((t->c.p_node != 0) && (t->c.p_node != as_paxos_succession_getprincipal()))
			cf_warning(AS_PAXOS, "Applying transaction not from %"PRIx64" principal is %"PRIx64"",
					   t->c.p_node, as_paxos_succession_getprincipal());

		if ((t->c.p_node == 0))
			cf_warning(AS_PAXOS, "Applying transaction from null principal node");

		/* Update the generation count */
		p->gen.sequence = t->gen.sequence;
		for (int i = 0; i < t->c.n_change; i++) {
			switch (t->c.type[i]) {
				case AS_PAXOS_CHANGE_NOOP:
					break;
				case AS_PAXOS_CHANGE_SUCCESSION_ADD:
					if (g_config.self_node == t->c.id[i]) {
						cf_warning(AS_PAXOS, "Found self %"PRIx64" on the succession list!", g_config.self_node);
					}
					cf_info(AS_PAXOS, "inserting node %"PRIx64"", t->c.id[i]);
					n_xact++;
					if (0 != as_paxos_succession_insert(t->c.id[i]))
						cf_crash(AS_PAXOS, "succession list full");

					break;
				case AS_PAXOS_CHANGE_SUCCESSION_REMOVE:
					cf_info(AS_PAXOS, "removing failed node %"PRIx64"", t->c.id[i]);
					if (p->principal_pro_tempore == t->c.id[i]) {
						cf_info(AS_PAXOS, "removed node %"PRIx64" is no longer principal pro tempore", t->c.id[i]);
						p->principal_pro_tempore = 0;
					}
					n_xact++;
					if (0 != as_paxos_succession_remove(t->c.id[i]))
						cf_warning(AS_PAXOS, "unable to remove node from succession: %"PRIx64"", t->c.id[i]);

					if (g_config.self_node == t->c.id[i])
						cf_warning(AS_PAXOS, "voted off the island! - ignoring...");

					break;
				case AS_PAXOS_CHANGE_UNKNOWN:
				default:
					cf_warning(AS_PAXOS, "unknown command, ignoring");
					break;
			}
		}

		cf_debug(AS_PAXOS, "{%d} principal retiring transaction %p",
				p->gen.sequence, t);
		as_paxos_transaction_retire(t);
	}

	cf_debug(AS_PAXOS, "Principal applied %d transactions", n_xact);

	if (0 == n_xact) {
		// No transactions have been applied. So do not send sync messages.
		// This is most likely because confirmation messages have crossed over
		cf_warning(AS_PAXOS, "{%d} no changes applied on paxos confirmation message, principal is %"PRIx64" - no sync messages will be sent",
				p->gen.sequence, as_paxos_succession_getprincipal());
		return;
	}

	p->need_to_rebalance = true;
}

/* as_paxos_wire_change_create
 * Create an as_paxos_change object. */
static as_paxos_wire_change *
as_paxos_wire_change_create(size_t cluster_limit)
{
	as_paxos_wire_change *wc;

	int wc_sz = sizeof(as_paxos_wire_change) + (sizeof(uint8_t) + sizeof(cf_node)) * cluster_limit;

	if (!(wc = cf_malloc(wc_sz))) {
		cf_crash(AS_PAXOS, "Failed to allocate an as_paxos_wire_change of size %d", wc_sz);
		return 0;
	}

	return wc;
}

/* as_paxos_wire_change_initialize
 * Create and initialize an as_paxos_wire_change object from an as_paxos_change object. */
static int
as_paxos_wire_change_initialize(as_paxos_wire_change **pwc, as_paxos_change *c, size_t cluster_limit)
{
	as_paxos_wire_change *wc;

	if (!(wc = as_paxos_wire_change_create(cluster_limit))) {
		cf_crash(AS_PAXOS, "Failed to allocate an as_paxos_wire_change");
		return -1;
	}

	wc->p_node = c->p_node;
	wc->n_change = c->n_change;

	for (int i = 0; i < cluster_limit; i++)
		wc->payload[i] = c->type[i];

	for (int i = 0; i < cluster_limit; i++)
		*(cf_node *)&(wc->payload[cluster_limit + i * sizeof(cf_node)]) = c->id[i];

	*pwc = wc;
	return 0;
}

/* as_paxos_wire_change_destroy
 * Free a previously-allocated as_paxos_wire_change object. */
static void
as_paxos_wire_change_destroy(as_paxos_wire_change *wc)
{
	cf_free(wc);
}

/* as_paxos_change_copy_from_as_paxos_wire_change
 * Copy the contents an as_paxos_change object from an as_paxos_wire_change object. */
static int
as_paxos_change_copy_from_as_paxos_wire_change(as_paxos_change *c, as_paxos_wire_change *wc, size_t wc_sz)
{
	c->p_node = wc->p_node;
	c->n_change = wc->n_change;

	// ( total buffer size - header size ) / (size of one node data which is (c->type + c->id)).
	int cluster_limit = (wc_sz - sizeof(as_paxos_wire_change)) / (sizeof(uint8_t) + sizeof(cf_node));

	for (int i = 0; i < cluster_limit; i++) {
		c->type[i] = wc->payload[i];
	}

	for (int i = 0; i < cluster_limit; i++) {
		c->id[i] = *(cf_node *) & (wc->payload[cluster_limit + i * sizeof(cf_node)]);
	}

	return 0;
}

/* as_paxos_msg_wrap
 * Encapsulate a Paxos transaction into a new message structure; returns a
 * pointer to the message, or NULL on error */
msg *
as_paxos_msg_wrap(as_paxos_transaction *t, uint32_t c)
{
	msg *m = NULL;
	int e = 0;

	if (!AS_PAXOS_ENABLED()) {
		cf_warning(AS_PAXOS, "Paxos messaging disabled ~~ Not wrapping message");
		return(NULL);
	}

	cf_assert(t, AS_PAXOS, "invalid transaction");

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "unable to get a fabric message");
		return(NULL);
	}

	/* Wrap up the message contents; track all the return values as we go */
	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, c);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, t->gen.sequence);
	e += msg_set_uint32(m, AS_PAXOS_MSG_GENERATION_PROPOSAL, 0); // not used

	size_t cluster_limit = AS_CLUSTER_SZ;

	/* Include the succession list length in all Paxos protocol v2 or greater messages. Except for heartbeat version v3.*/
	if (AS_PMC_USE() && !AS_PAXOS_PROTOCOL_IS_V(1)) {
		cluster_limit = g_config.paxos_max_cluster_size;
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, cluster_limit);
	}

	as_paxos_wire_change *wc;

	if (0 != as_paxos_wire_change_initialize(&wc, &t->c, cluster_limit)) {
		cf_crash(AS_PAXOS, "Failed to create as_paxos_wire_change object.");
		return(NULL);
	}

	size_t wc_sz = sizeof(as_paxos_wire_change) + (sizeof(uint8_t) + sizeof(cf_node)) * cluster_limit;
	e += msg_set_buf(m, AS_PAXOS_MSG_CHANGE, (void *)wc, wc_sz, MSG_SET_COPY);
	if (0 > e) {
		cf_warning(AS_PAXOS, "unable to wrap message");
		return(NULL);
	}

	as_paxos_wire_change_destroy(wc);

	return(m);
}

/* as_paxos_msg_unwrap
 * De-encapsulate a Paxos message into the provided transaction structure;
 * returns the message command type, or -1 on error */
int
as_paxos_msg_unwrap(msg *m, as_paxos_transaction *t)
{
	uint32_t c;
	int e = 0;
	as_paxos_wire_change *bufp = NULL;
	size_t bufsz;

	if (!AS_PAXOS_ENABLED()) {
		cf_warning(AS_PAXOS, "Paxos messaging disabled ~~ Not unwrapping message");
		return(-1);
	}

	cf_assert(m, AS_PAXOS, "invalid message");
	cf_assert(t, AS_PAXOS, "invalid transaction");

	/* Initialize the transaction structure */
	memset(t, 0, sizeof(as_paxos_transaction));

	/* Make sure this is actually a Paxos message */
	if (0 > msg_get_uint32(m, AS_PAXOS_MSG_ID, &c)) {
		cf_warning(AS_PAXOS, "received Paxos message without a valid ID");
		return(-1);
	}

	/* Check the protocol. */
	if (AS_PAXOS_PROTOCOL_IDENTIFIER() != c) {
		cf_warning(AS_PAXOS, "received Paxos message not for the currently active protocol version (received 0x%04x ; expected 0x%04x) ~~ Ignoring message!", c, AS_PAXOS_PROTOCOL_IDENTIFIER());
		return(-1);
	}

	/* The Paxos protocol v2 or greater provides a means of peaceful coexistence between nodes with different maximum cluster sizes:
	   If the succession list length of the incoming message does not agree with our maximum cluster size, simply ignore it. */
	if (AS_PMC_USE() && AS_PAXOS_MSG_V1_IDENTIFIER != c) {
		if (0 > (e += msg_get_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, &c))) {
			cf_warning(AS_PAXOS, "Received Paxos protocol v%d message without succession list length ~~ Ignoring message!", AS_PAXOS_PROTOCOL_VERSION_NUMBER(c));
			return(-1);
		}
		if (c != g_config.paxos_max_cluster_size) {
			cf_warning(AS_PAXOS, "Received Paxos message with a different maximum cluster size (received %d ; expected %d) ~~ Ignoring message!", c, g_config.paxos_max_cluster_size);
			return(-1);
		}
	}

	/* Unwrap the message contents; track all the return values as we go.
	 * Because synchronization messages are not expected to have generation
	 * numbers or change structures, they are handled slightly differently
	 * from the Paxos protocol messages.
	 * NB: the strange gyrations around bufp are because of the semantics of
	 * msg_get_buf() and the fact that the as_paxos_change structure is
	 * allocated directly within the as_paxos_transaction */
	c = 0; // just preserve previous behavior of msg_get_uint32()
	e += msg_get_uint32(m, AS_PAXOS_MSG_COMMAND, &c);
	if (c != AS_PAXOS_MSG_COMMAND_SYNC_REQUEST && c != AS_PAXOS_MSG_COMMAND_SYNC &&
		c != AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST && c != AS_PAXOS_MSG_COMMAND_PARTITION_SYNC  &&
		c != AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT && c != AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK) {
		t->gen.sequence = 0; // just preserve previous behavior of msg_get_uint32()
		e += msg_get_uint32(m, AS_PAXOS_MSG_GENERATION_SEQUENCE, &t->gen.sequence);
		// Older versions handled unused AS_PAXOS_MSG_GENERATION_PROPOSAL here.
		e += msg_get_buf(m, AS_PAXOS_MSG_CHANGE, (uint8_t **)&bufp, &bufsz, MSG_GET_DIRECT);
	}

	if (0 > e) {
		cf_warning(AS_PAXOS, "message unwrapping failed");
		return(-1);
	}

	if (NULL != bufp)
		as_paxos_change_copy_from_as_paxos_wire_change(&t->c, bufp, bufsz);

	return(c);
}

/* as_paxos_send_to_sl
 * Send a msg to the proposed succession list. */
static int
as_paxos_send_to_sl(msg *px_msg, int priority)
{
	as_node_list nl;
	as_paxos *p = g_paxos;
	int succ_list_len = 0, succ_list_num_alive = 0;

	nl.sz = 1;
	nl.nodes[0] = g_config.self_node;
	nl.alloc_sz = MAX_NODES_LIST;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (p->succession[i] == 0) {
			break;
		}

		if ((p->alive[i] && (g_config.self_node != p->succession[i]))) {
			nl.nodes[(nl.sz)++] = p->succession[i];
		}

		if (p->succession[i]) {
			succ_list_len++;
		}

		if (p->alive[i]) {
			succ_list_num_alive++;
		}
	}

	cf_debug(AS_PAXOS, "Paxos succession list length %d ; num. alive %d", succ_list_len, succ_list_num_alive);

	return as_fabric_send_list(nl.nodes, nl.sz, px_msg, priority);
}

/* as_paxos_spark
 * Begin a Paxos state change */
void
as_paxos_spark(as_paxos_change *c)
{
	as_paxos *p = g_paxos;
	cf_node self = g_config.self_node;
	as_paxos_transaction *s, t;
	msg *m = NULL;

	cf_debug(AS_PAXOS, "Entering as_paxos_spark");

	/* Populate a new transaction with the change list*/
	if (NULL == c) {
		cf_warning(AS_PAXOS, "Illegal params in as_paxos_spark");
		return;
	}
	memcpy(&t.c, c, sizeof(as_paxos_change));

	/* No matter what, we mark the node as dead immediately and check
	 * for quorum visibility; this lets us stop-the-world quickly if
	 * the cluster has collapsed
	 */
	for (int i = 0; i < t.c.n_change; i++) {
		if (t.c.type[i] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) {
			cf_debug(AS_PAXOS, "Node departure %"PRIx64"", t.c.id[i]);
			if (p->principal_pro_tempore == t.c.id[i]) {
				cf_info(AS_PAXOS, "in spark, removed node %"PRIx64" is no longer principal pro tempore", t.c.id[i]);
				p->principal_pro_tempore = 0;
			}
			as_paxos_succession_setdeceased(t.c.id[i]);
			if (false == as_paxos_succession_quorum()) {
				// Note:  Current policy is to continue providing service availability,
				//         even upon node departure exceeding the previous cluster quorum.
				cf_debug(AS_PAXOS, "quorum visibility lost! Continuing anyway ...");
			}
		}
	}

	/*
	 * If this is not the principal, we are done
	 */
	if (self != as_paxos_succession_getprincipal()) {
		cf_debug(AS_PAXOS, "I'm not Paxos principal ~~ Not sparking.");
		return;
	}

	t.gen.sequence = as_paxos_sequence_getnext();

	char change_buf[AS_CLUSTER_SZ * (2 * sizeof(cf_node) + 6) + 1] = { '\0' };
	char *op = "", *cb = change_buf;
	int count = 0;
	for (int i = 0; i < t.c.n_change; i++) {
		switch (t.c.type[i]) {
		  case AS_PAXOS_CHANGE_NOOP:
			  op = "NOP";
			  break;
		  case AS_PAXOS_CHANGE_SYNC:
			  op = "SYN";
			  break;
		  case AS_PAXOS_CHANGE_SUCCESSION_ADD:
			  op = "ADD";
			  break;
		  case AS_PAXOS_CHANGE_SUCCESSION_REMOVE:
			  op = "DEL";
			  break;
		  case AS_PAXOS_CHANGE_UNKNOWN:
		  default:
			  op = "UNC";
			  break;
		}
		count = sprintf(cb, "%s %"PRIx64"; ", op, t.c.id[i]);
		cb += count;
	}
	*cb = '\0';
	cf_info(AS_PAXOS, "as_paxos_spark establishing transaction [%"PRIu32"]@%"PRIx64" ClSz = %u ; # change = %d : %s",
			t.gen.sequence, g_config.self_node, as_exchange_cluster_size(), t.c.n_change, change_buf);

	/*
	 * Reset the sync attempt number.
	 */
	p->num_sync_attempts = 0;

	/* Populate a new transaction with the change, establish it
	 * in the pending transaction table, and send the message */
	if (NULL == (s = as_paxos_transaction_establish(&t))) {
		cf_warning(AS_PAXOS, "unable to establish transaction");
		return;
	}

	int cmd = AS_PAXOS_MSG_COMMAND_PREPARE;
	if (NULL == (m = as_paxos_msg_wrap(s, cmd))) {
		cf_warning(AS_PAXOS, "failed to construct Paxos command %s msg", as_paxos_cmd_name[cmd]);
		return;
	}

	int rv;
	if ((rv = as_paxos_send_to_sl(m, AS_FABRIC_CHANNEL_CTRL))) {
		cf_warning(AS_PAXOS, "in spark, sending Paxos command %s to succession list failed: rv %d", as_paxos_cmd_name[cmd], rv);
		as_fabric_msg_put(m);
	}
}

/* as_paxos_msgq_push
 * A shim to push an incoming message onto the Paxos queue.  NB: all message
 * processing is deferred! */
int
as_paxos_msgq_push(cf_node id, msg *m, void *udata)
{
	if (as_new_clustering()) {
		cf_warning(AS_PAXOS, "paxos v5 - got old paxos msg from %lx", id);
		as_fabric_msg_put(m);
		return 0;
	}

	as_paxos *p = g_paxos;
	as_paxos_msg *qm;

	cf_assert(m, AS_PAXOS, "null message");

	msg_preserve_all_fields(m);

	qm = cf_calloc(1, sizeof(as_paxos_msg));
	cf_assert(qm, AS_PAXOS, "allocation: %s", cf_strerror(errno));
	qm->id = id;
	qm->m = m;

	uint32_t c = 0;
	msg_get_uint32(m, AS_PAXOS_MSG_COMMAND, &c);
	cf_debug(AS_PAXOS, "PAXOS message with ID %d received from node %"PRIx64"", c, id);

	int q_priority;
	// The goal here is to try to prioritize events that change the cluster
	// membership over events that lead to a rebalance. Minimizing the number
	// of rebalances performed reduced the duplicate resolution load.
	switch (c) {
		case AS_PAXOS_MSG_COMMAND_PREPARE:
		case AS_PAXOS_MSG_COMMAND_PREPARE_ACK:
		case AS_PAXOS_MSG_COMMAND_PREPARE_NACK:
		case AS_PAXOS_MSG_COMMAND_COMMIT:
		case AS_PAXOS_MSG_COMMAND_COMMIT_ACK:
		case AS_PAXOS_MSG_COMMAND_COMMIT_NACK:
		case AS_PAXOS_MSG_COMMAND_CONFIRM:
		case AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT:
		case AS_PAXOS_MSG_COMMAND_SET_SUCC_LIST:
		case AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK:
			q_priority = CF_QUEUE_PRIORITY_MEDIUM;
			break;
		case AS_PAXOS_MSG_COMMAND_SYNC:
		case AS_PAXOS_MSG_COMMAND_SYNC_REQUEST:
		case AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST:
		case AS_PAXOS_MSG_COMMAND_PARTITION_SYNC:
		case AS_PAXOS_MSG_COMMAND_UNDEF:
		default:
			q_priority = CF_QUEUE_PRIORITY_LOW;
	}

	if (0 != cf_queue_priority_push(p->msgq, &qm, q_priority)) {
		cf_warning(AS_PAXOS, "PUSH FAILED: PAXOS message with ID %d received from node %"PRIx64"", c, id);
	}

	return 0;
}

/* as_paxos_event
 * An event processing stub for messages coming from heartbeat */
void
as_paxos_event(int nevents, as_hb_event_node *events, void *udata)
{
	if (as_new_clustering()) {
		// Ignore heartbeat events in new clustering mode.
		return;
	}

	// Leave one extra room for RESET event on top of node events
	if ((1 > nevents) || (AS_CLUSTER_SZ + 1 < nevents) || !events) {
		cf_warning(AS_PAXOS, "Illegal state in as_paxos_event, node events is: %d", nevents);
		return;
	}

	msg *m = NULL;
	int e = 0;

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "as_paxos_event: unable to get a fabric message");
		return;
	}

	/* Wrap up the message contents; track all the return values as we go */
	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT);
	e += msg_set_uint32(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS_COUNT, nevents);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. Except for heartbeat version v3.*/
	if (AS_PMC_USE() && !AS_PAXOS_PROTOCOL_IS_V(1))
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);

	e += msg_set_buf(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS, (void *)events,
	sizeof(as_hb_event_node) * (AS_CLUSTER_SZ + 1),
			MSG_SET_COPY);

	if (0 > e) {
		cf_warning(AS_PAXOS, "as_paxos_event: unable to wrap heartbeat message");
		return;
	}

	as_paxos_msgq_push(g_config.self_node, m, NULL);
}

/*
 *  Forcibly set the Paxos succession list to the given list of node IDs
 *  and then trigger new cluster formation if this node is the principal node
 *  (i.e., the first entry of the nodes list.)
 */
void
as_paxos_process_set_succession_list(cf_node *nodes)
{
	cf_debug(AS_PAXOS, "Paxos thread processing Set Succession List event:");

	if (! nodes) {
		cf_warning(AS_PAXOS, "set_succession_list called but nodes is NULL");

		return;
	}

	cf_node node, *nodes_p = nodes;
	int i = 0;
	while ((node = *nodes_p++)) {
		cf_debug(AS_PAXOS, "SLNode[%d] = %"PRIx64"", i, node);
		i++;
	}

	// Halt migrations before forcibly modifying the succession list.
	// [Note:  This is also done on the principal when the second phase is started below.]
	as_partition_balance_disallow_migrations();

	as_paxos *p = g_paxos;
	bool list_end = false;
	nodes_p = nodes;
	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		if (!list_end) {
			if (!(p->succession[i] = *nodes_p++)) {
				list_end = true;
				p->alive[i] = false;
			} else {
				p->alive[i] = true;
			}
		} else {
			p->succession[i] = 0;
			p->alive[i] = false;
		}
	}

	if (g_config.self_node == nodes[0]) {
		cf_warning(AS_PAXOS, "[I am the principal node ~~ Forcing a cluster SYNC event.]");
		as_paxos_start_second_phase();
	} else {
		cf_warning(AS_PAXOS, "[I am not the principal node ~~ Waiting for a SYNC event.]");
	}
}

void
as_paxos_process_heartbeat_event(msg *m)
{
	int e = 0;
	uint32_t nevents = 0;
	size_t bufsz = 0;
	as_hb_event_node *events = NULL;
	/*
	 * Extract the heartbeat information from the message
	 */
	e += msg_get_uint32(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS_COUNT, &nevents);
	e += msg_get_buf(m, AS_PAXOS_MSG_HEARTBEAT_EVENTS, (uint8_t **)&events, &bufsz, MSG_GET_DIRECT);

	if (0 > e) {
		cf_warning(AS_PAXOS, "as_paxos_process_heartbeat_event: unable to unwrap heartbeat message");
		return;
	}

	/*
	 * See if we are principal now
	 */
	cf_node old_principal = as_paxos_succession_getprincipal();
	cf_node self = g_config.self_node;
	as_paxos *p = g_paxos;

	as_paxos_change c;
	memset(&c, 0, sizeof(as_paxos_change));
	int j = 0;

	/*
	 * Allow redundant events if fabric reset is one of the event.
	 */
	bool reset_event_present = false;
	for (int i = 0; i < nevents; i++) {
		if (events[i].evt == AS_HB_AUTO_RESET) {
			cf_info(AS_PAXOS, "Got reset event. Forcing paxos spark.");
			reset_event_present = true;
		}
	}

	/*
	 * First remove departed nodes. We need to do that to compute the new principal.
	 */
	for (int i = 0; i < nevents; i++)
		switch (events[i].evt) {
			case AS_HB_NODE_ARRIVE:
				/*
				 * Do nothing. Will process in next loop
				 */
				break;
			case AS_HB_NODE_DEPART:
				if (p->principal_pro_tempore == events[i].nodeid) {
					cf_info(AS_PAXOS, "departed node %"PRIx64" is no longer principal pro tempore", events[i].nodeid);
					p->principal_pro_tempore = 0;
				}
				if (as_paxos_succession_ismember(events[i].nodeid) || reset_event_present) {
					c.type[j] = AS_PAXOS_CHANGE_SUCCESSION_REMOVE;
					c.id[j] = events[i].nodeid;
					cf_debug(AS_PAXOS, "Node departure %"PRIx64"", c.id[j]);
					as_paxos_succession_setdeceased(c.id[j]);
					j++;
					if (false == as_paxos_succession_quorum()) {
						// Note:  Current policy is to continue providing service availability,
						//         even upon node departure exceeding the previous cluster quorum.
						cf_debug(AS_PAXOS, "quorum visibility lost! Continuing anyway ...");
					}
				}
				break;
			case AS_HB_AUTO_RESET:
				break;
			default:
				cf_warning(AS_PAXOS, "unknown event type received in as_paxos_process_heartbeat_event() - aborting");
				return;
		}

	cf_node principal = as_paxos_succession_getprincipal();

	for (int i = 0; i < nevents; i++)
		switch (events[i].evt) {
			case AS_HB_NODE_ARRIVE:

				// Mark the node as alive if it is already in the succession list.
				as_paxos_succession_setrevived(events[i].nodeid);

				/*
				 * Check if this pulse came from a node whose principal is different than ours
				 * This means two clusters are merging - figure out who wins
				 */

				cf_node event_p_node = as_paxos_hb_get_principal(events[i].nodeid);

				if (! as_paxos_succession_ismember(events[i].nodeid) || reset_event_present) {
					cf_debug(AS_PAXOS, "Node arrival %"PRIx64" cluster principal %"PRIx64" pulse principal %"PRIx64"",
							 events[i].nodeid, principal, event_p_node);

					if ((0 != event_p_node) && (event_p_node != principal)) {
						if (!reset_event_present && principal < event_p_node) {
							/*
							 * We lose. We wait to be assimilated by the other
							 * TODO: Should we send a sync message to the other principal
							 */
							cf_info(AS_PAXOS, "Skip node arrival %"PRIx64" cluster principal %"PRIx64" pulse principal %"PRIx64"",
									 events[i].nodeid, principal, event_p_node);

							// TODO - but what if the other principal now
							// quietly disappears and never returns?
							p->need_to_rebalance = false;

							break; // skip this event
						}

						if (reset_event_present && principal < event_p_node) {
							// Could be a one direction link failure where we see this node
							// but not its principal. Hence we are assuming ourselves to be
							// principal and assimilating this node.
							cf_warning(AS_PAXOS, "Assimilating node %"PRIx64" having unknown larger principal %"PRIx64"",
									   events[i].nodeid, event_p_node);

						}
					}

					c.type[j] = AS_PAXOS_CHANGE_SUCCESSION_ADD;
					c.id[j] = events[i].nodeid;
					cf_debug(AS_PAXOS, "Node arrival %"PRIx64"", c.id[j]);
					j++;
				}

				break;
			case AS_HB_NODE_DEPART:
				/* Already processed in earlier loop */
				break;
			case AS_HB_AUTO_RESET:
				break;
			default:
				cf_warning(AS_PAXOS, "unknown event type received in as_paxos_process_heartbeat_event() - aborting");
				return;
		}

	/*
	 * If we find that we are the new principal and we were not an old principal before, then any non-live nodes in the paxos list need to be added to the change list
	 */
	if ((self == principal) && (self != old_principal)) {
		/* Go through the succession list and add nodes into the change list that are marked as dead */

		for (int i = 0; i < AS_CLUSTER_SZ; i++) {
			if (p->succession[i] == (cf_node) 0)
				break;
			if (p->alive[i])
				continue;
			bool found = false;
			for (int k = 0; k < j; k++)
				if ((c.type[k] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) && (c.id[k] == p->succession[i])) {
					found = true;
					break;
				}
			if (!found) {
				c.type[j] = AS_PAXOS_CHANGE_SUCCESSION_REMOVE;
				c.id[j] = p->succession[i];
				cf_debug(AS_PAXOS, "Adding dead node %"PRIx64" to departure list", c.id[j]);
				j++;
			}
		}
	}

	c.n_change = j;
	/*
	 *  The principal is stored with the change to enable members of other clusters to ignore this transaction
	 */
	c.p_node = principal;

	/*
	 * Spark paxos if there are any events
	 */
	if (c.n_change > 0) {
		as_paxos_spark(&c);
	} else {
		cf_debug(AS_PAXOS, "Skipping call as_paxos_spark");
	}
}

void
as_paxos_send_partition_sync_request(cf_node p_node) {
	msg *reply = NULL;
	if (NULL == (reply = as_paxos_partition_sync_request_msg_generate())) {
		cf_warning(AS_PAXOS, "unable to construct partition sync request message to node %"PRIx64"", p_node);
		return;
	} else if (0 != as_fabric_send(p_node, reply, AS_FABRIC_CHANNEL_CTRL)) {
		cf_warning(AS_PAXOS, "unable to send partition sync message to node %"PRIx64"", p_node);
		as_fabric_msg_put(reply);
		return;
	}
	cf_info(AS_PAXOS, "Sent partition sync request to node %"PRIx64"", p_node);
}

/* as_paxos_retransmit_check
 * An event processing stub for messages coming from the fabric */
int
as_paxos_retransmit_check()
{
	msg *m = NULL;
	int e = 0;

	if (!AS_PAXOS_ENABLED()) {
		cf_info(AS_PAXOS, "Paxos messaging disabled ~~ Not retransmitting check message.");
		return(0);
	}

	if (NULL == (m = as_fabric_msg_get(M_TYPE_PAXOS))) {
		cf_warning(AS_PAXOS, "as_paxos_retransmit_check: unable to get a fabric message");
		return(0);
	}

	/* Wrap up the message contents; track all the return values as we go */
	e += msg_set_uint32(m, AS_PAXOS_MSG_ID, AS_PAXOS_PROTOCOL_IDENTIFIER());
	e += msg_set_uint32(m, AS_PAXOS_MSG_COMMAND, AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK);

	/* Include the succession list length in all Paxos protocol v2 or greater messages. Except for heartbeat version v3.*/
	if (AS_PMC_USE() && !AS_PAXOS_PROTOCOL_IS_V(1)) {
		e += msg_set_uint32(m, AS_PAXOS_MSG_SUCCESSION_LENGTH, g_config.paxos_max_cluster_size);
	}

	if (0 > e) {
		cf_warning(AS_PAXOS, "as_paxos_retransmit_check: unable to wrap retransmit check message");
		return(0);
	}

	return (as_paxos_msgq_push(g_config.self_node, m, NULL));
}

/**
 * Get the wait time between auto reset corrections. This function gives larger clusters more time to correct themselves.
 */
uint32_t
as_paxos_get_auto_reset_wait_ms()
{
	as_paxos* p = g_paxos;
	size_t cluster_size = 0;
	for (int j = 0; j < AS_CLUSTER_SZ; j++) {
		if (p->succession[j] != (cf_node)0) {
			cluster_size++;
		} else {
			break;
		}
	}

	// Provide large wait times for larger clusters. Use retransmit interval
	// as a base constant since heartbeat can be dynamically changed.
	uint32_t wait_ms = (g_config.paxos_retransmit_period * 1000) *
			   MAX(1, (int)(cluster_size * 0.2));

	// Guard very high timeout values.
	if (wait_ms > AS_PAXOS_AUTO_RESET_MAX_WAIT) {
		wait_ms = AS_PAXOS_AUTO_RESET_MAX_WAIT;
	}
	return wait_ms;
}

/**
 * Fix succession list errors by resetting the cluster at master / potential
 * master nodes.
 *
 * @param reset_cluster set to true if the cluster has integrity fault or second
 * phase failure to force reforming the cluster from scratch.
 * @param corrective_event_count the number of corrective events.
 * @param corrective events the corrective events.
 */
void as_paxos_auto_reset_master(bool reset_cluster,
								int corrective_event_count,
								as_hb_event_node *corrective_events)
{

	cf_info(AS_PAXOS, "Corrective changes: %d. Integrity fault: %s",
			corrective_event_count, reset_cluster ? "true" : "false");

	as_paxos *p = g_paxos;

	uint32_t wait_ms = as_paxos_get_auto_reset_wait_ms();

	// Check if a paxos call has been triggered in between now and the last
	// check.
	cf_clock now = cf_getms();

	for (int i = 0; i < AS_PAXOS_ALPHA; i++) {
		if (p->pending[i].establish_time + wait_ms > now) {
			// A paxos transaction has been started in between now and the last
			// check. Give the paxos some more time to finish. Its alright if
			// the transaction has been applied. Give that
			// transaction some time. It might fix the cluster.
			cf_info(AS_PAXOS,
					"Paxos round running. Skipping succession list fix.");
			return;
		}
	}

	if (reset_cluster) {
		// Add nodes in the current succession list to the changes list, to
		// reform the cluster from scratch.
		for (int i = 0; i < AS_CLUSTER_SZ; i++) {
			if (p->succession[i] == 0) {
				break;
			}

			bool found = false;
			for (int j = 0; j < corrective_event_count; j++) {
				if (p->succession[i] == corrective_events[j].nodeid) {
					// We already have accounted for this node in succession
					// list, it is most likely an expired node.
					found = true;
					break;
				}
			}

			if (!found) {

				cf_info(AS_PAXOS, "Marking node add for paxos recovery: %" PRIx64 "",
				p->succession[i]);

				memset(&corrective_events[corrective_event_count], 0, sizeof(as_hb_event_node));
				corrective_events[corrective_event_count].evt =
					AS_HB_NODE_ARRIVE;
				corrective_events[corrective_event_count].nodeid =
					p->succession[i];
				corrective_event_count++;

			}
		}
	}

	// In a steady state (n/w or node health), corrective_event should generate
	// the ideal succession list.
	for (int i = 0; i < corrective_event_count; i++) {
		if ((corrective_events[i].evt == AS_HB_NODE_ARRIVE) &&
			corrective_events[i].nodeid > g_config.self_node) {
			// The list of stable nodes has a higher id principal, Wait for him
			// to correct this situation. If he does go away we will fix the
			// cluster in the next round.
			cf_info(AS_PAXOS, "Skipping paxos recovery: %" PRIx64
							  " will handle the recovery",
					corrective_events[i].nodeid);
			return;
		}
	}

	// Force a paxos spark with all events even if they are redundant.
	memset(&corrective_events[corrective_event_count], 0, sizeof(as_hb_event_node));
	corrective_events[corrective_event_count].evt = AS_HB_AUTO_RESET;
	corrective_event_count++;

	as_paxos_event(corrective_event_count, corrective_events, NULL);
}

void
as_paxos_check_integrity()
{
	as_paxos* p = g_paxos;

	// Perform a general consistency check between our succession list and
	// the list that heart beat thinks is correct. First get a copy of the
	// heartbeat's compiled list for each node in our succession list.
	cf_node other_succession_list[AS_CLUSTER_SZ];
	cf_node node_succession_list[AS_CLUSTER_SZ];

	memcpy(node_succession_list, p->succession, sizeof(p->succession));

	// For each node in the succession list compare the node's succession
	// list with this server's succession list.

	bool cluster_integrity_fault = false;

	for (int i = 0; i < AS_CLUSTER_SZ; i++) {

		if (node_succession_list[i] == 0) {
			break;
		}

		if (node_succession_list[i] == g_config.self_node) {
			continue;
		}

		cf_debug(AS_PAXOS,
			 "Cluster Integrity Check against node:  %" PRIx64 "",
			 node_succession_list[i]);

		memset(other_succession_list, 0, sizeof(other_succession_list));
		as_paxos_hb_get_succession_list(node_succession_list[i],
						other_succession_list);

		// 8byte nodeid will need 16 hex chars + space = 17
		char sbuf[(AS_CLUSTER_SZ * 17) + 28];
		snprintf(sbuf, 28, "HEARTBEAT %" PRIx64 ": ",
			 node_succession_list[i]);
		for (int j = 0; j < AS_CLUSTER_SZ; j++) {
			if ((cf_node)0 != other_succession_list[j]) {
				snprintf(sbuf + strlen(sbuf), 18,
					 "%" PRIx64 " ",
					 other_succession_list[j]);
			} else {
				break;
			}
		}

		cf_debug(AS_PAXOS, "%s", sbuf);

		if (memcmp(node_succession_list, other_succession_list,
			   sizeof(other_succession_list)) != 0) {
			cf_info(AS_PAXOS,
				"Cluster Integrity Check: Detected "
				"succession list discrepancy between "
				"node %" PRIx64 " and self %" PRIx64 "",
				node_succession_list[i], g_config.self_node);

			as_paxos_log_succession_list(
			  "Paxos List", node_succession_list, AS_CLUSTER_SZ);
			as_paxos_log_succession_list(
			  "Node List", other_succession_list, AS_CLUSTER_SZ);

			cluster_integrity_fault = true;
		}
	} // end for each node

	as_clustering_set_integrity(! cluster_integrity_fault);
}

void
as_paxos_process_retransmit_check()
{
	as_paxos* p = g_paxos;

	// check for succession list fault. We need space for AS_CLUSTER_SZ+1
	// elements as there will be RESET event on top of nodes events.
	as_hb_event_node corrective_events[AS_CLUSTER_SZ + 1];
	memset(corrective_events, 0, sizeof(corrective_events));
	int corrective_event_count = as_hb_get_corrective_events(
	  p->succession, AS_CLUSTER_SZ, corrective_events, AS_CLUSTER_SZ);
	bool succession_list_fault = corrective_event_count > 0;

	as_paxos_check_integrity();
	bool cluster_integrity_fault = ! as_clustering_has_integrity();

	// Second phase failed if migrations are disallowed and we have
	// attempted sync more than the threshold number of times.
	bool second_phase_failed =
	  as_partition_balance_are_migrations_allowed()
	    ? false
	    : (p->num_sync_attempts > AS_PAXOS_SYNC_ATTEMPTS_MAX);

	// Indicates if new paxos round was sparked for recovery.
	bool paxos_sparked = false;

	if (cluster_integrity_fault || succession_list_fault ||
			second_phase_failed) {
		as_paxos_auto_reset_master(
				cluster_integrity_fault || second_phase_failed,
				corrective_event_count, corrective_events);
		paxos_sparked = true;
	}

	// Second phase succeeded, we are already in a cluster, hence we are
	// done or we started a new paxos round and should wait longer.
	if (as_partition_balance_are_migrations_allowed() || paxos_sparked) {
		return;
	}

	cf_node p_node = as_paxos_succession_getprincipal();

	// Otherwise, we are in the middle of a Paxos reconfiguration of the
	// cluster:
	//   - Principal sends a SYNC message to all cluster nodes (including
	//   itself.)
	//   - Non-principals send a PARTITION_SYNC_REQUEST message to the
	//   principal.
	if (g_config.self_node == p_node) {
		cf_info(AS_PAXOS,
			"as_paxos_retransmit_check: principal %" PRIx64
			" retransmitting sync messages to nodes that have not "
			"responded yet ... ",
			p_node);
		as_paxos_send_sync_messages();
	} else {
		cf_info(
		  AS_PAXOS,
		  "as_paxos_retransmit_check: node %" PRIx64
		  " retransmitting partition sync request to principal %" PRIx64
		  " ... ",
		  g_config.self_node, p_node);
		as_paxos_send_partition_sync_request(p_node);
	}
}

static void
paxos_begin_partition_balance()
{
	uint32_t cluster_size = 0;

	while (cluster_size < AS_CLUSTER_SZ) {
		if (g_paxos->succession[cluster_size] == (cf_node)0) {
			break;
		}

		cluster_size++;
	}

	as_exchange_succession_set(g_paxos->succession, cluster_size);

	cf_info(AS_PAXOS, "CLUSTER SIZE = %u", cluster_size);

	// Currently all namespaces' succession lists are the same as the global
	// cluster list. The paxos replacement, "exchange", will fill in the
	// namespace lists independently.
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		ns->cluster_size = cluster_size;
		memset(ns->succession, 0, sizeof(ns->succession));
		memcpy(ns->succession, g_paxos->succession,
				sizeof(cf_node) * cluster_size);
	}

	// Balance partitions and kick off necessary migrations.
	as_partition_balance();
}

// as_paxos_thr
// A thread to handle all Paxos events
void *
as_paxos_thr(void *arg)
{
	as_paxos *p = g_paxos;
	cf_node self = g_config.self_node;
	int c;

	/* Event processing loop */
	while (! as_new_clustering()) {
		as_paxos_msg *qm = NULL;
		msg *reply = NULL;
		/* NB: t is the transaction being processed; s is a pointer to the
		 * corresponding entry in the pending transaction list; r is a pointer
		 * to a rejected transaction */
		as_paxos_transaction *r, *s, t;

		cf_detail(AS_PAXOS, "Popping paxos queue %p", p->msgq);

		static const int Q_WAIT_MS = 1; // TODO - what to do with this?

		if (Q_WAIT_MS >= (g_config.paxos_retransmit_period * 1000)) {
			cf_crash(AS_PAXOS, "paxos_retransmit_period %d s is less than paxos msgq wait %d ms.",
					g_config.paxos_retransmit_period, Q_WAIT_MS);
		}

		// Get the next message from the queue.
		if (0 != cf_queue_priority_pop(p->msgq, &qm, Q_WAIT_MS)) {
			// Q: Couldn't this cause us to starve rebalance if there are
			//    frequent cluster disruptions?
			// A: We sure hope so! We want to minimize the number of rebalances
			//    during these scenarios since they would have been pointless
			//    and will cause unnecessary partition version
			//    changes/creations which increases write duplicate resolution
			//    load.

			if (! p->need_to_rebalance) {
				continue;
			}

			// No event came - do the rebalance.
			p->need_to_rebalance = false;

			cf_node principal = as_paxos_succession_getprincipal();

			if (self != principal) {
				// Only principal can initiate rebalance.
				continue;
			}

			as_paxos_start_second_phase();

			if (as_paxos_is_single_node_cluster()) {
				// Clean out the sync states array.
				memset(p->partition_sync_state, 0, sizeof(p->partition_sync_state));

				paxos_begin_partition_balance();

				if (p->cb) {
					as_exchange_cluster_changed_event c = {
							.cluster_key = as_exchange_cluster_key(),
							.cluster_size = as_exchange_cluster_size(),
							.succession = as_exchange_succession()
					};

					for (int i = 0; i < p->n_callbacks; i++) {
						(p->cb[i])(&c, p->cb_udata[i]);
					}
				}
			}

			continue;
		}

		/* Unwrap and sanity check the message, then undertake the
		 * appropriate action */
		if (0 > (c = as_paxos_msg_unwrap(qm->m, &t))) {
			cf_warning(AS_PAXOS, "failed to unwrap Paxos message from node %"PRIx64" ~~ check Paxos protocol version", qm->id);
			goto cleanup;
		}

		cf_debug(AS_PAXOS, "unwrapped | received paxos message from node %"PRIx64" command %s (%d)", qm->id, as_paxos_cmd_name[c], c);

		if (c == AS_PAXOS_MSG_COMMAND_SET_SUCC_LIST) {
			as_paxos_process_set_succession_list(t.c.id);
			c = AS_PAXOS_MSG_COMMAND_SYNC;
			goto cleanup;
		}

		if (c == AS_PAXOS_MSG_COMMAND_HEARTBEAT_EVENT) {
			as_paxos_process_heartbeat_event(qm->m);
			goto cleanup;
		}

		if (c == AS_PAXOS_MSG_COMMAND_RETRANSMIT_CHECK) {
			as_paxos_process_retransmit_check();
			goto cleanup;
		}

		cf_node principal = as_paxos_succession_getprincipal();
		bool principal_is_alive = as_hb_is_alive(principal);

		/* Accept all messages from a new potential principal. This will enable
		   hostile takeovers where this node needs to participate in the paxos
		   for convergence. Else only the principal will accept messages from
		   nodes that aren't in the succession, unless they're synchronization
		   messages.

		   The case to worry about would be if we accidently receive a confirm
		   and or messages after confirm in the state transition. But in the
		   current design that is hard to guard against.
		 */
		if (false == as_paxos_succession_ismember(qm->id)) {
			cf_debug(AS_PAXOS, "got a message from a node not in the succession: %"PRIx64, qm->id);
			if (self != principal && AS_PAXOS_MSG_COMMAND_SYNC != c && qm->id < principal && principal_is_alive) {
				cf_warning(AS_PAXOS, "ignoring message from a node not in the succession: %"PRIx64" command %d", qm->id, c);
				goto cleanup;
			}
		}

		/*
		 * Refuse transactions with changes initiated by a principal that is not the current principal
		 * If the principal node is set to 0, let this through. This will be the case for sync messages
		 */
		if ((t.c.p_node != 0) && (t.c.p_node != principal)) {
			/*
			 * Check if this new principal out ranks our own principal - could have just arrived
			 * Since it is possible we have not yet removed failed nodes for our state
			 * reject the transaction only if it is also from a node NOT in our current
			 * succession list
			 */
			if ((t.c.p_node < principal && principal_is_alive) && (false == as_paxos_succession_ismember(qm->id))) {
				cf_debug(AS_PAXOS, "Ignoring transaction from principal %"PRIx64" < current principal %"PRIx64" from node %"PRIx64" not in succession list", t.c.p_node, principal, qm->id);
				goto cleanup;
			}
			/*
			 * reject transaction if a node from the succession list is sending this to us and we are the principal
			 */
			if ((true == as_paxos_succession_ismember(qm->id)) && (principal == self)) {
				cf_debug(AS_PAXOS, "Ignoring transaction from node %"PRIx64" in succession list", qm->id);
				goto cleanup;
			}
		}

		// Check if our principal is sending a node removal list that contains us
		// This can actually happen in some one-way network situations.
		// Ignore this transaction. The principal will not bother since our vote will not be needed for this vote
		// We are only getting this message because the principal is sending to all nodes known by fabric
		if ((t.c.p_node == principal) && (self != principal)) {
			for (int i = 0; i < t.c.n_change; i++) {
				switch (t.c.type[i]) {
					case AS_PAXOS_CHANGE_NOOP:
						break;
					case AS_PAXOS_CHANGE_SUCCESSION_ADD:
						if (self == t.c.id[i]) {
							cf_info(AS_PAXOS, "Self(%"PRIx64") add from Principal %"PRIx64"", self, principal);
							// Sounds draconian to skip the entire transaction
							// on add. Breaks the cluster reset fix.
							// Disabling this skip.
							// goto cleanup;
						}
						break;
					case AS_PAXOS_CHANGE_SUCCESSION_REMOVE:
						if (self == t.c.id[i]) {
							cf_info(AS_PAXOS, "Ignoring self(%"PRIx64") remove from Principal %"PRIx64"", self, principal);
							goto cleanup;
						}
						break;
					case AS_PAXOS_CHANGE_UNKNOWN:
					default:
						cf_warning(AS_PAXOS, "unknown command %d, ignoring", t.c.type[i]);
						break;
				}
			}
		}

		switch (c) {
			case AS_PAXOS_MSG_COMMAND_PREPARE:
				cf_debug(AS_PAXOS, "{%d} received prepare message from %"PRIx64"",
						t.gen.sequence, qm->id);

				s = as_paxos_transaction_search(t);
				if (NULL == s) {
					// Otherwise this prepare is a retransmit.
					if (as_paxos_current_is_candidate(t)) {
						if (NULL == (s = as_paxos_transaction_establish(&t))) {
							cf_warning(AS_PAXOS, "unable to establish transaction");
							break;
						}
						cf_info(AS_PAXOS, "{%d} sending prepare_ack to %"PRIx64"",
							p->gen.sequence, qm->id);
						reply = as_paxos_msg_wrap(s, as_paxos_state_next(c, ACK));
					}
					else {
						// Reject: the proposed sequence number is out of order.
						// FIXME: we need to come up with a different way to do this.
						cf_info(AS_PAXOS, "{%d} sending prepare_nack to %"PRIx64"",
							p->gen.sequence, qm->id);
						reply = as_paxos_msg_wrap(as_paxos_current_get(), as_paxos_state_next(c, NACK));
					}

					if (0 != as_fabric_send(qm->id, reply, AS_FABRIC_CHANNEL_CTRL)) {
						as_fabric_msg_put(reply);
					}
					break;
				}
				else {
					if (self == principal) {
						// Principal establishes the transaction in spark,
						// need to ack it.
						reply = as_paxos_msg_wrap(s, as_paxos_state_next(c, ACK));

						cf_info(AS_PAXOS, "{%d} principal acking it's prepare %"PRIx64"",
								t.gen.sequence, qm->id);
						if (0 != as_fabric_send(qm->id, reply, AS_FABRIC_CHANNEL_CTRL)) {
							as_fabric_msg_put(reply);
						}

					}
					else {
						cf_info(AS_PAXOS, "{%d} prevented PREPARE in COMMIT path.",
								t.gen.sequence);
					}
				}
				break;

			case AS_PAXOS_MSG_COMMAND_COMMIT:
				cf_debug(AS_PAXOS, "{%d} received commit message from %"PRIx64"",
						t.gen.sequence, qm->id);
				if (NULL != (s = as_paxos_transaction_search(t))) {
					// We've seen this transaction before.
					reply = as_paxos_msg_wrap(s, as_paxos_state_next(c, ACK));

					cf_debug(AS_PAXOS, "{%d} sending commit_ack to %"PRIx64"",
							t.gen.sequence, qm->id);
					if (0 != as_fabric_send(qm->id, reply, AS_FABRIC_CHANNEL_CTRL))
						as_fabric_msg_put(reply);
				}
				else {
					cf_debug(AS_PAXOS, "{%d} prevented COMMIT in PREPARE path.",
							t.gen.sequence);
				}
				break;

			case AS_PAXOS_MSG_COMMAND_PREPARE_ACK:
				cf_debug(AS_PAXOS, "{%d} received prepare_ack message from %"PRIx64"",
						t.gen.sequence, qm->id);
				// no break
			case AS_PAXOS_MSG_COMMAND_COMMIT_ACK:
				if (c == AS_PAXOS_MSG_COMMAND_COMMIT_ACK) {
					cf_debug(AS_PAXOS, "{%d} received commit_ack message from %"PRIx64"",
							t.gen.sequence, qm->id);

				}

				if (self != as_paxos_succession_getprincipal()) {
					cf_debug(AS_PAXOS, "I'm not principal ~~ Ignoring ACK %d message from %"PRIx64, c, qm->id);
					break;
				}

				if (NULL == (s = as_paxos_transaction_search(t))) {
					cf_warning(AS_PAXOS, "received acknowledgment for unknown type");
					break;
				}

				if (c == AS_PAXOS_MSG_COMMAND_PREPARE_ACK
						&& s->election_cycle == AS_PAXOS_MSG_COMMAND_COMMIT) {
					cf_debug(AS_PAXOS, "{%d} prepare_ack in commit_ack path - ignoring",
							t.gen.sequence);
					break;
				}
				if (c == AS_PAXOS_MSG_COMMAND_COMMIT_ACK
						&& s->election_cycle == AS_PAXOS_MSG_COMMAND_PREPARE) {
					cf_debug(AS_PAXOS, "{%d} commit_ack in prepare_ack path - ignoring",
							t.gen.sequence);
					break;
				}

				// Attempt to record the vote; if this results in a quorum,
				// send a commit message and reset the vote count
				switch (as_paxos_transaction_vote(s, qm->id, &t)) {
					case AS_PAXOS_TRANSACTION_VOTE_ACCEPT:
						cf_debug(AS_PAXOS, "{%d} received 'accept' vote from %"PRIx64" election %d",
								t.gen.sequence, qm->id, s->election_cycle);
						break;
					case AS_PAXOS_TRANSACTION_VOTE_REJECT:
						cf_debug(AS_PAXOS, "{%d} received 'reject' vote from %"PRIx64" election %d",
								t.gen.sequence, qm->id, s->election_cycle);
						break;
					case AS_PAXOS_TRANSACTION_VOTE_QUORUM:
						cf_debug(AS_PAXOS, "{%d} received 'accept' vote from %"PRIx64" and reached quorum, election %d",
								t.gen.sequence, qm->id, s->election_cycle);
						int cmd, rv;
						if (!(reply = as_paxos_msg_wrap(s, cmd = as_paxos_state_next(c, ACK)))) {
							cf_warning(AS_PAXOS, "failed to wrap Paxos command %s msg", as_paxos_cmd_name[cmd]);
							break;
						}

						cf_debug(AS_PAXOS, "{%d} sending %s to %"PRIx64"",
								t.gen.sequence, as_paxos_cmd_name[cmd], qm->id);
						if ((rv = as_paxos_send_to_sl(reply, AS_FABRIC_CHANNEL_CTRL))) {
							cf_warning(AS_PAXOS, "sending Paxos command %s to succession list failed: rv %d", as_paxos_cmd_name[cmd], rv);
							as_fabric_msg_put(reply);
						}

						as_paxos_transaction_vote_reset(s);
						s->election_cycle = AS_PAXOS_MSG_COMMAND_COMMIT;

						break;
				}

				break;
			case AS_PAXOS_MSG_COMMAND_PREPARE_NACK:
			case AS_PAXOS_MSG_COMMAND_COMMIT_NACK:
				cf_debug(AS_PAXOS, "{%d} received prepare_nack/commit_nack message from %"PRIx64"",
						t.gen.sequence, qm->id);
				if (self != as_paxos_succession_getprincipal()) {
					cf_debug(AS_PAXOS, "I'm not principal ~~ Ignoring NACK %d message from %"PRIx64, c, qm->id);
					break;
				}

				if (NULL == (r = as_paxos_transaction_search(t))) {
					cf_warning(AS_PAXOS, "received negative acknowledgment for unknown transaction");
					break;
				}

				break;
			case AS_PAXOS_MSG_COMMAND_CONFIRM:
				if (!as_hb_is_alive(t.c.p_node)) {
					// Basically the other node can see us
					// but the converse is not true . This
					// could happen with a uni directional
					// link failure or if a new principal
					// comes up, however this node does not
					// add it to its adjacency list because
					// of a max cluster size breach.
					cf_warning(AS_PAXOS, "Rejecting confirm from node %"PRIx64" not in adjacency list.",
							t.c.p_node);
				   break;
				}

				// At this point, we cannot complain -- so we just accept
				// what we're told.
				cf_debug(AS_PAXOS, "{%d} received state confirmation message from %"PRIx64"",
						t.gen.sequence, qm->id);

				if (NULL == (s = as_paxos_transaction_search(t))) {
					s = as_paxos_transaction_establish(&t);
				} else {
					as_paxos_transaction_update(s, &t);
				}
				as_paxos_transaction_confirm(s);

				/*
				 * If we are the principal and this confirmation message is not from us, ignore it.
				 * This case happens when two clusters are merging and the winning cluster's principal assimilates this node.
				 * The subsequent sync message from the new principal will clean the state up
				 */
				cf_node principal = as_paxos_succession_getprincipal();
				if ((self == principal) && (qm->id != self)) {
					cf_debug(AS_PAXOS, "Principal %"PRIx64" is ignoring confirmation message from foreign principal %"PRIx64"", self, qm->id);
					break;
				}
				/*
				 * Apply the transaction locally and send
				 * sync requests to all nodes other than the principal.
				 */
				as_paxos_transaction_apply(qm->id);

				/*
				 * Check for the single node cluster case but only if the node is principal
				 */
				if (as_paxos_is_single_node_cluster()) {
					// The principal can now balance its partitions.
					// Should we have another phase to the synchronizations to
					// make sure that every cluster node has had its state
					// updated before starting partition rebalance?
					// Currently, the answer to this question is "no."
					cf_info(AS_PAXOS, "{%d} SINGLE NODE CLUSTER",
							t.gen.sequence);

					p->need_to_rebalance = true;
				}

				/*
				 *  This should not happen, but log a warning if it ever does.
				 */
				if (p->principal_pro_tempore && (principal != p->principal_pro_tempore) && (principal != g_config.self_node)) {
					cf_warning(AS_PAXOS, "joining cluster with non-self principal %"PRIx64" which is not principal pro tempore %"PRIx64,
							   principal, p->principal_pro_tempore);
				}

				/*
				 * The principal pro tempore no longer, but the principal in fact.
				 */
				p->principal_pro_tempore = 0;

				/*
				 * TODO - We need to detect case where paxos messages get lost and retransmit
				 */

				break;
			case AS_PAXOS_MSG_COMMAND_SYNC_REQUEST:
				cf_debug(AS_PAXOS, "received sync request message from %"PRIx64"", qm->id);
				if (self != as_paxos_succession_getprincipal())
					break;

				uint64_t cluster_key = as_exchange_cluster_key();
				if (NULL == (reply = as_paxos_sync_msg_generate(cluster_key)))
					cf_warning(AS_PAXOS, "unable to construct reply message");
				else if (0 != as_fabric_send(qm->id, reply, AS_FABRIC_CHANNEL_CTRL))
					as_fabric_msg_put(reply);

				break;
			case AS_PAXOS_MSG_COMMAND_SYNC:
				cf_debug(AS_PAXOS, "received sync message from %"PRIx64"", qm->id);

				/*
				 * A principal should never get the sync message unless
				 * it is from another principal as part of cluster merge
				 */
				principal = as_paxos_succession_getprincipal();

				/*
				 * Check if this new principal out ranks our own principal - could have just arrived
				 * Since it is possible we have not yet removed failed nodes for our state
				 * reject the transaction only if it is also from a node NOT in our current
				 * succession list
				 */
				if ((qm->id < principal && as_hb_is_alive(principal)) && (false == as_paxos_succession_ismember(qm->id))) {
					cf_debug(AS_PAXOS, "Ignoring sync message from principal %"PRIx64" < current principal %"PRIx64" and not in succession list", qm->id, principal);
					break;
				}

				/*
				 * Check if the principal sending the node is greater than our current principal or is part of the succession list
				 */
				if (self == principal) {
					cf_debug(AS_PAXOS, "Principal applying sync message from %"PRIx64"", qm->id);
				}

				/*
				 * Check if we have already SYNC'd with a greater principal pro tempore.
				 */
				if (qm->id < p->principal_pro_tempore && as_hb_is_alive(p->principal_pro_tempore)) {
					cf_info(AS_PAXOS, "Ignoring sync message from principal %"PRIx64" < principal pro tempore %"PRIx64,
							qm->id, p->principal_pro_tempore);
					break;
				}

				if (0 != as_paxos_sync_msg_apply(qm->m)) {
					cf_warning(AS_PAXOS, "unable to apply received state from sync msg");
					break;
				}

				char sbuf[(AS_CLUSTER_SZ * 17) + 49];
				snprintf(sbuf, 49, "SUCCESSION [%d]@%"PRIx64"*: ", p->gen.sequence, qm->id);
				for (int i = 0; i < AS_CLUSTER_SZ; i++) {
					if ((cf_node)0 != p->succession[i]) {
						snprintf(sbuf + strlen(sbuf), 18, "%"PRIx64" ", p->succession[i]);
					} else {
						break;
					}
				}
				cf_info(AS_PAXOS, "%s", sbuf);
				as_paxos_print_cluster_key("SYNC MESSAGE");

				if (qm->id != p->succession[0]) {
					cf_warning(AS_PAXOS, "Received paxos sync message from someone who is not principal %"PRIx64"", qm->id);
				}

				/*
				 * The incoming SYNC message principal is now our principal pro tempore.
				 */
				cf_info(AS_PAXOS, "node %"PRIx64" is %s principal pro tempore",
						qm->id, (p->principal_pro_tempore != qm->id ? "now" : "still"));

				p->principal_pro_tempore = qm->id;

				/*
				 * Send the partition state to the principal as part of
				 * the partition sync request
				 */
				as_paxos_send_partition_sync_request(qm->id);
				break;
			case AS_PAXOS_MSG_COMMAND_PARTITION_SYNC_REQUEST:
				cf_debug(AS_PAXOS, "received partition sync request message from %"PRIx64"", qm->id);
				if (self != as_paxos_succession_getprincipal()) {
					cf_warning(AS_PAXOS, "Received paxos partition sync request - not a principal, ignoring ...");
					break;
				}
				/*
				 * First note this node's partition data, assuming the paxos sequence matches
				 * If all nodes have not responded, then do nothing.
				 * If all nodes have responded, send PARTITION_SYNC to all nodes.
				 */
				cf_info(AS_PAXOS, "Received paxos partition sync request from %"PRIx64"", qm->id);
				int npos; // find position of the sender in the succession list
				if (0 > (npos = as_paxos_get_succession_index(qm->id))) {
					/*
					 * This is an inconsistent state and is detected and fixed in the heartbeat code.
					 */
					cf_warning(AS_PAXOS, "Received paxos partition sync request from node not in succession list, ignoring ...");
					break;
				}
				bool already_sent_partition_sync_messages = as_paxos_partition_sync_states_all();
				/*
				 * apply the partition sync request
				 */
				if ((0 != as_paxos_partition_sync_request_msg_apply(qm->m, npos)) || (false == as_paxos_set_partition_sync_state(qm->id))) {
					cf_warning(AS_PAXOS, "unable to apply received state in partition sync request from node %"PRIx64"", qm->id);
					break;
				}

				if (true == as_paxos_partition_sync_states_all()) {
					if (already_sent_partition_sync_messages) { // this is a retransmission of partition sync messages
						cf_info(AS_PAXOS, "Re-sending paxos partition sync message to %"PRIx64"", p->succession[npos]);
						if (NULL == (reply = as_paxos_partition_sync_msg_generate()))
							cf_warning(AS_PAXOS, "unable to construct partition sync message to node %"PRIx64"", p->succession[npos]);
						else if (0 != as_fabric_send(p->succession[npos], reply, AS_FABRIC_CHANNEL_CTRL)) {
							as_fabric_msg_put(reply);
							cf_warning(AS_PAXOS, "unable to send partition sync message to node %"PRIx64"", p->succession[npos]);
						}
					}
					else { //sending partition sync message to all nodes
						cf_info(AS_PAXOS, "All partition data has been received by principal");
						for (int i = 1; i < AS_CLUSTER_SZ; i++) { /* skip the principal */
							if (p->succession[i] == 0) {
								break;
							}
							if (p->alive[i]) {
								cf_info(AS_PAXOS, "Sending paxos partition sync message to %"PRIx64"", p->succession[i]);
								if (NULL == (reply = as_paxos_partition_sync_msg_generate()))
									cf_warning(AS_PAXOS, "unable to construct partition sync message to node %"PRIx64"", p->succession[i]);
								else if (0 != as_fabric_send(p->succession[i], reply, AS_FABRIC_CHANNEL_CTRL)) {
									as_fabric_msg_put(reply);
									cf_warning(AS_PAXOS, "unable to send partition sync message to node %"PRIx64"", p->succession[i]);
								}
							}
						}

						/*
						 * Check if the state of this node is correct for applying a partition sync message
						 */
						if (as_partition_balance_are_migrations_allowed()) {
							cf_info(AS_PAXOS, "principal node allows migrations - ignoring duplicate partition sync message");

							break;
						}

						paxos_begin_partition_balance();

						if (p->cb) {
							as_exchange_cluster_changed_event c = {
									.cluster_key = as_exchange_cluster_key(),
									.cluster_size = as_exchange_cluster_size(),
									.succession = as_exchange_succession()
							};

							for (int i = 0; i < p->n_callbacks; i++) {
								(p->cb[i])(&c, p->cb_udata[i]);
							}
						}
					}
				}

				break;
			case AS_PAXOS_MSG_COMMAND_PARTITION_SYNC:
				cf_info(AS_PAXOS, "received partition sync message from %"PRIx64"", qm->id);
				/*
				 * Received the cluster's current partition data. Make sure that the paxos sequence matches
				 * Accept the message and continue normal processing if the paxos sequence matches
				 * Ignore message if the paxos sequence does not match.
				 */

				if (0 != as_paxos_partition_sync_msg_apply(qm->m)) {
					cf_detail(AS_PAXOS, "unable to apply partition sync message state");
					break;
				}

				/*
				 * We now need to perform migrations as a result of external
				 * synchronizations, since nodes outside the cluster could contain data due to a cluster merge
				 */
				paxos_begin_partition_balance();

				if (p->cb) {
					as_exchange_cluster_changed_event c = {
							.cluster_key = as_exchange_cluster_key(),
							.cluster_size = as_exchange_cluster_size(),
							.succession = as_exchange_succession()
					};

					for (int i = 0; i < p->n_callbacks; i++) {
						(p->cb[i])(&c, p->cb_udata[i]);
					}
				}
				break;
			default:
				cf_warning(AS_PAXOS, "unknown command %d received from %"PRIx64"", c, qm->id);
				break;
		}

cleanup:
		/* Free the message */
		as_fabric_msg_put(qm->m);
		cf_free(qm);
	}

	return NULL;
}

/**
 * Indicates if two paxos protocol identifiers are compatibile to coexist in the same cluster.
 */
static bool
as_paxos_are_proto_compatible(uint32_t protocol1, uint32_t protocol2)
{
	return AS_PAXOS_PROTOCOL_VERSION_NUMBER(protocol1) ==
	       AS_PAXOS_PROTOCOL_VERSION_NUMBER(protocol2);
}

/**
 * Get a pointer to the succession list in the message.
 *
 * @param msg the incoming message.
 * @param succession output. on success will point to the succession list in the
 * message.
 * @param succession_length output. on success will contain the length of the succession
 * list.
 * @param source the source node. Required for logging.
 * @return 0 on success. -1 if the succession list is absent.
 */
static int
as_paxos_hb_msg_succession_get(msg* msg, cf_node** succession,
			       size_t* succession_length, cf_node source)
{

	uint8_t* payload;
	size_t payload_size;
	int field_id = msg->type == M_TYPE_HEARTBEAT ?
			AS_HB_MSG_PAXOS_DATA : AS_HB_V2_MSG_ANV;

	if (msg_get_buf(msg, field_id, &payload, &payload_size,
			MSG_GET_DIRECT) != 0) {
		return -1;
	}

	if (msg->type == M_TYPE_HEARTBEAT) {
		// Check paxos protocol compatibility.
		uint32_t msg_protocol = *((uint32_t*)payload);
		uint32_t expected_protocol = AS_PAXOS_PROTOCOL_IDENTIFIER();
		if (!as_paxos_are_proto_compatible(expected_protocol,
						   msg_protocol)) {
			cf_warning(AS_PAXOS,
				   "Received message with incompatible paxos "
				   "protocol (expected %d, was %d) from node "
				   "%" PRIx64,
				   expected_protocol, msg_protocol, source);
			return -1;
		}

		*succession = (cf_node*)(payload + sizeof(uint32_t));

		// correct succession list length.
		*succession_length =
		  (payload_size - sizeof(uint32_t)) / sizeof(cf_node);
	} else {
		*succession = (cf_node*)payload;
		// The size of the succession list is AS_CLUSTER_SZ.
		// Succession list contains the current nodes and the rest of
		// the data is set to zero.
		*succession_length = payload_size / sizeof(cf_node);
	}

	return 0;
}

/**
 * Set the succession list on an outgoing messages.
 *
 * @param msg the outgoing message.
 * @param succession the succession list to set.
 * @para succession_length the length of the adjacecny list.
 */
static void
as_paxos_hb_msg_succession_set(msg* msg, uint8_t* payload, size_t payload_size)
{

	if (msg->type == M_TYPE_HEARTBEAT) {
		if (msg_set_buf(msg, AS_HB_MSG_PAXOS_DATA, payload,
				payload_size, MSG_SET_COPY) != 0) {
			cf_crash(AS_PAXOS,
				 "Error setting succession list on msg.");
		}
	} else {

		// Include the ANV length in all heartbeat protocol greater than v2.
		if (as_hb_protocol_get() != AS_HB_PROTOCOL_V1) {
			if (0 >
			    msg_set_uint32(msg, AS_HB_V2_MSG_ANV_LENGTH,
					   g_config.paxos_max_cluster_size)) {
				cf_crash(AS_HB, "Failed to set ANV "
						"length in heartbeat "
						"protocol v2 message.");
			}
		}

		if (msg_set_buf(msg, AS_HB_V2_MSG_ANV, payload, payload_size,
				MSG_SET_COPY) != 0) {
			cf_crash(AS_PAXOS,
				 "Error setting succession list on msg.");
		}
	}

	return;
}

/**
 * Set the succession list in an outgoing heartbeat pulse message.
 */
static void
as_paxos_hb_plugin_set_fn(msg* msg)
{
	if (as_new_clustering()) {
		// Ignore if in new clustering mode.
		return;
	}

	// TODO: Protect the succession list with a lock.
	size_t cluster_size = 0;

	if (msg->type == M_TYPE_HEARTBEAT_V2) {
		// In v1 and v2 we always send an array of max cluster size
		cluster_size = g_config.paxos_max_cluster_size;
	} else {
		// Recompute the cluster size. With v3 we only send the exact
		for (int i = 0; i < AS_CLUSTER_SZ; i++) {
			if (g_paxos->succession[i] == 0) {
				break;
			}
			cluster_size++;
		}
	}

	uint8_t* payload = alloca(
	  sizeof(uint32_t) // For the paxos version identifier
	  + (sizeof(cf_node) * cluster_size)); // For the succession list.

	size_t payload_size = 0;
	cf_node* succession = NULL;
	if (msg->type == M_TYPE_HEARTBEAT) {
		// set the paxos protocol identifier
		uint32_t protocol = AS_PAXOS_PROTOCOL_IDENTIFIER();
		memcpy(payload, &protocol, sizeof(protocol));

		succession = (cf_node*)(payload + sizeof(uint32_t));
		// For the paxos version identifier
		payload_size += sizeof(uint32_t);
	} else {
		succession = (cf_node*)payload;
	}

	memcpy(succession, g_paxos->succession, sizeof(cf_node) * cluster_size);

	// Populate succession list into the message.
	payload_size += (sizeof(cf_node) * cluster_size);

	as_paxos_hb_msg_succession_set(msg, payload, payload_size);
}

/**
 * Plugin function that parses succession list out of a heartbeat pulse message.
 */
static void
as_paxos_hb_plugin_parse_data_fn(msg* msg, cf_node source,
		as_hb_plugin_node_data* plugin_data)
{
	if (as_new_clustering()) {
		// Ignore if in new clustering mode.
		return;
	}

	size_t succession_length = 0;
	cf_node* succession = NULL;

	if (as_paxos_hb_msg_succession_get(msg, &succession, &succession_length,
			source) != 0) {
		// store a zero length succession list. Should not have happened.
		cf_warning(AS_PAXOS, "Unable to read succession list from heartbeat from node %" PRIx64,
				source);
		succession_length = 0;
	}

	size_t data_size = sizeof(size_t) + (succession_length * sizeof(cf_node));

	if (data_size > plugin_data->data_capacity) {
		// Round up to nearest multiple of block size to prevent very frequent
		// reallocation.
		size_t data_capacity = ((data_size + HB_PLUGIN_DATA_BLOCK_SIZE - 1) /
		HB_PLUGIN_DATA_BLOCK_SIZE) *
		HB_PLUGIN_DATA_BLOCK_SIZE;

		// Reallocate since we have outgrown existing capacity.
		plugin_data->data = cf_realloc(plugin_data->data, data_capacity);

		if (plugin_data->data == NULL) {
			cf_crash(
					AS_PAXOS,
					"Error allocating space for storing succession list for node %" PRIx64,
					source);
		}
		plugin_data->data_capacity = data_capacity;
	}

	plugin_data->data_size = data_size;
	memcpy(plugin_data->data, &succession_length, sizeof(size_t));

	if (succession_length) {
		cf_node* dest = (cf_node*)(plugin_data->data + sizeof(size_t));
		memcpy(dest, succession, succession_length * sizeof(cf_node));
	}
}

/**
 * Get succession list for node from the latest heartbeat pulse.
 */
static void
as_paxos_hb_get_succession_list(cf_node nodeid, cf_node* succession)
{
	// Initialize to an empty list.
	succession[0] = 0;

	as_hb_plugin_node_data plugin_data;
	// Initial data capacity.
	plugin_data.data_capacity = 1024;

	int tries_remaining = 3;
	while (tries_remaining--) {
		plugin_data.data = alloca(plugin_data.data_capacity);
		if (as_hb_plugin_data_get(nodeid, AS_HB_PLUGIN_PAXOS,
					  &plugin_data, NULL, NULL) == 0) {
			// Read success.
			break;
		}

		if (errno == ENOENT) {
			// No entry present for this node in heartbeat.
			return;
		}

		if (errno == ENOMEM) {
			plugin_data.data_capacity = plugin_data.data_size;
		}
	}

	if (tries_remaining < 0) {
		// Should never happen in practice.
		cf_crash(AS_PAXOS, "Error allocating space for paxos hb plugin data.");
	}

	size_t succession_length;
	cf_node* src = (cf_node*)(plugin_data.data + sizeof(size_t));
	memcpy(&succession_length, plugin_data.data, sizeof(size_t));

	if (succession_length > AS_CLUSTER_SZ) {
		cf_warning(AS_PAXOS, "node %" PRIx64 " has succession list of length %zu greater than max cluster size %d. Ignoring succession list.",
				nodeid, succession_length, AS_CLUSTER_SZ);
		succession[0] = 0;
		return;
	}

	// v3 does not send zero as the last element. Ensure the succession list
	// is zero terminated, assuming succession to be of the size
	// AS_CLUSTER_SZ.
	memset(succession, 0, AS_CLUSTER_SZ * sizeof(cf_node));
	memcpy(succession, src, succession_length * sizeof(cf_node));
}

/**
 * Get principal for a node from the latest heartbeat pulse.
 * @return the current principal based on latest heartbeat if node is adjacent,
 * else 0.
 */
static cf_node
as_paxos_hb_get_principal(cf_node nodeid)
{

	// Perform a general consistency check between our succession list and
	// the list that heart beat thinks is correct. First get a copy of the
	// heartbeat's compiled list for each node in our succession list.
	cf_node other_succession_list[AS_CLUSTER_SZ];
	memset(other_succession_list, 0, sizeof(other_succession_list));

	as_paxos_hb_get_succession_list(nodeid, other_succession_list);

	return other_succession_list[0];
}

/* as_paxos_init
 * Initialize the Paxos state structures */
void
as_paxos_init()
{
	g_paxos = cf_calloc(1, sizeof(as_paxos));
	cf_assert(g_paxos, AS_PAXOS, "allocation: %s", cf_strerror(errno));

	as_paxos *p = g_paxos; // shortcut pointer

	if (0 != pthread_mutex_init(&p->lock, NULL)) {
		cf_crash(AS_PAXOS, "unable to init mutex: %s", cf_strerror(errno));
	}

	as_paxos_current_init(p);
	p->msgq= cf_queue_priority_create(sizeof(void *), true);

	p->need_to_rebalance = false;
	p->ready = false;

	p->n_callbacks = 0;

	/* Register the paxos plugin for heartbeat subsystem. */
	as_hb_plugin paxos_plugin;
	memset(&paxos_plugin, 0, sizeof(paxos_plugin));
	paxos_plugin.id = AS_HB_PLUGIN_PAXOS;
	// Includes the size for the protocol version.
	paxos_plugin.wire_size_fixed =  sizeof(uint32_t);
	// Size of the node in succession list.
	paxos_plugin.wire_size_per_node = sizeof(cf_node);
	paxos_plugin.set_fn = as_paxos_hb_plugin_set_fn;
	paxos_plugin.parse_fn = as_paxos_hb_plugin_parse_data_fn;
	paxos_plugin.change_listener = NULL;
	as_hb_plugin_register(&paxos_plugin);

	/* Register with heartbeat*/
	as_hb_register_listener(as_paxos_event, NULL);

	/* Register with the fabric */
	as_fabric_register_msg_fn(
	  M_TYPE_PAXOS, as_paxos_msg_template, sizeof(as_paxos_msg_template),
	  AS_PAXOS_MSG_SCRATCH_SIZE, &as_paxos_msgq_push, NULL);

	/* Clean out the sync states array */
	memset(p->partition_sync_state, 0, sizeof(p->partition_sync_state));

	memset(p->succession, 0, sizeof(p->succession));

	memset(p->alive, 0, sizeof(p->alive));
	p->alive[0] = true;

	p->ready = true;
}

/*
 *  Register/deregister a Paxos cluster state change callback function.
 *
 *  XXX -- These two functions not are thread safe with respect to callbacks
 *          being simultaneously registered and deregistered (very unlikely),
 *          as well as if callbacks are being (de-)registered simultaneous with
 *          cluster state changes (also very unlikely), since the callback
 *          invocations happen on a separate thread (the Paxos thread.)
 */

int
as_paxos_register_change_callback(as_exchange_cluster_changed_cb cb, void *udata)
{
	as_paxos *p = g_paxos;

	if (p->n_callbacks < MAX_CHANGE_CALLBACKS - 1) {
		p->cb[p->n_callbacks] = cb;
		p->cb_udata[p->n_callbacks] = udata;
		p->n_callbacks++;
		return(0);
	}
	return(-1);
}

int
as_paxos_deregister_change_callback(as_exchange_cluster_changed_cb cb, void *udata)
{
	as_paxos *p = g_paxos;
	int i = 0;
	bool found = false;

	while (i < p->n_callbacks) {
		if (!found && (p->cb[i] == cb) && (p->cb_udata[i] == udata)) {
			found = true;
			p->cb[i] = NULL;
			p->cb_udata[i] = NULL;
		} else if (found) {
			p->cb[i - 1] = p->cb[i];
			p->cb_udata[i - 1] = p->cb_udata[i];
		}
		i++;
	}

	if (found) {
		p->n_callbacks--;
	}

	return (found ? 0 : -1);
}

/* as_paxos_sup_thr
 * paxos supervisor logic for retransmission */
void*
as_paxos_sup_thr(void* arg)
{
	cf_clock last_retransmit_ts = cf_getms();
	while (!as_new_clustering()) {

		as_paxos* p = g_paxos;
		size_t cluster_size = 0;
		for (int j = 0; j < AS_CLUSTER_SZ; j++) {
			if (p->succession[j] != (cf_node)0) {
				cluster_size++;
			} else {
				break;
			}
		}

		usleep(1000 * 100);

		// For larger clusters allow more time for partition rebalance to
		// finish.
		uint32_t retransmit_period_ms =
				MAX(as_hb_node_timeout_get() / 1000,
						MAX((int)(cluster_size * 0.5), g_config.paxos_retransmit_period))
						* 1000;

		cf_clock now = cf_getms();
		if (last_retransmit_ts + retransmit_period_ms < now) {
			// Drop a retransmit check paxos message into the paxos message
			// queue.
			as_paxos_retransmit_check();
			last_retransmit_ts = now;
		}
	}

	return (NULL);
}

/* as_paxos_start
 * Start the Paxos service */
void
as_paxos_start()
{
	uint64_t cluster_key;

	// Generate a non-zero cluster key that fits in 7 bytes.
	while ((cluster_key = (cf_get_rand64() >> 8)) == 0) {
		;
	}

	as_exchange_cluster_key_set(cluster_key);
	cf_info(AS_PAXOS, "cluster key set to %lx", cluster_key);

	g_paxos->succession[0] = g_config.self_node;
	as_exchange_succession_set(g_paxos->succession, 1);

	int32_t wait_ms = as_hb_node_timeout_get() * 2;

	// Wait at least 2 hb intervals to ensure we receive heartbeats and also
	// give the hb subsystem time to send out our heartbeats before starting
	// a new paxos round.
	uint32_t wait_interval_ms = MAX(wait_ms < 100 ? 100 : wait_ms / 100, 2 * as_hb_tx_interval_get());

	cf_info(AS_PAXOS, "listening for other nodes (max %u milliseconds) ...",
			wait_ms);

	while (wait_ms > 0) {
		usleep(wait_interval_ms * 1000);

		if (as_partition_balance_is_multi_node_cluster()) {
			// Heartbeats have been received from other node(s) - we'll be in a
			// multi-node cluster.
			cf_info(AS_PAXOS, "... other node(s) detected - node will operate in a multi-node cluster");
			break;
		}

		wait_ms -= wait_interval_ms;
	}

	if (wait_ms <= 0) {
		// Didn't hear from other nodes, assume we'll be a single node cluster.
		cf_info(AS_PAXOS, "... no other nodes detected - node will operate as a single-node cluster");

		as_partition_balance_init_single_node_cluster();
	}

	as_paxos *p = g_paxos;
	pthread_attr_t thr_attr;

	cf_info(AS_PAXOS, "starting paxos threads");

	/* Start the Paxos service thread */
	if (0 != pthread_attr_init(&thr_attr))
		cf_crash(AS_PAXOS, "unable to initialize thread attributes: %s", cf_strerror(errno));
	if (0 != pthread_attr_setscope(&thr_attr, PTHREAD_SCOPE_SYSTEM))
		cf_crash(AS_PAXOS, "unable to set thread scope: %s", cf_strerror(errno));
	if (0 != pthread_create(&g_thr_id, &thr_attr, as_paxos_thr, p))
		cf_crash(AS_PAXOS, "unable to create paxos thread: %s", cf_strerror(errno));
	if (0 != pthread_create(&g_sup_thr_id, 0, as_paxos_sup_thr, 0))
		cf_crash(AS_PAXOS, "unable to create paxos supervisor thread: %s", cf_strerror(errno));
}

/* as_paxos_dump
 * Print info. about the Paxos state to the log.
 * (Verbose true prints partition map as well.)
 */
void
as_paxos_dump(bool verbose)
{
	as_paxos *p = g_paxos;
	bool self = false, principal = false;

	cf_info(AS_PAXOS, "Current Cluster Size: %u", as_exchange_cluster_size());

	cf_info(AS_PAXOS, "Cluster Key: %"PRIx64"", as_exchange_cluster_key());

	cf_info(AS_PAXOS, "cluster generation: [%d]@%"PRIx64,
			p->gen.sequence, p->succession[0]);

	cf_info(AS_PAXOS, "Migrations are%s allowed.", (as_partition_balance_are_migrations_allowed() ? "" : " NOT"));

	// Print the succession list.
	cf_node principal_node = as_paxos_succession_getprincipal();
	for (int i = 0; i < AS_CLUSTER_SZ; i++) {
		cf_node node = p->succession[i];
		if ((cf_node) 0 == node) {
			break;
		}

		self = (node == g_config.self_node);
		principal = (node == principal_node);
		cf_info(AS_PAXOS, "SuccessionList[%d]: Node %"PRIx64" %s%s %s", i, node,
				(self ? "[Self]" : ""), (principal ? "[Principal]" : ""), (p->alive[i] ? "" : "DEAD"));
	}
}
