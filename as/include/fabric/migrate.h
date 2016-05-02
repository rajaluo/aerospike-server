/*
 * migrate.h
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
 * The migration module moves partition data from node to node
*/

#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_shash.h"

#include "msg.h"
#include "rchash.h"
#include "util.h"

#include "base/index.h"
#include "base/datamodel.h"


// For receiver-side migration flow-control.
// By default, allow up to 2 concurrent migrates from each member of the cluster.
#define AS_MIGRATE_DEFAULT_MAX_NUM_INCOMING (2 * AS_CLUSTER_SZ)

/*
 *  Default lifetime (in ms) for a migrate recv control object to stay in the recv control
 *  hash table after receiving the first START event.  This provides a time window to de-bounce
 *  re-transmitted migrate START message from crossing paths with the DONE ACK message.  After
 *  that interval, the RX control object will be reaped by the reaper thread.
 *
 *  (A value of 0 disables this feature and reaps objects immediately upon receipt of the DONE event.)
 */
#define AS_MIGRATE_DEFAULT_RX_LIFETIME_MS (60 * 1000) // 1 minute

/*
 *  Maximum permissible number of migrate xmit threads.
 */
#define MAX_NUM_MIGRATE_XMIT_THREADS  (100)

#define TX_FLAGS_NONE           ((uint32_t) 0x0)
#define TX_FLAGS_ACTING_MASTER  ((uint32_t) 0x1)
#define TX_FLAGS_REQUEST        ((uint32_t) 0x2)

// If 0 then it is a migration start from an old node.
#define MIG_TYPE_START_IS_NORMAL 1
#define MIG_TYPE_START_IS_REQUEST 2

typedef enum as_migrate_state_e {
	AS_MIGRATE_STATE_DONE,
	AS_MIGRATE_STATE_START,
	AS_MIGRATE_STATE_ERROR,
	AS_MIGRATE_STATE_EAGAIN
} as_migrate_state;

#define AS_MIGRATE_RX_STATE_SUBRECORD 1
#define AS_MIGRATE_RX_STATE_RECORD 2
typedef uint8_t as_partition_mig_rx_state;

// an a 'START' notification, the callback may return a value.
// If that value is -1, the migration will be abandoned (with 'ERROR' notification)
// If the value is -2, the migration will be tried again later (a subsequent START notification
// will be tried later)
//

typedef enum as_migrate_result_e {
	AS_MIGRATE_OK,
	AS_MIGRATE_FAIL,
	AS_MIGRATE_AGAIN,
	AS_MIGRATE_ALREADY_DONE
} as_migrate_result;

// A data structure for temporarily en-queuing partition migrations.
typedef struct partition_migrate_record_s {
	cf_node dest;
	as_namespace *ns;
	as_partition_id pid;
	uint64_t cluster_key;
	uint32_t tx_flags;
} partition_migrate_record;

// Public API.
void as_migrate_init();
void as_migrate_emigrate(const partition_migrate_record *pmr);
bool as_migrate_is_incoming(cf_digest *subrec_digest, uint64_t version, as_partition_id partition_id, int state);
void as_migrate_set_num_xmit_threads(int n_threads);
void as_migrate_dump(bool verbose);

as_migrate_result as_partition_migrate_rx(as_migrate_state s,
		as_namespace *ns, as_partition_id pid, uint64_t orig_cluster_key,
		uint32_t start_type, cf_node source_node);
as_migrate_result as_partition_migrate_tx(as_migrate_state s,
		as_namespace *ns, as_partition_id pid, uint64_t orig_cluster_key,
		uint32_t tx_flags);


//==========================================================
// Private API - for enterprise separation only.
//

typedef enum {
	// These values go on the wire, so mind backward compatibility if changing.
	MIG_FIELD_OP,
	MIG_FIELD_EMIG_INSERT_ID,
	MIG_FIELD_EMIG_ID,
	MIG_FIELD_NAMESPACE,
	MIG_FIELD_PARTITION,
	MIG_FIELD_DIGEST,
	MIG_FIELD_GENERATION,
	MIG_FIELD_RECORD,
	MIG_FIELD_CLUSTER_KEY,
	MIG_FIELD_VINFOSET, // deprecated
	MIG_FIELD_VOID_TIME,
	MIG_FIELD_TYPE, // deprecated
	MIG_FIELD_REC_PROPS,
	MIG_FIELD_INFO,
	MIG_FIELD_VERSION,
	MIG_FIELD_PDIGEST,
	MIG_FIELD_EDIGEST,
	MIG_FIELD_PGENERATION,
	MIG_FIELD_PVOID_TIME,
	MIG_FIELD_LAST_UPDATE_TIME,
	MIG_FIELD_FEATURES,
	MIG_FIELD_PARTITION_SIZE,
	MIG_FIELD_META_RECORDS,
	MIG_FIELD_META_SEQUENCE,
	MIG_FIELD_META_SEQUENCE_FINAL,

	NUM_MIG_FIELDS
} migrate_msg_fields;

#define OPERATION_UNDEF 0
#define OPERATION_INSERT 1
#define OPERATION_INSERT_ACK 2
#define OPERATION_START 3
#define OPERATION_START_ACK_OK 4
#define OPERATION_START_ACK_EAGAIN 5
#define OPERATION_START_ACK_FAIL 6
#define OPERATION_START_ACK_ALREADY_DONE 7
#define OPERATION_DONE 8
#define OPERATION_DONE_ACK 9
#define OPERATION_CANCEL 10 // deprecated
#define OPERATION_MERGE_META 11
#define OPERATION_MERGE_META_ACK 12

#define MIG_FEATURE_MERGE 0x00000001
#define MIG_FEATURES_SEEN 0x80000000 // needed for backward compatibility
extern const uint32_t MY_MIG_FEATURES;

typedef struct meta_record_s {
	cf_digest keyd;
	uint16_t generation;
	uint64_t last_update_time: 40;
} __attribute__ ((__packed__)) meta_record;

typedef struct meta_batch_s {
	bool is_final;
	uint32_t n_records;
	meta_record *records;
} meta_batch;

typedef struct emig_meta_q_s {
	uint32_t current_rec_i;
	meta_batch current_batch;
	cf_queue *batch_q;
	cf_atomic32 last_acked;
	bool is_done;
} emig_meta_q;

typedef struct emigration_s {
	cf_node     dest;
	uint64_t    cluster_key;
	uint32_t    id;
	uint32_t    tx_flags;
	cf_atomic32 state;
	bool        aborted;
	as_partition_mig_tx_state tx_state; // really only for LDT

	cf_atomic32 bytes_emigrating;
	shash       *reinsert_hash;
	cf_queue    *ctrl_q; // TODO - Remove, overkill for single dest.
	emig_meta_q *meta_q;

	as_partition_reservation rsv;
} emigration;

typedef struct immig_meta_q_s {
	meta_batch current_batch;
	cf_queue *batch_q;
	uint32_t sequence;
	cf_atomic32 last_acked;
} immig_meta_q;

typedef struct immigration_s {
	cf_node          src;
	uint64_t         cluster_key;
	as_partition_id  pid;
	as_partition_mig_rx_state rx_state; // really only for LDT
	uint64_t         incoming_ldt_version;

	cf_atomic32      done_recv;      // flag - 0 if not yet received, atomic counter for receives
	uint64_t         start_recv_ms;  // time the first START event was received
	uint64_t         done_recv_ms;   // time the first DONE event was received

	uint32_t         emig_id;
	immig_meta_q     meta_q;

	as_partition_reservation rsv;
} immigration;

typedef struct immigration_hkey_s {
	cf_node src;
	uint32_t emig_id;
} __attribute__((__packed__)) immigration_hkey;


// Globals.
extern rchash *g_emigration_hash;
extern rchash *g_immigration_hash;


// Emigration, immigration, & pickled record destructors.
void emigration_release(emigration *emig);
void immigration_release(immigration *immig);

// Emigration.
bool should_emigrate_record(emigration *emig, as_index_ref *r_ref);

// Emigration meta queue.
emig_meta_q *emig_meta_q_create();
void emig_meta_q_destroy(emig_meta_q *emq);
void emig_meta_q_push_batch(emig_meta_q *emq, const meta_batch *batch);

// Migrate fabric message handling.
void emigration_handle_meta_batch_request(cf_node src, msg *m);
void immigration_handle_meta_batch_ack(cf_node src, msg *m);

// Meta sender.
bool immigration_start_meta_sender(immigration *immig, uint32_t emig_features, uint32_t emig_n_recs);

// Immigration meta queue.
void immig_meta_q_init(immig_meta_q *imq);
void immig_meta_q_destroy(immig_meta_q *imq);
