/*
 * thr_proxy.c
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
 * Basic proxy system, which is called when there's an operation that can't be
 * done on this node, and is then forwarded to another node for processing.
 */


#include "base/thr_proxy.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_shash.h"

#include "fault.h"
#include "msg.h"
#include "util.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "base/udf_rw.h"
#include "fabric/fabric.h"
#include "fabric/paxos.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"


// Template for migrate messages.
#define PROXY_FIELD_OP 0
#define PROXY_FIELD_TID 1
#define PROXY_FIELD_DIGEST 2
#define PROXY_FIELD_REDIRECT 3
#define PROXY_FIELD_AS_PROTO 4 // request as_proto - currently contains only as_msg's
#define PROXY_FIELD_CLUSTER_KEY 5
#define PROXY_FIELD_TIMEOUT_MS 6 // deprecated
#define PROXY_FIELD_INFO 7

#define PROXY_INFO_SHIPPED_OP 0x0001

#define PROXY_OP_REQUEST 1
#define PROXY_OP_RESPONSE 2
#define PROXY_OP_RETURN_TO_SENDER 3

msg_template proxy_mt[] = {
	{ PROXY_FIELD_OP, M_FT_UINT32 },
	{ PROXY_FIELD_TID, M_FT_UINT32 },
	{ PROXY_FIELD_DIGEST, M_FT_BUF },
	{ PROXY_FIELD_REDIRECT, M_FT_UINT64 },
	{ PROXY_FIELD_AS_PROTO, M_FT_BUF },
	{ PROXY_FIELD_CLUSTER_KEY, M_FT_UINT64 },
	{ PROXY_FIELD_TIMEOUT_MS, M_FT_UINT32 },
	{ PROXY_FIELD_INFO, M_FT_UINT32 },
};

#define PROXY_MSG_SCRATCH_SIZE 128

typedef struct proxy_request_s {
	uint32_t		msg_fields;

	uint8_t			origin;
	uint8_t			from_flags;

	union {
		void*				any;
		as_file_handle*		proto_fd_h;
		as_batch_shared*	batch_shared;
		// No need for other members of this union.
	} from;

	// No need yet for a 'from_data" union.
	uint32_t		batch_index;

	rw_request		*rw; // origin info for ship-op proxies

	uint64_t		start_time;
	uint64_t		end_time;

	msg		        *fab_msg;    // this is the fabric message in case we have to retransmit
	cf_clock         xmit_ms;    // the ms time of the NEXT retransmit
	uint32_t         retry_interval_ms; // number of ms to wait next time

	cf_node          dest; // the node we're sending to
	// These are very helpful in the error case of needing to re-redirect.
	// Arguably, it's so rare that it's better to recalculate these.
	as_partition_id  pid;

	as_namespace    *ns;
} proxy_request;


static cf_atomic32   g_proxy_tid = 0;

static shash *g_proxy_hash = 0;

static pthread_t g_proxy_retransmit_th;


uint32_t
proxy_id_hash(void *value)
{
	return *(uint32_t *)value;
}


static inline void
client_proxy_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_PROTO_RESULT_OK:
		cf_atomic64_incr(&ns->n_client_proxy_success);
		break;
	case AS_PROTO_RESULT_FAIL_TIMEOUT:
		cf_atomic64_incr(&ns->n_client_proxy_timeout);
		break;
	default:
		cf_atomic64_incr(&ns->n_client_proxy_error);
		break;
	}
}


// Make a request to another node.
//
// Note: there's a cheat here. 'as_msg' is used in a raw form, and includes
// structured data (version - type - nfields - sz ...) which should be made more
// wire-protocol-friendly.
int
as_proxy_divert(cf_node dst, as_transaction *tr, as_namespace *ns, uint64_t cluster_key)
{
	cf_detail(AS_PROXY, "proxy divert");

	cf_atomic_int_incr(&g_config.stat_proxy_reqs);
	if (tr->msgp && (tr->msgp->msg.info1 & AS_MSG_INFO1_XDR)) {
		cf_atomic_int_incr(&g_config.stat_proxy_reqs_xdr);
	}
	as_partition_id pid = as_partition_getid(tr->keyd);

	if (dst == 0) {
		// Get the list of replicas.
		dst = as_partition_getreplica_read(ns, pid);
	}

	// Create a fabric message, fill it out.
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);
	if (!m)	{
		return -1;
	}

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void *) &tr->keyd, sizeof(cf_digest), MSG_SET_COPY);
	msg_set_type msettype = tr->origin == FROM_BATCH ? MSG_SET_COPY : MSG_SET_HANDOFF_MALLOC;
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void *) tr->msgp, as_proto_size_get(&tr->msgp->proto), msettype);
	msg_set_uint64(m, PROXY_FIELD_CLUSTER_KEY, cluster_key);

	tr->msgp = NULL; // pattern, not needed

	cf_debug_digest(AS_PROXY, &tr->keyd, "proxy_divert: fab_msg %p dst %"PRIx64, m, dst);

	// Fill out a retransmit structure, insert into the retransmit hash.
	msg_incr_ref(m);

	proxy_request pr;

	pr.msg_fields = tr->msg_fields;

	pr.origin = tr->origin;
	pr.from_flags = tr->from_flags;
	pr.from.any = tr->from.any;
	pr.batch_index = tr->from_data.batch_index;
	tr->from.any = NULL;  // pattern, not needed

	pr.rw = NULL;

	pr.start_time = tr->start_time;
	pr.end_time = tr->end_time;

	pr.fab_msg = m;
	pr.xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	pr.retry_interval_ms = g_config.transaction_retry_ms;

	pr.dest = dst;
	pr.pid = pid;
	pr.ns = ns;

	if (0 != shash_put(g_proxy_hash, &tid, &pr)) {
		cf_debug(AS_PROXY, " shash_put failed, need cleanup code");
		return -1;
	}

	// Send to the remote node.
	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_debug(AS_PROXY, "as_proxy_divert: returned error %d", rv);
		as_fabric_msg_put(m);
	}

	cf_atomic_int_incr(&g_config.proxy_initiate);

	return 0;
}


int
as_proxy_shipop(cf_node dst, rw_request *rw)
{
	as_partition_id pid = as_partition_getid(rw->keyd);

	if (dst == 0) {
		cf_crash(AS_PROXY, "the destination should never be zero");
	}

	// Create a fabric message, fill it out.
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);
	if (!m)	{
		return -1;
	}

	uint32_t tid = cf_atomic32_incr(&g_proxy_tid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_REQUEST);
	msg_set_uint32(m, PROXY_FIELD_TID, tid);
	msg_set_buf(m, PROXY_FIELD_DIGEST, (void *) &rw->keyd, sizeof(cf_digest), MSG_SET_COPY);
	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (void *) rw->msgp, as_proto_size_get(&rw->msgp->proto), MSG_SET_HANDOFF_MALLOC);
	msg_set_uint64(m, PROXY_FIELD_CLUSTER_KEY, as_paxos_get_cluster_key());
	rw->msgp = NULL;

	// If it is shipped op.
	uint32_t info = 0;
	info |= PROXY_INFO_SHIPPED_OP;
	msg_set_uint32(m, PROXY_FIELD_INFO, info);

	cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP %s->WINNER msg %p Proxy Sent to %"PRIx64" %p tid(%d)",
			rw->from.proxy_node != 0 ? "NONORIG" : "ORIG", m, dst, rw, tid);

	// Fill out a retransmit structure, insert into the retransmit hash.
	msg_incr_ref(m);
	proxy_request pr;

	pr.msg_fields = rw->msg_fields;

	pr.origin = 0;
	pr.from_flags = 0;
	pr.from.any = NULL;
	pr.batch_index = 0;

	cf_rc_reserve(rw);
	pr.rw = rw;

	pr.start_time = rw->start_time;
	pr.end_time = rw->end_time;

	pr.fab_msg = m;
	pr.xmit_ms = cf_getms() + g_config.transaction_retry_ms;
	pr.retry_interval_ms = g_config.transaction_retry_ms;

	pr.dest = dst;
	pr.pid = pid;
	pr.ns = NULL; // needed only for retry, which ship-op doesn't do

	if (0 != shash_put(g_proxy_hash, &tid, &pr)) {
		cf_info(AS_PROXY, " shash_put failed, need cleanup code");
		return -1;
	}

	// Send to the remote node.
	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_detail(AS_PROXY, "SHIPPED_OP ORIG [Digest %"PRIx64"] Failed with %d", *(uint64_t *)&rw->keyd, rv);
		as_fabric_msg_put(m);
	}

	cf_atomic_int_incr(&g_config.ldt_proxy_initiate);

	return 0;
}


/*
 * The work horse function to process the acknowledgment for the duplicate op.
 * It is received after the intended node has finished performing the op. In
 * case of success the op would have been successfully performed and replicated.
 * In case of failure the op would not have been performed anywhere.
 *
 * The retransmit is handled by making sure op hangs from the write hash as long
 * as it is not applied or failed. Any attempt to perform next operation has to
 * hang behind it unless it is finished. Also operation is assigned a timestamp
 * so that there is some protection in case the op arrives out of order, or the
 * same op comes back again. That would be a duplicate op ...
 *
 * Received a op message - I'm a winner duplicate on this partition. Perform the
 * UDF op and replicate to all the nodes in the replica list. We only replicate
 * the subrecord if the partition is in subrecord migration phase. If not, ship
 * both subrecord and record. In case partition is read replica on this node, do
 * the write and signal back that I'm done.
 *
 * THUS - PROLE SIDE
 *
 * is_write is misnamed. Differentiates between the 'duplicate' phase and the
 *    'operation' phase. If is_write == false, we're in the 'duplicate' phase.
 *
 * Algorithm
 *
 * This code is called when op is shipped to the winner node.
 *
 * 1. Assert that current node is indeed the winner node.
 * 2. Assert the cluster key matches.
 * 3. Create a transaction and apply the UDF. Create an internal transaction and
 *    make sure it does some sort of reservation and applies the write and
 *    replicates to replica set. Once the write is done it sends the op ack.
 *
 *    TODO: How do you handle retransmits?
 *    TODO: How do you handle partition reservation? Is it something special.
 *    TODO: How to send along with replication request? Same infra should be
 *          used by normal replication as well.
 *
 *    There won't be any deadlock because the requests are triggered from the
 *    write. Get down to the udf apply code. Replicate to replica set and then
 *    make sure the response is sent back to the originating node. This node has
 *    to make sure the replication actually did succeed.
 *
 * In the response code you need to add the callback function.
 */
int
as_proxy_shipop_response_hdlr(msg *m, proxy_request *pr, bool *free_msg)
{
	int rv            = -1;
	rw_request *rw = pr->rw;
	if (! rw) {
		return -1;
	}
	cf_assert((pr->origin == 0), AS_PROXY, CF_WARNING, "origin set for shipop proxy response");

	// If there is a write request hanging from pr then this is a response to
	// the proxy ship op request. This node is the resolving node (node @ which
	// duplicate resolution was triggered). It could be:
	// 1. Originating node [where the request was sent from the client] - in
	//    that case send response back to the client directly.
	// 2. Non-originating node [where the request arrived as a regular proxy] -
	//    in that case send response back to the proxy originating node.

	// Case 1: Non-originating node.
	if (rw->origin == FROM_PROXY) {
		msg_preserve_fields(m, 2, PROXY_FIELD_OP, PROXY_FIELD_AS_PROTO);
		// Remember that "digest" gets printed at the end of cf_detail_digest().
		// Fake the ORIGINATING Proxy tid
		msg_set_uint32(m, PROXY_FIELD_TID, rw->from_data.proxy_tid);
		cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP NON-ORIG :: Got Op Response(%p) :", rw);
		cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP NON-ORIG :: Back Forwarding Response for tid (%d). : ", rw->from_data.proxy_tid);
		if (0 != (rv = as_fabric_send(rw->from.proxy_node, m, AS_FABRIC_PRIORITY_MEDIUM))) {
			cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP NONORIG Failed Forwarding Response");
			as_fabric_msg_put(m);
		}
		*free_msg = false;
	}
	// Case 2: Originating node. FROM_CLIENT
	else {
		cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP ORIG Got Op Response");
		pthread_mutex_lock(&rw->lock);
		if (rw->origin == FROM_CLIENT) {
			if (! rw->from.proto_fd_h->fd) {
				cf_warning_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP ORIG Missing fd in proto_fd ");
			}
			else {
				as_proto *proto;
				size_t proto_sz;
				if (0 != msg_get_buf(m, PROXY_FIELD_AS_PROTO, (byte **) &proto, &proto_sz, MSG_GET_DIRECT)) {
					cf_info(AS_PROXY, "msg get buf failed!");
				}

				size_t pos = 0;
				while (pos < proto_sz) {
					rv = send(rw->from.proto_fd_h->fd, (((uint8_t *)proto) + pos), proto_sz - pos, MSG_NOSIGNAL);
					if (rv > 0) {
						pos += rv;
					}
					else if (rv < 0) {
						if (errno != EWOULDBLOCK) {
							// Common message when a client aborts.
							cf_debug(AS_PROTO, "protocol proxy write fail: fd %d "
									"sz %zu pos %zu rv %d errno %d",
									rw->from.proto_fd_h->fd, proto_sz, pos, rv, errno);
							as_end_of_transaction_force_close(rw->from.proto_fd_h);
							rw->from.proto_fd_h = NULL;
							break;
						}
						usleep(1); // yield
					}
					else {
						cf_info(AS_PROTO, "protocol write fail zero return: fd %d sz %zu pos %zu ",
								rw->from.proto_fd_h->fd, proto_sz, pos);
						as_end_of_transaction_force_close(rw->from.proto_fd_h);
						rw->from.proto_fd_h = NULL;
						break;
					}
				}
				cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP ORIG Response Sent to Client");

				if (rw->from.proto_fd_h) {
					as_end_of_transaction_ok(rw->from.proto_fd_h);
					rw->from.proto_fd_h = NULL; // pattern, not needed
				}
			}
		} else {
			// this may be NULL if the request has already timedout and the wr proto_fd_h
			// will be cleaned up by then
			cf_detail_digest(AS_PROXY, &rw->keyd, "SHIPPED_OP ORIG Missing proto_fd ");

			if (rw->origin == FROM_IUDF) {
				// TODO - can we really get here?
				rw->from.iudf_orig->cb(rw->from.iudf_orig->udata, 0);
				rw->from.iudf_orig = NULL;
			}
		}
		pthread_mutex_unlock(&rw->lock);
	}

	// This node is the ship-op initiator - remove rw_request from hash.

	rw_request_hkey hkey = { rw->rsv.ns->id, rw->keyd };

	rw_request_hash_delete(&hkey);
	rw_request_release(rw);
	pr->rw = NULL;

	return 0;
}


// Incoming messages start here.
// - Could get a request that we need to service.
// - Could get a response to one of our requests - need to find the request and
//   send the real response to the remote end.
int
proxy_msg_fn(cf_node id, msg *m, void *udata)
{
	int rv;

	if (cf_rc_count((void*)m) == 0) {
		cf_debug(AS_PROXY, " proxy_msg_fn was given a refcount 0 message! Someone has been naugty %p", m);
		return -1;
	}

	uint32_t op = 99999;
	msg_get_uint32(m, PROXY_FIELD_OP, &op);
	uint32_t transaction_id = 0;
	msg_get_uint32(m, PROXY_FIELD_TID, &transaction_id);

	cf_detail(AS_PROXY, "received proxy message: tid %d type %d from %"PRIx64, transaction_id, op, id);

	switch (op) {
		case PROXY_OP_REQUEST:
		{
			cf_atomic_int_incr(&g_config.proxy_action);

			cf_digest *key;
			size_t sz = 0;

			if (0 != msg_get_buf(m, PROXY_FIELD_DIGEST, (byte **) &key, &sz, MSG_GET_DIRECT)) {
				cf_info(AS_PROXY, "proxy msg function: no digest, problem");
				as_fabric_msg_put(m);
				return 0;
			}
			cl_msg *msgp;
			sz = 0;
			if (0 != msg_get_buf(m, PROXY_FIELD_AS_PROTO, (byte **) &msgp, &sz, MSG_GET_COPY_MALLOC)) {
				cf_info(AS_PROXY, "proxy msg function: no as msg, problem");
				as_fabric_msg_put(m);
				return 0;
			}

			// Sanity check as_proto fields.
			as_proto *proto = &msgp->proto;
			if (! as_proto_wrapped_is_valid(proto, sz)) {
				cf_warning(AS_PROXY, "proxyee got unusable proto: version %u, type %u, sz %lu [%lu]",
						proto->version, proto->type, (uint64_t)proto->sz, sz);
				as_fabric_msg_put(m);
				as_proxy_send_response(id, transaction_id, AS_PROTO_RESULT_FAIL_UNKNOWN,
						0, 0, NULL, NULL, 0, NULL, 0, NULL);
				return 0;
			}

			uint32_t info = 0;
			msg_get_uint32(m, PROXY_FIELD_INFO, &info);

			if ((info & PROXY_INFO_SHIPPED_OP) != 0) {
				uint64_t cluster_key = 0;
				if (0 == msg_get_uint64(m, PROXY_FIELD_CLUSTER_KEY, &cluster_key) &&
						cluster_key != as_paxos_get_cluster_key()) {
					as_fabric_msg_put(m);
					as_proxy_send_response(id, transaction_id, AS_PROTO_RESULT_FAIL_CLUSTER_KEY_MISMATCH,
							0, 0, NULL, NULL, 0, NULL, 0, NULL);
					return 0;
				}
			}

			// Put the as_msg on the normal queue for processing.
			// INIT_TR
			as_transaction tr;
			as_transaction_init_head(&tr, key, msgp);
			// msgp might not have digest - batch sub-transactions, old clients.
			// For old clients, will compute it again from msgp key and set.

			tr.start_time = cf_getns();
			MICROBENCHMARK_SET_TO_START();

			tr.origin = FROM_PROXY;
			tr.from.proxy_node = id;
			tr.from_data.proxy_tid = transaction_id;
			as_transaction_proxyee_prepare(&tr);

			// For batch sub-transactions, make sure we flag them so they're not
			// mistaken for multi-record transactions (which never proxy).
			if (as_transaction_has_no_key_or_digest(&tr)) {
				tr.from_flags |= FROM_FLAG_BATCH_SUB;
			}

			// Flag shipped ops.
			if ((info & PROXY_INFO_SHIPPED_OP) != 0) {
				tr.from_flags |= FROM_FLAG_SHIPPED_OP;
			}

			as_fabric_msg_put(m);
			thr_tsvc_enqueue(&tr);
		}
		break;

		case PROXY_OP_RESPONSE:
		{
			// Look up the element.
			proxy_request pr;
			bool free_msg = true;

			if (SHASH_OK == shash_get_and_delete(g_proxy_hash, &transaction_id, &pr)) {
				// Found the element (sometimes we get two acks so it's OK for
				// an ack to not find the transaction).

				if (pr.rw) {
					as_proxy_shipop_response_hdlr(m, &pr, &free_msg);
				}
				else {
					as_proto *proto;
					size_t proto_sz;
					if (0 != msg_get_buf(m, PROXY_FIELD_AS_PROTO, (byte **) &proto, &proto_sz, MSG_GET_DIRECT)) {
						cf_info(AS_PROXY, "msg get buf failed!");
					}

					if (pr.origin == FROM_BATCH) {
						cf_assert(pr.from.batch_shared, AS_PROXY, CF_CRITICAL, "null batch shared");

						cf_digest* digest;
						size_t digest_sz = 0;

						if (msg_get_buf(pr.fab_msg, PROXY_FIELD_DIGEST, (byte **)&digest, &digest_sz, MSG_GET_DIRECT) == 0) {
							as_batch_add_proxy_result(pr.from.batch_shared, pr.batch_index, digest, (cl_msg*)proto, proto_sz);
						}
						else {
							cf_warning(AS_PROXY, "Failed to find batch proxy digest %u", transaction_id);
							as_batch_add_error(pr.from.batch_shared, pr.batch_index, AS_PROTO_RESULT_FAIL_UNKNOWN);
						}

						pr.from.batch_shared = NULL; // pattern, not needed
						// Note - no worries about msgp, proxy divert copied it.
					}
					else { // FROM_CLIENT - TODO - make this a switch!
						cf_assert(pr.origin == FROM_CLIENT, AS_PROXY, CF_CRITICAL, "proxy response for unexpected origin %u", pr.origin);
						cf_assert(pr.from.proto_fd_h, AS_PROXY, CF_CRITICAL, "null file handle");

						int stat_result = AS_PROTO_RESULT_OK;

						size_t pos = 0;
						while (pos < proto_sz) {
							rv = send(pr.from.proto_fd_h->fd, (((uint8_t *)proto) + pos), proto_sz - pos, MSG_NOSIGNAL);
							if (rv > 0) {
								pos += rv;
							}
							else if (rv < 0) {
								if (errno != EWOULDBLOCK) {
									// Common when a client aborts.
									as_end_of_transaction_force_close(pr.from.proto_fd_h);
									pr.from.proto_fd_h = NULL;
									stat_result = AS_PROTO_RESULT_FAIL_UNKNOWN;
									break;
								}
								usleep(1); // yield
							}
							else {
								cf_warning(AS_PROTO, "protocol write fail zero return: fd %d sz %zu pos %zu ", pr.from.proto_fd_h->fd, proto_sz, pos);
								as_end_of_transaction_force_close(pr.from.proto_fd_h);
								pr.from.proto_fd_h = NULL;
								stat_result = AS_PROTO_RESULT_FAIL_UNKNOWN;
								break;
							}
						}

						// Return the fabric message or the direct file descriptor -
						// after write and complete.
						if (pr.from.proto_fd_h) {
							as_end_of_transaction_ok(pr.from.proto_fd_h);
							pr.from.proto_fd_h = NULL; // pattern, not needed
						}

						// TODO - is scope incomplete?
						client_proxy_update_stats(pr.ns, stat_result);
					}

					as_fabric_msg_put(pr.fab_msg);
					pr.fab_msg = 0;

					// TODO - not sure if this should care about origin.
					histogram_insert_data_point(pr.ns->proxy_hist, pr.start_time);
				}
			}
			else {
				cf_debug(AS_PROXY, "proxy: received result but no transaction, tid %d", transaction_id);
			}

			if (free_msg) {
				as_fabric_msg_put(m);
			}
		}
		break;

		case PROXY_OP_RETURN_TO_SENDER:
		{
			// Look in the proxy retransmit hash for the tid.
			proxy_request *pr;
			pthread_mutex_t *pr_lock;
			int r = 0;

			if (0 != (r = shash_get_vlock(g_proxy_hash, &transaction_id, (void **)&pr, &pr_lock))) {
				cf_debug(AS_PROXY, "redirect: could not find transaction %d", transaction_id);
				as_fabric_msg_put(m);
				return -1;
			}

			cf_node redirect_node;
			if (0 == msg_get_uint64(m, PROXY_FIELD_REDIRECT, &redirect_node)
					&& redirect_node != g_config.self_node
					&& redirect_node != (cf_node)0) {
				// If this node was a "random" node, i.e. neither acting nor
				// eventual master, it diverts to the eventual master (the best
				// it can do.) The eventual master must inform this node about
				// the acting master.
				pr->dest = redirect_node;
				pr->xmit_ms = cf_getms() + g_config.transaction_retry_ms;

				msg_incr_ref(pr->fab_msg);

				if (0 != (rv = as_fabric_send(pr->dest, pr->fab_msg, AS_FABRIC_PRIORITY_MEDIUM))) {
					as_fabric_msg_put(pr->fab_msg);
				}

				pthread_mutex_unlock(pr_lock);

				as_fabric_msg_put(m);

				return rv == 0 ? 0 : -1;
			}

			cf_digest *key;
			size_t sz = 0;
			if (0 != msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (byte **) &key, &sz, MSG_GET_DIRECT)) {
				cf_warning(AS_PROXY, "op_redirect: proxy msg function: no digest, problem");
				pthread_mutex_unlock(pr_lock);
				as_fabric_msg_put(m);
				return -1;
			}

			cl_msg *msgp;
			sz = 0;
			if (0 != msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (byte **) &msgp, &sz, MSG_GET_COPY_MALLOC)) {
				cf_warning(AS_PROXY, "op_redirect: proxy msg function: no as proto, problem");
				pthread_mutex_unlock(pr_lock);
				as_fabric_msg_put(m);
				return -1;
			}

			// Put the as_msg on the normal queue for processing.
			as_transaction tr;
			as_transaction_init_head(&tr, key, msgp);
			// msgp might not have digest - batch sub-transactions, old clients.
			// For old clients, will compute it again from msgp key and set.

			tr.msg_fields = pr->msg_fields;
			tr.origin = pr->origin;
			tr.from_flags = pr->from_flags;
			tr.from.any = pr->from.any;
			tr.from_data.batch_index = pr->batch_index;
			tr.start_time = pr->start_time;

			MICROBENCHMARK_RESET();

			thr_tsvc_enqueue(&tr);

			as_fabric_msg_put(pr->fab_msg);
			shash_delete_lockfree(g_proxy_hash, &transaction_id);

			pthread_mutex_unlock(pr_lock);

			as_fabric_msg_put(m);
		}
		break;

		default:
			cf_debug(AS_PROXY, "proxy_msg_fn: received unknown, unsupported message %d from remote endpoint", op);
			msg_dump(m, "proxy received unknown msg");
			as_fabric_msg_put(m);
			break;
	} // end switch

	return 0;
} // end proxy_msg_fn()


// Send a redirection message - consumes the message.
void
as_proxy_return_to_sender(const as_transaction *tr, as_namespace *ns)
{
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	as_partition_id pid = as_partition_getid(tr->keyd);
	cf_node redirect_node = as_partition_proxyee_redirect(ns, pid);

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RETURN_TO_SENDER);
	msg_set_uint32(m, PROXY_FIELD_TID, tr->from_data.proxy_tid);
	msg_set_uint64(m, PROXY_FIELD_REDIRECT,
			redirect_node == (cf_node)0 ? tr->from.proxy_node : redirect_node);

	if (0 != as_fabric_send(tr->from.proxy_node, m, AS_FABRIC_PRIORITY_MEDIUM)) {
		as_fabric_msg_put(m);
	}
} // end as_proxy_send_redirect()


// Looked up the message in the store. Time to send the response value back to
// the requester. The CF_BYTEARRAY is handed off in this case. If you want to
// keep a reference, then keep the reference yourself.
void
as_proxy_send_response(cf_node dst, uint32_t proxy_tid, uint32_t result_code,
		uint32_t generation, uint32_t void_time, as_msg_op **ops, as_bin **bins,
		uint16_t bin_count, as_namespace *ns, uint64_t trid,
		const char *setname)
{
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, proxy_tid);

	size_t msg_sz = 0;
	cl_msg * msgp = as_msg_make_response_msg(result_code, generation, void_time, ops,
			bins, bin_count, ns, 0, &msg_sz, trid, setname);

	msg_set_buf(m, PROXY_FIELD_AS_PROTO, (byte *) msgp, msg_sz, MSG_SET_HANDOFF_MALLOC);

	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_debug(AS_PROXY, "sending proxy response: fabric send err %d, catch you on the retry", rv);
		as_fabric_msg_put(m);
	}
} // end as_proxy_send_response()

void
as_proxy_send_ops_response(cf_node dst, uint32_t proxy_tid, cf_dyn_buf *db)
{
	msg *m = as_fabric_msg_get(M_TYPE_PROXY);

	if (! m) {
		return;
	}

	msg_set_uint32(m, PROXY_FIELD_OP, PROXY_OP_RESPONSE);
	msg_set_uint32(m, PROXY_FIELD_TID, proxy_tid);

	uint8_t *msgp = db->buf;
	size_t msg_sz = db->used_sz;

	if (db->is_stack) {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_COPY);
	}
	else {
		msg_set_buf(m, PROXY_FIELD_AS_PROTO, msgp, msg_sz, MSG_SET_HANDOFF_MALLOC);
		db->buf = NULL; // the fabric owns the buffer now
	}

	int rv = as_fabric_send(dst, m, AS_FABRIC_PRIORITY_MEDIUM);
	if (rv != 0) {
		cf_debug(AS_PROXY, "sending proxy response: fabric send err %d, catch you on the retry", rv);
		as_fabric_msg_put(m);
	}
} // end as_proxy_send_ops_response()


//
// RETRANSMIT FUNCTIONS
//

typedef struct now_times_s {
	uint64_t now_ns;
	uint64_t now_ms;
} now_times;

// Reduce through the outstanding requests and retransmit sometime.
int
proxy_retransmit_reduce_fn(void *key, void *data, void *udata)
{
	proxy_request *pr = data;
	now_times *p_now = (now_times*)udata;

	if (pr->xmit_ms < p_now->now_ms) {

		cf_debug(AS_PROXY, "proxy_retransmit: now %"PRIu64" xmit_ms %"PRIu64" m %p", p_now->now_ms, pr->xmit_ms, pr->fab_msg);

		// Determine if the time is too much, and terminate if so.
		if (p_now->now_ns > pr->end_time) {

			// Can get very verbose, when another server is slow.
			cf_debug(AS_PROXY, "proxy_retransmit: too old request %zu ms: terminating (dest %"PRIx64" {%s:%d}",
					(p_now->now_ns - pr->start_time) / 1000000, pr->dest, pr->ns->name, pr->pid);

			if (pr->rw) {
				cf_detail_digest(AS_PROXY, &pr->rw->keyd, "SHIPPED_OP Proxy Retransmit Timeout ...");
				cf_atomic_int_incr(&g_config.ldt_proxy_timeout);
				pthread_mutex_lock(&pr->rw->lock);
				if (pr->rw->origin == FROM_IUDF) {
					// TODO - can we really get here?
					pr->rw->from.iudf_orig->cb(pr->rw->from.iudf_orig->udata, AS_PROTO_RESULT_FAIL_TIMEOUT);
					pr->rw->from.iudf_orig = NULL;
				}
				pthread_mutex_unlock(&pr->rw->lock);
				rw_request_release(pr->rw);
				pr->rw = NULL;
			}

			if (pr->fab_msg) {
				as_fabric_msg_put(pr->fab_msg);
				pr->fab_msg = 0;
			}

			switch (pr->origin) {
			case FROM_CLIENT:
				cf_assert(pr->from.proto_fd_h, AS_PROXY, CF_CRITICAL, "null file handle");
				as_end_of_transaction_force_close(pr->from.proto_fd_h);
				pr->from.proto_fd_h = NULL; // pattern, not needed
				client_proxy_update_stats(pr->ns, AS_PROTO_RESULT_FAIL_TIMEOUT);
				break;
			case FROM_BATCH:
				cf_assert(pr->from.batch_shared, AS_PROXY, CF_CRITICAL, "null batch shared");
				as_batch_add_error(pr->from.batch_shared, pr->batch_index, AS_PROTO_RESULT_FAIL_TIMEOUT);
				pr->from.batch_shared = NULL; // pattern, not needed
				// Note - no worries about msgp, proxy divert copied it.
				break;
			case FROM_PROXY:
			case FROM_IUDF:
			case FROM_NSUP:
				// Proxyees, internal UDFs, and nsup deletes don't proxy.
			default:
				cf_crash(AS_PROXY, "unexpected transaction origin %u", pr->origin);
				break;
			}

			return SHASH_REDUCE_DELETE;
		}

		// Update the retry interval, exponentially.
		pr->xmit_ms = p_now->now_ms + pr->retry_interval_ms;
		pr->retry_interval_ms *= 2;

		// msg_dump(pr->fab_msg, "proxy retransmit");
		msg_incr_ref(pr->fab_msg);
		int try = 0;

Retry:
		;
		if (try++ > 5) {
			cf_info(AS_PROXY, "retransmit loop detected: bailing");
			as_fabric_msg_put(pr->fab_msg);
			return 0;
		}

		if (pr->rw) {
			cf_detail_digest(AS_PROXY, &pr->rw->keyd, "SHIPPED_OP Proxy Retransmit... NOOP");
			as_fabric_msg_put(pr->fab_msg);
			return 0;
		}

		cf_atomic_int_incr(&g_config.proxy_retry);

		int rv = as_fabric_send(pr->dest, pr->fab_msg, AS_FABRIC_PRIORITY_MEDIUM);

		if (rv == 0) {
			return 0;
		}

		if (rv == AS_FABRIC_ERR_QUEUE_FULL) {
			cf_debug(AS_PROXY, "retransmit queue full");
			as_fabric_msg_put(pr->fab_msg);
			cf_atomic_int_incr(&g_config.proxy_retry_q_full);
			return -1;
		}
		else if (rv == -3) {

			// The node I'm proxying to is no longer up. Find another node.
			// (Easier to just send to the master and not pay attention to
			// whether it's read or write.)
			cf_node new_dst = as_partition_getreplica_write(pr->ns, pr->pid);
			cf_debug(AS_PROXY, "node failed with proxies in flight: trying alternative node %"PRIx64, new_dst);

			// Need to "unproxy" and divert back to main queue because
			// destination is self.
			if (new_dst == g_config.self_node) {

				cf_digest *keyp;
				size_t sz = 0;
				msg_get_buf(pr->fab_msg, PROXY_FIELD_DIGEST, (byte **) &keyp, &sz, MSG_GET_DIRECT);

				cl_msg *msgp;
				sz = 0;
				msg_get_buf(pr->fab_msg, PROXY_FIELD_AS_PROTO, (byte **) &msgp, &sz, MSG_GET_COPY_MALLOC);

				// INIT_TR
				as_transaction tr;
				as_transaction_init_head(&tr, keyp, msgp);
				// msgp might not have digest - batch sub-transactions, old clients.
				// For old clients, will compute it again from msgp key and set.

				tr.msg_fields = pr->msg_fields;
				tr.origin = pr->origin;
				tr.from_flags = pr->from_flags;
				tr.from.any = pr->from.any;
				tr.from_data.batch_index = pr->batch_index;
				tr.start_time = pr->start_time;

				cf_atomic_int_incr(&g_config.proxy_unproxy);

				MICROBENCHMARK_RESET();

				thr_tsvc_enqueue(&tr);

				// Getting deleted - cleanup the proxy request.
				as_fabric_msg_put(pr->fab_msg);
				return SHASH_REDUCE_DELETE;

			}
			else if (new_dst == pr->dest) {
				// Just wait for the next retransmit.
				cf_atomic_int_incr(&g_config.proxy_retry_same_dest);
				as_fabric_msg_put(pr->fab_msg);
			}
			else {
				// Not self, not same, redo. Don't need to return fab-msg-ref
				// because I'm just going to send again.
				cf_atomic_int_incr(&g_config.proxy_retry_new_dest);
				pr->dest = new_dst;
				goto Retry;
			}


		}
		else {
			as_fabric_msg_put(pr->fab_msg);
			cf_info(AS_PROXY, "retransmit: send failed, unknown error: dst %"PRIx64" rv %d", pr->dest, rv);
			return -2;
		}
	}

	return 0;
} // end proxy_retransmit_reduce_fn()


void *
proxy_retransmit_fn(void *unused)
{
	while (1) {
		usleep(75 * 1000);

		cf_detail(AS_PROXY, "proxy retransmit: size %d", shash_get_size(g_proxy_hash));

		now_times now;
		now.now_ns = cf_getns();
		now.now_ms = now.now_ns / 1000000;

		shash_reduce_delete(g_proxy_hash, proxy_retransmit_reduce_fn, (void *) &now);
	}

	return NULL;
} // end proxy_retransmit_fn()


// This function is called when there's a paxos change and the partitions have
// updated. If the change is a delete, then go through the retransmit structure
// and move forward any transactions that are destined to the newly departed
// node.
int
proxy_node_delete_reduce_fn(void *key, void *data, void *udata)
{
	proxy_request *pr = data;
	cf_node *node = (cf_node *)udata;

	if (pr->dest == *node) {
		pr->xmit_ms = 0;

		uint32_t	*tid = (uint32_t *) data;
		cf_debug(AS_PROXY, "node fail: speed proxy transaction tid %d", *tid);
	}

	return 0;
}


typedef struct as_proxy_paxos_change_struct_t {
	cf_node succession[AS_CLUSTER_SZ];
	cf_node deletions[AS_CLUSTER_SZ];
} as_proxy_paxos_change_struct;

// Discover nodes in the hash table that are no longer in the succession list.
int
proxy_node_succession_reduce_fn(void *key, void *data, void *udata)
{
	as_proxy_paxos_change_struct *del = (as_proxy_paxos_change_struct *)udata;
	proxy_request *pr = data;

	// Check if this dest is in the succession list.
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		if (pr->dest == del->succession[i]) {
			return 0;
		}
	}

	// dest is not in succession list - mark it to be deleted.
	for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
		// If an empty slot exists, then it means key is not there yet.
		if (del->deletions[i] == (cf_node)0)
		{
			del->deletions[i] = pr->dest;
			return 0;
		}
		// If dest already exists, return.
		if (pr->dest == del->deletions[i]) {
			return 0;
		}
	}

	// Should not get here.
	return 0;
}


void
as_proxy_paxos_change(as_paxos_generation gen, as_paxos_change *change, cf_node succession[], void *udata)
{
	if ((NULL == change) || (1 > change->n_change)) {
		return;
	}

	as_proxy_paxos_change_struct del;
	memset(&del, 0, sizeof(as_proxy_paxos_change_struct));
	memcpy(del.succession, succession, sizeof(cf_node)*g_config.paxos_max_cluster_size);

	// Find out if the request is sync.
	if (change->type[0] == AS_PAXOS_CHANGE_SYNC) {
		// Iterate through the proxy hash table and find nodes that are not in
		// the succession list.
		shash_reduce(g_proxy_hash, proxy_node_succession_reduce_fn, (void *) &del);

		// If there are any nodes to be deleted, execute the deletion algorithm.
		for (int i = 0; i < g_config.paxos_max_cluster_size; i++) {
			if ((cf_node)0 != del.deletions[i]) {
				cf_info(AS_PROXY, "notified: REMOVE node %"PRIx64"", del.deletions[i]);
				shash_reduce(g_proxy_hash, proxy_node_delete_reduce_fn, (void *) &del.deletions[i]);
			}
		}

		return;
	}

	// This is the deprecated case where this code is called at the end of a
	// paxos transaction commit.

	for (int i = 0; i < change->n_change; i++) {
		if (change->type[i] == AS_PAXOS_CHANGE_SUCCESSION_REMOVE) {
			cf_info(AS_PROXY, "notified: REMOVE node %"PRIx64, change->id[i]);
			shash_reduce(g_proxy_hash, proxy_node_delete_reduce_fn, (void *) & (change->id[i]));
		}
	}
}


uint32_t
as_proxy_inprogress()
{
	return shash_get_size(g_proxy_hash);
}


void
as_proxy_init()
{
	shash_create(&g_proxy_hash, proxy_id_hash, sizeof(uint32_t), sizeof(proxy_request), 4 * 1024, SHASH_CR_MT_MANYLOCK);

	pthread_create(&g_proxy_retransmit_th, 0, proxy_retransmit_fn, 0);

	as_fabric_register_msg_fn(M_TYPE_PROXY, proxy_mt, sizeof(proxy_mt), PROXY_MSG_SCRATCH_SIZE, proxy_msg_fn, NULL);

	as_paxos_register_change_callback(as_proxy_paxos_change, 0);
}
