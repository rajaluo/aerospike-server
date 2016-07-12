/*
 * socket_ce.h
 *
 * Copyright (C) 2016 Aerospike, Inc.
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

#include "socket.h"

#include <errno.h>
#include <netdb.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "fault.h"

#include "citrusleaf/alloc.h"

static char *
safe_strndup(const char *string, size_t length)
{
	char *res = cf_strndup(string, length);

	if (res == NULL) {
		cf_crash(CF_SOCKET, "Out of memory");
	}

	return res;
}

int32_t
cf_ip_addr_from_string(const char *string, cf_ip_addr *addr)
{
	struct addrinfo *info;
	static struct addrinfo hints = {
		.ai_flags = 0,
		.ai_family = AF_INET
	};

	int32_t res = getaddrinfo(string, NULL, &hints, &info);

	if (res != 0) {
		cf_warning(CF_SOCKET, "Error while converting address '%s': %s", string, gai_strerror(res));
		return -1;
	}

	struct sockaddr_in *sai = (struct sockaddr_in *)info->ai_addr;
	*addr = sai->sin_addr;

	freeaddrinfo(info);
	return 0;
}

int32_t
cf_ip_addr_to_string(const cf_ip_addr *addr, char *string, size_t size)
{
	if (inet_ntop(AF_INET, &addr->s_addr, string, size) == NULL) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	return strlen(string);
}

int32_t
cf_ip_addr_from_binary(const uint8_t *binary, size_t size, cf_ip_addr *addr)
{
	if (size < 16) {
		cf_warning(CF_SOCKET, "Input buffer underflow");
		return -1;
	}

	for (int32_t i = 0; i < 10; ++i) {
		if (binary[i] != 0) {
			cf_warning(CF_SOCKET, "Invalid binary IPv4 address");
			return -1;
		}
	}

	if (binary[10] != 0xff || binary[11] != 0xff) {
		cf_warning(CF_SOCKET, "Invalid binary IPv4 address");
		return -1;
	}

	memcpy(&addr->s_addr, binary + 12, 4);
	return 16;
}

int32_t
cf_ip_addr_to_binary(const cf_ip_addr *addr, uint8_t *binary, size_t size)
{
	if (size < 16) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	for (int32_t i = 0; i < 10; ++i) {
		binary[i] = 0;
	}

	binary[10] = binary[11] = 0xff;
	memcpy(binary + 12, &addr->s_addr, 4);
	return 16;
}

int32_t
cf_ip_addr_compare(const cf_ip_addr *lhs, const cf_ip_addr *rhs)
{
	return memcmp(&lhs->s_addr, &rhs->s_addr, 4);
}

void
cf_ip_addr_copy(const cf_ip_addr *from, cf_ip_addr *to)
{
	to->s_addr = from->s_addr;
}

bool
cf_ip_addr_is_loopback(const cf_ip_addr *addr)
{
	return (ntohl(addr->s_addr) & 0xff000000) == 0x7f000000;
}

int32_t
cf_sock_addr_from_string(const char *string, cf_sock_addr *addr)
{
	int32_t res = -1;
	const char *colon = strchr(string, ':');

	if (colon == NULL) {
		cf_warning(CF_SOCKET, "Missing ':' in socket address '%s'", string);
		goto cleanup0;
	}

	const char *host = safe_strndup(string, colon - string);

	if (cf_ip_addr_from_string(host, &addr->addr) < 0) {
		cf_warning(CF_SOCKET, "Invalid host address '%s' in socket address '%s'", host, string);
		goto cleanup1;
	}

	if (cf_ip_port_from_string(colon + 1, &addr->port) < 0) {
		cf_warning(CF_SOCKET, "Invalid port '%s' in socket address '%s'", colon + 1, string);
		goto cleanup1;
	}

	res = 0;

cleanup1:
	cf_free((void *)host);

cleanup0:
	return res;
}

static int32_t
from_x(const msg *msg, cf_sock_addr *addr, int32_t addr_id, int32_t port_id)
{
	int32_t res = 0;
	uint32_t v4;

	if (msg_get_uint32(msg, addr_id, &v4) < 0) {
		cf_crash(CF_SOCKET, "Heartbeat message without IPv4 address field (AS_HB_MSG_ADDR)");
	}

	if (v4 != 0) {
		++res;
	}

	uint32_t port;

	if (msg_get_uint32(msg, port_id, &port) < 0) {
		cf_crash(CF_SOCKET, "Heartbeat message without port field (AS_HB_MSG_PORT)");
	}

	if (port != 0) {
		++res;
	}

	addr->addr.s_addr = v4;
	addr->port = (cf_ip_port)port;
	return res;
}

int32_t
cf_sock_addr_from_heartbeat(const msg *msg, cf_sock_addr *addr)
{
	return from_x(msg, addr, AS_HB_MSG_ADDR, AS_HB_MSG_PORT);
}

int32_t
cf_sock_addr_from_fabric(const msg *msg, cf_sock_addr *addr)
{
	return from_x(msg, addr, FS_ADDR, FS_PORT);
}

static void
to_x(cf_sock_addr *addr, msg *msg, int32_t addr_id, int32_t port_id)
{
	uint32_t v4 = addr->addr.s_addr;

	if (msg_set_uint32(msg, addr_id, v4)) {
		cf_crash(CF_SOCKET, "Error while adding IPv4 address field (AS_HB_MSG_ADDR) to heartbeat.");
	}

	uint32_t port = addr->port;

	if (msg_set_uint32(msg, port_id, port)) {
		cf_crash(CF_SOCKET, "Error while adding port field (AS_HB_MSG_PORT) to heartbeat.");
	}
}

void cf_sock_addr_to_heartbeat(cf_sock_addr *addr, msg *msg)
{
	to_x(addr, msg, AS_HB_MSG_ADDR, AS_HB_MSG_PORT);
}

void cf_sock_addr_to_fabric(cf_sock_addr *addr, msg *msg)
{
	to_x(addr, msg, FS_ADDR, FS_PORT);
}

void
cf_sock_addr_from_native(struct sockaddr *native, cf_sock_addr *addr)
{
	struct sockaddr_in *sai = (struct sockaddr_in *)native;

	if (sai->sin_family != AF_INET) {
		cf_crash(CF_SOCKET, "Invalid address family: %d", sai->sin_family);
	}

	addr->addr = sai->sin_addr;
	addr->port = ntohs(sai->sin_port);
}

void
cf_sock_addr_to_native(cf_sock_addr *addr, struct sockaddr *native)
{
	struct sockaddr_in *sai = (struct sockaddr_in *)native;
	memset(sai, 0, sizeof(struct sockaddr_in));
	sai->sin_family = AF_INET;
	sai->sin_addr = addr->addr;
	sai->sin_port = htons(addr->port);
}

int32_t
cf_socket_mcast_set_inter(cf_socket sock, const cf_ip_addr *iaddr)
{
	struct ip_mreqn mr;
	memset(&mr, 0, sizeof(mr));
	mr.imr_address = *iaddr;

	if (setsockopt(sock.fd, IPPROTO_IP, IP_MULTICAST_IF, &mr, sizeof(mr)) < 0) {
		cf_warning(CF_SOCKET, "setsockopt(IP_MULTICAST_IF) failed on FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
		return -1;
	}

	return 0;
}

int32_t
cf_socket_mcast_join_group(cf_socket sock, const cf_ip_addr *iaddr, const cf_ip_addr *gaddr)
{
	struct ip_mreqn mr;
	memset(&mr, 0, sizeof(mr));

	if (iaddr != NULL) {
		mr.imr_address = *iaddr;
	}

	mr.imr_multiaddr = *gaddr;

	if (setsockopt(sock.fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mr, sizeof(mr)) < 0) {
		cf_warning(CF_SOCKET, "setsockopt(IP_ADD_MEMBERSHIP) failed on FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
		return -1;
	}

	return 0;
}

size_t
cf_socket_addr_len(const struct sockaddr *sa)
{
	switch (sa->sa_family) {
	case AF_INET:
		return sizeof(struct sockaddr_in);

	default:
		cf_crash(CF_SOCKET, "Invalid address family: %d", sa->sa_family);
		// XXX - Find a way to mark cf_crash() as "noreturn".
		return 0;
	}
}

int32_t
cf_socket_parse_netlink(bool allow_ipv6, uint32_t family, uint32_t flags,
		void *data, size_t len, cf_ip_addr *addr)
{
	(void)allow_ipv6;
	(void)flags;

	if (family != AF_INET || len != 4) {
		return -1;
	}

	memcpy(&addr->s_addr, data, 4);
	return 0;
}
