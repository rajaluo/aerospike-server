/*
 * socket.h
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

#pragma once

#if defined USE_IPV6
#include "socket_ee.h"
#else
#include "socket_ce.h"
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <sys/socket.h>

#include "msg.h"
#include "util.h"

typedef struct {
	int32_t fd;
} __attribute__((packed)) cf_socket;

typedef struct {
	const char *addr;
	cf_ip_port port;
	bool reuse_addr;
	int32_t type;
	cf_socket sock;
} cf_socket_cfg;

// Accesses the socket file descriptor as an lvalue, i.e., the socket file descriptor
// can be modified.
#define SFD(sock) ((sock).fd)

// More restrictive version (think "const") of the above. Produces an rvalue, i.e.,
// the socket file descriptor cannot be modified.
#define CSFD(sock) ((int32_t)(sock).fd)

// Wraps a socket file descriptor.
#define WSFD(_fd) ((cf_socket){ .fd = _fd })

typedef struct {
	cf_socket_cfg conf;
	const char *if_addr;
	uint8_t ttl;
} cf_socket_mcast_cfg;

// XXX - Cleanly share the following with hb.c and fabric.c.
#define AS_HB_MSG_ADDR 3
#define AS_HB_MSG_PORT 4
#define AS_HB_MSG_ADDR_EX 7

#define FS_ADDR 1
#define FS_PORT 2
#define FS_ADDR_EX 4

int32_t cf_ip_addr_from_string(const char *string, cf_ip_addr *addr);
int32_t cf_ip_addr_to_string(const cf_ip_addr *addr, char *string, size_t size);
int32_t cf_ip_addr_from_binary(const uint8_t *binary, size_t size, cf_ip_addr *addr);
int32_t cf_ip_addr_to_binary(const cf_ip_addr *addr, uint8_t *binary, size_t size);
int32_t cf_ip_addr_compare(const cf_ip_addr *lhs, const cf_ip_addr *rhs);
void cf_ip_addr_copy(const cf_ip_addr *from, cf_ip_addr *to);
bool cf_ip_addr_is_loopback(const cf_ip_addr *addr);

int32_t cf_ip_port_from_string(const char *string, cf_ip_port *port);
int32_t cf_ip_port_to_string(cf_ip_port port, char *string, size_t size);
int32_t cf_ip_port_from_binary(const uint8_t *binary, size_t size, cf_ip_port *port);
int32_t cf_ip_port_to_binary(cf_ip_port port, uint8_t *binary, size_t size);
void cf_ip_port_from_node_id(cf_node id, cf_ip_port *port);

int32_t cf_sock_addr_from_string(const char *string, cf_sock_addr *addr);
int32_t cf_sock_addr_to_string(const cf_sock_addr *addr, char *string, size_t size);
int32_t cf_sock_addr_from_binary(const uint8_t *binary, size_t size, cf_sock_addr *addr);
int32_t cf_sock_addr_to_binary(const cf_sock_addr *addr, uint8_t *binary, size_t size);

int32_t cf_sock_addr_from_host_port(const char *host, cf_ip_port port, cf_sock_addr *addr);
void cf_sock_addr_from_addr_port(const cf_ip_addr *ip_addr, cf_ip_port port, cf_sock_addr *addr);

int32_t cf_sock_addr_from_heartbeat(const msg *msg, cf_sock_addr *addr);
void cf_sock_addr_to_heartbeat(cf_sock_addr *addr, msg *msg);
int32_t cf_sock_addr_from_fabric(const msg *msg, cf_sock_addr *addr);
void cf_sock_addr_to_fabric(cf_sock_addr *addr, msg *msg);

int32_t cf_sock_addr_compare(const cf_sock_addr *lhs, const cf_sock_addr *rhs);
void cf_sock_addr_copy(const cf_sock_addr *from, cf_sock_addr *to);

void cf_sock_addr_from_native(struct sockaddr *native, cf_sock_addr *addr);
void cf_sock_addr_to_native(cf_sock_addr *addr, struct sockaddr *native);

void cf_socket_disable_blocking(cf_socket sock);
void cf_socket_enable_blocking(cf_socket sock);
void cf_socket_disable_nagle(cf_socket sock);
void cf_socket_enable_nagle(cf_socket sock);
void cf_socket_keep_alive(cf_socket sock, int32_t idle, int32_t interval, int32_t count);
void cf_socket_set_send_buffer(cf_socket sock, int32_t size);
void cf_socket_set_receive_buffer(cf_socket sock, int32_t size);
void cf_socket_set_window(cf_socket sock, int32_t size);

int32_t cf_socket_init_server(cf_socket_cfg *conf);
int32_t cf_socket_init_client(cf_socket_cfg *conf, int32_t timeout);
int32_t cf_socket_init_client_nb(cf_sock_addr *addr, cf_socket *sock);

int32_t cf_socket_accept(cf_socket lsock, cf_socket *sock, cf_sock_addr *addr);
int32_t cf_socket_remote_name(cf_socket sock, cf_sock_addr *addr);
int32_t cf_socket_local_name(cf_socket sock, cf_sock_addr *addr);
int32_t cf_socket_available(cf_socket sock);

int32_t cf_socket_recv_from(cf_socket sock, void *buff, size_t size, int32_t flags, cf_sock_addr *addr);
int32_t cf_socket_recv(cf_socket sock, void *buff, size_t size, int32_t flags);
int32_t cf_socket_send_to(cf_socket sock, void *buff, size_t size, int32_t flags, cf_sock_addr *addr);
int32_t cf_socket_send(cf_socket sock, void *buff, size_t size, int32_t flags);

void cf_socket_write_shutdown(cf_socket sock);
void cf_socket_shutdown(cf_socket sock);
void cf_socket_close(cf_socket sock);
void cf_socket_drain_close(cf_socket sock);

int32_t cf_socket_mcast_init(cf_socket_mcast_cfg *mconf);
int32_t cf_socket_mcast_set_inter(cf_socket sock, const cf_ip_addr *iaddr);
int32_t cf_socket_mcast_join_group(cf_socket sock, const cf_ip_addr *iaddr, const cf_ip_addr *gaddr);
void cf_socket_mcast_close(cf_socket_mcast_cfg *mconf);

int32_t cf_inter_get_addr(cf_ip_addr **addrs, int32_t *n_addrs, uint8_t *buff, size_t size);
int32_t cf_inter_get_addr_ex(cf_ip_addr **addrs, int32_t *n_addrs, uint8_t *buff, size_t size);

int32_t cf_node_id_get(cf_ip_port port, const char *if_hint, cf_node *id, char **ip_addr);

#if defined CF_SOCKET_PRIVATE
size_t cf_socket_addr_len(const struct sockaddr *sa);
int32_t cf_socket_parse_netlink(bool allow_v6, uint32_t family, uint32_t flags,
		void *data, size_t len, cf_ip_addr *addr);
#endif
