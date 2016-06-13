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

typedef struct {
	int32_t fd;
} __attribute__((packed)) cf_socket;

typedef uint64_t cf_sock_addr_legacy;

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

int32_t cf_ip_addr_from_string(const char *string, cf_ip_addr *addr);
int32_t cf_ip_addr_to_string(const cf_ip_addr *addr, char *string, size_t size);
int32_t cf_ip_addr_from_binary(const uint8_t *binary, cf_ip_addr *addr, size_t size);
int32_t cf_ip_addr_to_binary(const cf_ip_addr *addr, uint8_t *binary, size_t size);

int32_t cf_ip_port_from_string(const char *string, cf_ip_port *port);
int32_t cf_ip_port_to_string(cf_ip_port port, char *string, size_t size);
int32_t cf_ip_port_from_binary(const uint8_t *binary, cf_ip_port *port, size_t size);
int32_t cf_ip_port_to_binary(cf_ip_port port, uint8_t *binary, size_t size);

int32_t cf_sock_addr_from_host_port(const char *host, cf_ip_port port, cf_sock_addr *addr);
int32_t cf_sock_addr_from_string(const char *string, cf_sock_addr *addr);
int32_t cf_sock_addr_to_string(const cf_sock_addr *addr, char *string, size_t size);
int32_t cf_sock_addr_from_binary(const uint8_t *binary, cf_sock_addr *addr, size_t size);
int32_t cf_sock_addr_to_binary(const cf_sock_addr *addr, uint8_t *binary, size_t size);

void cf_sock_addr_from_binary_legacy(const cf_sock_addr_legacy *sal, cf_sock_addr *addr);
void cf_sock_addr_to_binary_legacy(const cf_sock_addr *addr, cf_sock_addr_legacy *sal);
void cf_sock_addr_legacy_set_port(cf_sock_addr_legacy *legacy, cf_ip_port port);

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

void cf_socket_shutdown(cf_socket sock);
void cf_socket_close(cf_socket sock);
void cf_socket_drain_close(cf_socket sock);

// -------------------- OLD CODE --------------------

/* cf_mcastsocket_cfg
 * A multicast socket */
typedef struct cf_mcastsocket_cfg_t {
	cf_socket_cfg s;
	struct ip_mreq ireq;
    char *tx_addr;    // if there is a specific ip address that should be used to send the mcast message
    unsigned char mcast_ttl;
} cf_mcastsocket_cfg;

/* Function declarations */
extern int cf_mcastsocket_init(cf_mcastsocket_cfg *ms);
extern void cf_mcastsocket_close(cf_mcastsocket_cfg *ms);

/*
** get information about all interfaces
** currently returns ipv4 only - but does return loopback
**
** example:
**
** uint8_t buf[512];
** cf_ifaddr *ifaddr;
** int        ifaddr_sz;
** cf_ifaddr_get(&ifaddr, &ifaddr_sz, buf, sizeof(buf));
**
*/

typedef struct cf_ifaddr_s {
	uint32_t		flags;
	unsigned short	family;
	struct sockaddr sa;
} cf_ifaddr;

extern int cf_ifaddr_get(cf_ifaddr **ifaddr, int *ifaddr_sz, uint8_t *buf, size_t buf_sz);
