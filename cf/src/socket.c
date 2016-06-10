/*
 * socket.c
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

#include "socket.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "fault.h"

int32_t
cf_ip_port_from_string(const char *string, cf_ip_port *port)
{
	char *end;
	uint64_t tmp = strtoul(string, &end, 10);

	if (*end != 0 || tmp > 65535) {
		cf_warning(CF_SOCKET, "Invalid port '%s'", string);
		return -1;
	}

	*port = (cf_ip_port)tmp;
	return 0;
}

int32_t
cf_ip_port_to_string(cf_ip_port port, char *string, size_t size)
{
	int32_t count = snprintf(string, size, "%hu", port);

	if ((size_t)count >= size) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	return count;
}

int32_t
cf_ip_port_from_binary(const uint8_t *binary, cf_ip_port *port, size_t size)
{
	if (size < 2) {
		cf_warning(CF_SOCKET, "Input buffer underflow");
		return -1;
	}

	*port = (binary[0] << 8) | binary[1];
	return 2;
}

int32_t
cf_ip_port_to_binary(cf_ip_port port, uint8_t *binary, size_t size)
{
	if (size < 2) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	binary[0] = port >> 8;
	binary[1] = port & 255;
	return 2;
}

int32_t
cf_sock_addr_from_host_port(const char *host, cf_ip_port port, cf_sock_addr *addr)
{
	if (cf_ip_addr_from_string(host, &addr->addr) < 0) {
		cf_warning(CF_SOCKET, "Invalid host address '%s'", host);
		return -1;
	}

	addr->port = port;
	return 0;
}

int32_t
cf_sock_addr_to_string(const cf_sock_addr *addr, char *string, size_t size)
{
	int32_t total = 0;
	int32_t count = cf_ip_addr_to_string(&addr->addr, string, size);

	if (count < 0) {
		return -1;
	}

	total += count;

	if (size - total < 2) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	string[total++] = ':';
	string[total] = 0;

	count = cf_ip_port_to_string(addr->port, string + total, size - total);

	if (count < 0) {
		return -1;
	}

	total += count;
	return total;
}

int32_t
cf_sock_addr_from_binary(const uint8_t *binary, cf_sock_addr *addr, size_t size)
{
	int32_t total = 0;
	int32_t count = cf_ip_addr_from_binary(binary, &addr->addr, size);

	if (count < 0) {
		return -1;
	}

	total += count;
	count = cf_ip_port_from_binary(binary + total, &addr->port, size - total);

	if (count < 0) {
		return -1;
	}

	total += count;
	return total;
}

int32_t
cf_sock_addr_to_binary(const cf_sock_addr *addr, uint8_t *binary, size_t size)
{
	int32_t total = 0;
	int32_t count = cf_ip_addr_to_binary(&addr->addr, binary, size);

	if (count < 0) {
		return -1;
	}

	total += count;
	count = cf_ip_port_to_binary(addr->port, binary + total, size - total);

	if (count < 0) {
		return -1;
	}

	total += count;
	return total;
}

void
cf_sock_addr_legacy_set_port(cf_sock_addr_legacy *legacy, cf_ip_port port)
{
	uint8_t *binary = (uint8_t *)legacy;
	uint16_t net_port = htons(port);
	memcpy(binary + 4, &net_port, 2);
}

static int32_t
safe_fcntl(int32_t fd, int32_t cmd, int32_t arg)
{
	int32_t res = fcntl(fd, cmd, arg);

	if (res < 0) {
		cf_crash(CF_SOCKET, "fcntl() failed on FD %d: %d (%s)", fd, errno, cf_strerror(errno));
	}

	return res;
}

static void
safe_setsockopt(int32_t fd, int32_t level, int32_t name, void *val, socklen_t len)
{
	if (setsockopt(fd, level, name, val, len) < 0) {
		cf_crash(CF_SOCKET, "setsockopt() failed on FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}
}

static void
safe_getsockopt(int32_t fd, int32_t level, int32_t name, void *val, socklen_t *len)
{
	if (getsockopt(fd, level, name, val, len) < 0) {
		cf_crash(CF_SOCKET, "getsockopt() failed on FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}
}

static int32_t
safe_wait(int32_t efd, struct epoll_event *events, int32_t max, int32_t timeout)
{
	while (true) {
		cf_debug(CF_SOCKET, "Waiting on epoll FD %d", efd);
		int32_t count = epoll_wait(efd, events, max, timeout);

		if (count < 0) {
			if (errno == EINTR) {
				cf_debug(CF_SOCKET, "Interrupted");
				continue;
			}

			cf_crash(CF_SOCKET, "epoll_wait() failed on epoll FD %d: %d (%s)",
					efd, errno, cf_strerror(errno));
		}

		return count;
	}
}

static void
safe_close(int32_t fd)
{
	if (close(fd) < 0) {
		cf_crash(CF_SOCKET, "Error while closing FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}
}

void
cf_socket_disable_blocking(int32_t fd)
{
	int32_t flags = safe_fcntl(fd, F_GETFL, 0);
	safe_fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void
cf_socket_enable_blocking(int32_t fd)
{
	int32_t flags = safe_fcntl(fd, F_GETFL, 0);
	safe_fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

void
cf_socket_disable_nagle(int32_t fd)
{
	static int32_t flag = 1;
	safe_setsockopt(fd, SOL_TCP, TCP_NODELAY, &flag, sizeof flag);
}

void
cf_socket_enable_nagle(int32_t fd)
{
	static int32_t flag = 0;
	safe_setsockopt(fd, SOL_TCP, TCP_NODELAY, &flag, sizeof flag);
}

static size_t
addr_len(const struct sockaddr *sa)
{
	switch (sa->sa_family) {
	case AF_INET:
		return sizeof (struct sockaddr_in);

	case AF_INET6:
		return sizeof (struct sockaddr_in6);

	default:
		cf_crash(CF_SOCKET, "Invalid address family: %d", sa->sa_family);
		// XXX - Find a way to mark cf_crash() as "noreturn".
		return 0;
	}
}

static int32_t
config_to_native(const cf_socket_cfg *conf, struct sockaddr *sa)
{
	cf_sock_addr addr;

	if (conf->addr == NULL) {
		cf_warning(CF_SOCKET, "Missing service address");
		return -1;
	}

	if (cf_ip_addr_from_string(conf->addr, &addr.addr) < 0) {
		cf_warning(CF_SOCKET, "Invalid service address: %s", conf->addr);
		return -1;
	}

	if (conf->port == 0) {
		cf_warning(CF_SOCKET, "Missing service port");
		return -1;
	}

	addr.port = conf->port;
	cf_sock_addr_to_native(&addr, sa);
	return 0;
}

int32_t
cf_socket_init_server(cf_socket_cfg *conf)
{
	int32_t res = -1;
	struct sockaddr_storage sas;

	if (config_to_native(conf, (struct sockaddr *)&sas) < 0) {
		goto cleanup0;
	}

	int32_t fd = socket(sas.ss_family, conf->type, 0);

	if (fd < 0) {
		cf_warning(CF_SOCKET, "Error while creating socket for %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup0;
	}

	if (conf->reuse_addr) {
		static int32_t flag = 1;
		safe_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof flag);
	}

	// XXX - Why are we doing this?
	safe_fcntl(fd, F_SETFD, FD_CLOEXEC);

	while (bind(fd, (struct sockaddr *)&sas, addr_len((struct sockaddr *)&sas)) < 0) {
		if (errno != EADDRINUSE) {
			cf_warning(CF_SOCKET, "Error while binding to %s:%d: %d (%s)",
					conf->addr, conf->port, errno, cf_strerror(errno));
			goto cleanup1;
		}

		cf_warning(CF_SOCKET, "Socket %s:%d in use, waiting", conf->addr, conf->port);
		usleep(5 * 1000);
	}

	if (conf->type == SOCK_STREAM && listen(fd, 512) < 0) {
		cf_warning(CF_SOCKET, "Error while listening on %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup1;
	}

	// No Nagle here. It will be disabled for the accepted connections.

	conf->sock = fd;
	res = 0;
	goto cleanup0;

cleanup1:
	safe_close(fd);

cleanup0:
	return res;
}

static int32_t
connect_socket(int32_t fd, struct sockaddr *sa, int32_t timeout)
{
	cf_debug(CF_SOCKET, "Connecting FD %d", fd);
	int32_t res = -1;

	cf_socket_disable_blocking(fd);
	int32_t rv = connect(fd, sa, addr_len(sa));

	if (rv == 0) {
		cf_debug(CF_SOCKET, "FD %d connected [1]", fd);
		res = 0;
		goto cleanup1;
	}

	if (errno != EINPROGRESS) {
		cf_warning(CF_SOCKET, "Error while connecting FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
		goto cleanup1;
	}

	int32_t efd = epoll_create(1);

	if (efd < 0) {
		cf_crash(CF_SOCKET, "epoll_create() failed: %d (%s)", errno, cf_strerror(errno));
	}

	struct epoll_event event = { .data.fd = fd, .events = EPOLLOUT };

	if (epoll_ctl(efd, EPOLL_CTL_ADD, fd, &event) < 0) {
		cf_crash(CF_SOCKET, "epoll_ctl() failed for FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}

	int32_t count = safe_wait(efd, &event, 1, timeout);

	if (count == 0) {
		cf_warning(CF_SOCKET, "Timeout while connecting FD %d", fd);
		goto cleanup2;
	}

	int32_t err;
	socklen_t err_len = sizeof err;
	safe_getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len);

	if (err != 0) {
		cf_warning(CF_SOCKET, "Error while connecting FD %d: %d (%s)",
				fd, err, cf_strerror(err));
		goto cleanup2;
	}

	cf_debug(CF_SOCKET, "FD %d connected [2]", fd);
	res = 0;

cleanup2:
	if (epoll_ctl(efd, EPOLL_CTL_DEL, fd, NULL) < 0) {
		cf_crash(CF_SOCKET, "epoll_ctl() failed for FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}

	safe_close(efd);

cleanup1:
	cf_socket_enable_blocking(fd);
	return res;
}

int32_t
cf_socket_init_client(cf_socket_cfg *conf, int32_t timeout)
{
	int32_t res = -1;
	struct sockaddr_storage sas;

	if (config_to_native(conf, (struct sockaddr *)&sas) < 0) {
		goto cleanup0;
	}

	int32_t fd = socket(sas.ss_family, conf->type, 0);

	if (fd < 0) {
		cf_warning(CF_SOCKET, "Error while creating socket for %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup0;
	}

	// XXX - Why are we doing this?
	safe_fcntl(fd, F_SETFD, FD_CLOEXEC);

	if (connect_socket(fd, (struct sockaddr *)&sas, timeout) < 0) {
		cf_warning(CF_SOCKET, "Error while connecting socket to %s:%d",
				conf->addr, conf->port);
		goto cleanup1;
	}

	cf_socket_disable_nagle(fd);

	conf->sock = fd;
	res = 0;
	goto cleanup0;

cleanup1:
	safe_close(fd);

cleanup0:
	return res;
}

int32_t
cf_socket_init_client_nb(cf_sock_addr *addr)
{
	int32_t res = -1;

	struct sockaddr_storage sas;
	cf_sock_addr_to_native(addr, (struct sockaddr *)&sas);

	char friendly[1000];
	cf_sock_addr_to_string(addr, friendly, sizeof friendly);

	int32_t fd = socket(sas.ss_family, SOCK_STREAM, 0);

	if (fd < 0) {
		cf_warning(CF_SOCKET, "Error while creating socket for %s: %d (%s)",
				friendly, errno, cf_strerror(errno));
		goto cleanup0;
	}

	// XXX - Why are we doing this?
	safe_fcntl(fd, F_SETFD, FD_CLOEXEC);

	cf_socket_disable_blocking(fd);

	if (connect(fd, (struct sockaddr *)&sas, addr_len((struct sockaddr *)&sas)) < 0) {
		if (errno != EINPROGRESS) {
			cf_warning(CF_SOCKET, "Error while connecting socket to %s: %d (%s)",
					friendly, errno, cf_strerror(errno));
			goto cleanup1;
		}
	}

	res = fd;
	goto cleanup0;

cleanup1:
	safe_close(fd);

cleanup0:
	return res;
}

int32_t
cf_socket_peer_name(int32_t fd, cf_sock_addr *addr)
{
	struct sockaddr_storage sas;
	socklen_t sas_len = sizeof sas;

	if (getpeername(fd, (struct sockaddr *)&sas, &sas_len) < 0) {
		cf_warning(CF_SOCKET, "Error while getting peer name: %d (%s)", errno, cf_strerror(errno));
		return -1;
	}

	cf_sock_addr_from_native((struct sockaddr *)&sas, addr);
	return 0;
}

int32_t
cf_socket_send_to(int32_t fd, void *buff, size_t size, int32_t flags, cf_sock_addr *addr)
{
	struct sockaddr_storage sas;
	struct sockaddr *sa = NULL;
	socklen_t sa_len = 0;

	if (addr != NULL) {
		cf_sock_addr_to_native(addr, (struct sockaddr *)&sas);
		sa = (struct sockaddr *)&sas;
		sa_len = addr_len((struct sockaddr *)&sas);
	}

	int32_t res = sendto(fd, buff, size, flags | MSG_NOSIGNAL, sa, sa_len);

	if (res < 0) {
		cf_debug(CF_SOCKET, "Error while sending on FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}

	return res;
}

int32_t
cf_socket_send(int32_t fd, void *buff, size_t size, int32_t flags)
{
	return cf_socket_send_to(fd, buff, size, flags, NULL);
}

int32_t
cf_socket_recv_from(int32_t fd, void *buff, size_t size, int32_t flags, cf_sock_addr *addr)
{
	struct sockaddr_storage sas;
	struct sockaddr *sa = NULL;
	socklen_t sa_len = 0;

	if (addr != NULL) {
		sa = (struct sockaddr *)&sas;
		sa_len = sizeof sas;
	}

	int32_t res = recvfrom(fd, buff, size, flags | MSG_NOSIGNAL, sa, &sa_len);

	if (res < 0) {
		cf_debug(CF_SOCKET, "Error while receiving on FD %d: %d (%s)",
				fd, errno, cf_strerror(errno));
	}
	else if (addr != NULL) {
		cf_sock_addr_from_native(sa, addr);
	}

	return res;
}

int32_t
cf_socket_recv(int32_t fd, void *buff, size_t size, int32_t flags)
{
	return cf_socket_recv_from(fd, buff, size, flags, NULL);
}

void
cf_socket_shutdown(int32_t fd)
{
	cf_debug(CF_SOCKET, "Shutting down FD %d", fd);

	if (shutdown(fd, SHUT_RDWR) < 0) {
		if (errno != ENOTCONN) {
			cf_crash(CF_SOCKET, "shutdown() failed on FD %d: %d (%s)",
					fd, errno, cf_strerror(errno));
		}
		else {
			cf_warning(CF_SOCKET, "shutdown() on disconnected FD %d: %d (%s)",
					fd, errno, cf_strerror(errno));
		}
	}
}

void
cf_socket_close(int32_t fd)
{
	cf_debug(CF_SOCKET, "Closing FD %d", fd);
	safe_close(fd);
}

void
cf_socket_drain_close(cf_socket sock)
{
	cf_debug(CF_SOCKET, "Draining and closing FD %d", sock.fd);
	int32_t efd = epoll_create(1);

	if (efd < 0) {
		cf_crash(CF_SOCKET, "epoll_create() failed: %d (%s)", errno, cf_strerror(errno));
	}

	struct epoll_event event = { .data.fd = sock.fd, .events = EPOLLRDHUP };

	if (epoll_ctl(efd, EPOLL_CTL_ADD, sock.fd, &event) < 0) {
		cf_crash(CF_SOCKET, "epoll_ctl() failed for FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
	}

	cf_socket_shutdown(sock.fd);
	int32_t count = safe_wait(efd, &event, 1, 5000);

	if (count == 0) {
		cf_warning(CF_SOCKET, "Timeout while waiting for FD %d to drain", sock.fd);
		goto cleanup1;
	}

	cf_debug(CF_SOCKET, "FD %d drained", sock.fd);

cleanup1:
	if (epoll_ctl(efd, EPOLL_CTL_DEL, sock.fd, NULL) < 0) {
		cf_crash(CF_SOCKET, "epoll_ctl() failed for FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
	}

	safe_close(efd);
	cf_socket_close(sock.fd);
}

// -------------------- OLD CODE --------------------

#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_types.h>

#include "fault.h"

/* cf_svcmsocket_init
 * Initialize a multicast service/receive socket
 * Bind is done to INADDR_ANY - all interfaces
 *  */
int
cf_mcastsocket_init(cf_mcastsocket_cfg *ms)
{
	cf_assert(ms, CF_SOCKET, CF_CRITICAL, "invalid argument");
	cf_socket_cfg *s = &(ms->s);

	if (!s->addr) {
		cf_warning(CF_SOCKET, "multicast socket address not configured");
		return -1;
	}

	if (!s->port) {
		cf_warning(CF_SOCKET, "multicast socket port not configured");
		return -1;
	}

	if (0 > (s->sock = socket(AF_INET, SOCK_DGRAM, 0))) {
		cf_warning(CF_SOCKET, "multicast socket open error: %d %s", errno, cf_strerror(errno));
		return -1;
	}

	cf_debug(CF_SOCKET, "cf_mcastsocket_init: socket %d", s->sock);

	// allows multiple readers on the same address
	uint yes=1;
 	if (setsockopt(s->sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
		cf_warning(CF_SOCKET, "multicast socket reuse failed: %d %s", errno, cf_strerror(errno));
		goto err_cleanup;
	}

	/* Set close-on-exec */
	fcntl(s->sock, F_SETFD, 1);

	// Bind to the incoming port on the specified mcast IP address.
	memset(&s->saddr, 0, sizeof(s->saddr));
	s->saddr.sin_family = AF_INET;

	if (!inet_pton(AF_INET, s->addr, &s->saddr.sin_addr)) {
		cf_warning(CF_SOCKET, "multicast socket inet_pton(%s) failed: %d %s", s->addr, errno, cf_strerror(errno));
		goto err_cleanup;
	}

#ifdef IP_MULTICAST_ALL
	// [FYI:  This socket option has existed since the Linux 2.6.31 kernel.]

	// Only receive traffic from multicast groups this socket actually joins.
	// [Note:  Bind address filtering takes precedence, so this is simply an extra level of restriction.]
	uint no = 0;
	if (setsockopt(s->sock, IPPROTO_IP, IP_MULTICAST_ALL, &no, sizeof(no)) == -1) {
		cf_warning(CF_SOCKET, "IP_MULTICAST_ALL: %d %s", errno, cf_strerror(errno));
		goto err_cleanup;
	}
#endif

	s->saddr.sin_port = htons(s->port);
	if (ms->tx_addr) {
		struct in_addr iface_in;
		memset((char *)&iface_in,0,sizeof(iface_in));
		iface_in.s_addr = inet_addr(ms->tx_addr);

		if (setsockopt(s->sock, IPPROTO_IP, IP_MULTICAST_IF, (const char*)&iface_in, sizeof(iface_in)) == -1) {
			cf_warning(CF_SOCKET, "IP_MULTICAST_IF: %d %s", errno, cf_strerror(errno));
			goto err_cleanup;
		}
	}
	unsigned char ttlvar = ms->mcast_ttl;
	if (ttlvar>0) {
		if (setsockopt(s->sock,IPPROTO_IP,IP_MULTICAST_TTL,(char *)&ttlvar, sizeof(ttlvar)) == -1) {
			cf_warning(CF_SOCKET, "IP_MULTICAST_TTL: %d %s", errno, cf_strerror(errno));
			goto err_cleanup;
		} else {
			cf_info(CF_SOCKET, "setting multicast TTL to be %d",ttlvar);
		}
	}

	struct timespec delay;
	delay.tv_sec = 5;
	delay.tv_nsec = 0;

	while (0 > (bind(s->sock, (struct sockaddr *)&s->saddr, sizeof(struct sockaddr)))) {
		if (EADDRINUSE != errno) {
			cf_warning(CF_SOCKET, "multicast bind: %d %s", errno, cf_strerror(errno));
			goto err_cleanup;
		}

		cf_warning(CF_SOCKET, "multicast bind: socket in use, waiting (port:%d)", s->port);

		nanosleep(&delay, NULL);
	}

	// Register for the multicast group
	inet_pton(AF_INET, s->addr, &ms->ireq.imr_multiaddr.s_addr);
	ms->ireq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (ms->tx_addr) {
		ms->ireq.imr_interface.s_addr = inet_addr(ms->tx_addr);
	}
	if (setsockopt(s->sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, (const void *)&ms->ireq, sizeof(struct ip_mreq))) {
		cf_warning(CF_SOCKET, "IP_ADD_MEMBERSHIP: %d %s", errno, cf_strerror(errno));
		goto err_cleanup;
	}

	return 0;

 err_cleanup:

	close(s->sock);
	s->sock = -1;

	return -1;
}

void
cf_mcastsocket_close(cf_mcastsocket_cfg *ms)
{
	cf_socket_cfg *s = &(ms->s);

	safe_close(s->sock);
}

//
// get information about the interfaces and what their addresses are
//

// Pass in a buffer that you think is big enough, and it'll get filled out
// error will return if you haven't passed in enough data
// ordering not guaranteed

int
cf_ifaddr_get( cf_ifaddr **ifaddr, int *ifaddr_sz, uint8_t *buf, size_t bufsz)
{
	struct ifaddrs *ifa;
	int rv = getifaddrs(&ifa);
	if (rv != 0) {
		cf_info(CF_SOCKET, " could not get interface information: return value %d errno %d",rv,errno);
		return(-1);
	}
	struct ifaddrs *ifa_orig = ifa;

	// currently, return ipv4 only (?)
	int n_ifs = 0;
	while (ifa) {
		if ((ifa->ifa_addr) && (ifa->ifa_addr->sa_family == AF_INET)) {
			n_ifs++;
		}
		ifa = ifa->ifa_next;
	}

	if (bufsz < sizeof(cf_ifaddr) * n_ifs) {
		freeifaddrs(ifa_orig);
		return(-2);
	}

	*ifaddr_sz = n_ifs;
	*ifaddr = (cf_ifaddr *) buf;
	ifa = ifa_orig;
	int i = 0;
	while (ifa) {

		if ((ifa->ifa_addr) && (ifa->ifa_addr->sa_family == AF_INET))
		{

			(*ifaddr)[i].flags = ifa->ifa_flags;
			(*ifaddr)[i].family = ifa->ifa_addr->sa_family;
			memcpy( &((*ifaddr)[i].sa), ifa->ifa_addr, sizeof(struct sockaddr) );

			i++;
		}
		ifa = ifa->ifa_next;
	}

	freeifaddrs(ifa_orig);
	return(0);
}
