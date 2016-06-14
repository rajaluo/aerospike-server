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
#include <sys/ioctl.h>
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
		cf_crash(CF_SOCKET, "fcntl(%d) failed on FD %d: %d (%s)",
				cmd, fd, errno, cf_strerror(errno));
	}

	return res;
}

static int32_t
safe_ioctl(int32_t fd, int32_t req, int32_t *arg)
{
	int32_t res = ioctl(fd, req, arg);

	if (res < 0) {
		cf_crash(CF_SOCKET, "ioctl(%d) failed on FD %d: %d (%s)",
				req, fd, errno, cf_strerror(errno));
	}

	return res;
}

static void
safe_setsockopt(int32_t fd, int32_t level, int32_t name, const void *val, socklen_t len)
{
	if (setsockopt(fd, level, name, val, len) < 0) {
		cf_crash(CF_SOCKET, "setsockopt(%d¸ %d) failed on FD %d: %d (%s)",
				level, name, fd, errno, cf_strerror(errno));
	}
}

static void
safe_getsockopt(int32_t fd, int32_t level, int32_t name, void *val, socklen_t *len)
{
	if (getsockopt(fd, level, name, val, len) < 0) {
		cf_crash(CF_SOCKET, "getsockopt(%d, %d) failed on FD %d: %d (%s)",
				level, name, fd, errno, cf_strerror(errno));
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
cf_socket_disable_blocking(cf_socket sock)
{
	int32_t flags = safe_fcntl(sock.fd, F_GETFL, 0);
	safe_fcntl(sock.fd, F_SETFL, flags | O_NONBLOCK);
}

void
cf_socket_enable_blocking(cf_socket sock)
{
	int32_t flags = safe_fcntl(sock.fd, F_GETFL, 0);
	safe_fcntl(sock.fd, F_SETFL, flags & ~O_NONBLOCK);
}

void
cf_socket_disable_nagle(cf_socket sock)
{
	static const int32_t flag = 1;
	safe_setsockopt(sock.fd, SOL_TCP, TCP_NODELAY, &flag, sizeof flag);
}

void
cf_socket_enable_nagle(cf_socket sock)
{
	static const int32_t flag = 0;
	safe_setsockopt(sock.fd, SOL_TCP, TCP_NODELAY, &flag, sizeof flag);
}

void
cf_socket_keep_alive(cf_socket sock, int32_t idle, int32_t interval, int32_t count)
{
	static const int32_t flag = 1;
	safe_setsockopt(sock.fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof flag);

	if (idle > 0) {
		safe_setsockopt(sock.fd, SOL_TCP, TCP_KEEPIDLE, &idle, sizeof idle);
	}

	if (interval > 0) {
		safe_setsockopt(sock.fd, SOL_TCP, TCP_KEEPINTVL, &interval, sizeof interval);
	}

	if (count > 0) {
		safe_setsockopt(sock.fd, SOL_TCP, TCP_KEEPCNT, &count, sizeof count);
	}
}

void
cf_socket_set_send_buffer(cf_socket sock, int32_t size)
{
	safe_setsockopt(sock.fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof size);
}

void
cf_socket_set_receive_buffer(cf_socket sock, int32_t size)
{
	safe_setsockopt(sock.fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof size);
}

void cf_socket_set_window(cf_socket sock, int32_t size)
{
	safe_setsockopt(sock.fd, SOL_TCP, TCP_WINDOW_CLAMP, &size, sizeof size);
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
config_address(const cf_socket_cfg *conf, struct sockaddr *sa, cf_sock_addr *addr)
{
	cf_sock_addr _addr;

	if (addr == NULL) {
		addr = &_addr;
	}

	if (conf->addr == NULL) {
		cf_warning(CF_SOCKET, "Missing service address");
		return -1;
	}

	if (cf_ip_addr_from_string(conf->addr, &addr->addr) < 0) {
		cf_warning(CF_SOCKET, "Invalid service address: %s", conf->addr);
		return -1;
	}

	if (conf->port == 0) {
		cf_warning(CF_SOCKET, "Missing service port");
		return -1;
	}

	addr->port = conf->port;
	cf_sock_addr_to_native(addr, sa);
	return 0;
}

int32_t
cf_socket_init_server(cf_socket_cfg *conf)
{
	int32_t res = -1;
	struct sockaddr_storage sas;

	if (config_address(conf, (struct sockaddr *)&sas, NULL) < 0) {
		goto cleanup0;
	}

	int32_t fd = socket(sas.ss_family, conf->type, 0);

	if (fd < 0) {
		cf_warning(CF_SOCKET, "Error while creating socket for %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup0;
	}

	cf_socket sock = (cf_socket){ .fd = fd };
	fd = -1;

	if (conf->reuse_addr) {
		static const int32_t flag = 1;
		safe_setsockopt(sock.fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof flag);
	}

	// XXX - Why are we doing this?
	safe_fcntl(sock.fd, F_SETFD, FD_CLOEXEC);

	while (bind(sock.fd, (struct sockaddr *)&sas, addr_len((struct sockaddr *)&sas)) < 0) {
		if (errno != EADDRINUSE) {
			cf_warning(CF_SOCKET, "Error while binding to %s:%d: %d (%s)",
					conf->addr, conf->port, errno, cf_strerror(errno));
			goto cleanup1;
		}

		cf_warning(CF_SOCKET, "Socket %s:%d in use, waiting", conf->addr, conf->port);
		usleep(5 * 1000 * 1000);
	}

	if (conf->type == SOCK_STREAM && listen(sock.fd, 512) < 0) {
		cf_warning(CF_SOCKET, "Error while listening on %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup1;
	}

	// No Nagle here. It will be disabled for the accepted connections.

	conf->sock = sock;
	res = 0;
	goto cleanup0;

cleanup1:
	safe_close(sock.fd);

cleanup0:
	return res;
}

static int32_t
connect_socket(cf_socket sock, struct sockaddr *sa, int32_t timeout)
{
	cf_debug(CF_SOCKET, "Connecting FD %d", sock.fd);
	int32_t res = -1;

	cf_socket_disable_blocking(sock);
	int32_t rv = connect(sock.fd, sa, addr_len(sa));

	if (rv == 0) {
		cf_debug(CF_SOCKET, "FD %d connected [1]", sock.fd);
		res = 0;
		goto cleanup1;
	}

	if (errno != EINPROGRESS) {
		cf_warning(CF_SOCKET, "Error while connecting FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
		goto cleanup1;
	}

	int32_t efd = epoll_create(1);

	if (efd < 0) {
		cf_crash(CF_SOCKET, "epoll_create() failed: %d (%s)", errno, cf_strerror(errno));
	}

	struct epoll_event event = { .data.fd = sock.fd, .events = EPOLLOUT };

	if (epoll_ctl(efd, EPOLL_CTL_ADD, sock.fd, &event) < 0) {
		cf_crash(CF_SOCKET, "epoll_ctl() failed for FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
	}

	int32_t count = safe_wait(efd, &event, 1, timeout);

	if (count == 0) {
		cf_warning(CF_SOCKET, "Timeout while connecting FD %d", sock.fd);
		goto cleanup2;
	}

	int32_t err;
	socklen_t err_len = sizeof err;
	safe_getsockopt(sock.fd, SOL_SOCKET, SO_ERROR, &err, &err_len);

	if (err != 0) {
		cf_warning(CF_SOCKET, "Error while connecting FD %d: %d (%s)",
				sock.fd, err, cf_strerror(err));
		goto cleanup2;
	}

	cf_debug(CF_SOCKET, "FD %d connected [2]", sock.fd);
	res = 0;

cleanup2:
	if (epoll_ctl(efd, EPOLL_CTL_DEL, sock.fd, NULL) < 0) {
		cf_crash(CF_SOCKET, "epoll_ctl() failed for FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
	}

	safe_close(efd);

cleanup1:
	cf_socket_enable_blocking(sock);
	return res;
}

int32_t
cf_socket_init_client(cf_socket_cfg *conf, int32_t timeout)
{
	int32_t res = -1;
	struct sockaddr_storage sas;

	if (config_address(conf, (struct sockaddr *)&sas, NULL) < 0) {
		goto cleanup0;
	}

	int32_t fd = socket(sas.ss_family, conf->type, 0);

	if (fd < 0) {
		cf_warning(CF_SOCKET, "Error while creating socket for %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup0;
	}

	cf_socket sock = (cf_socket){ .fd = fd };
	fd = -1;

	// XXX - Why are we doing this?
	safe_fcntl(sock.fd, F_SETFD, FD_CLOEXEC);

	if (connect_socket(sock, (struct sockaddr *)&sas, timeout) < 0) {
		cf_warning(CF_SOCKET, "Error while connecting socket to %s:%d",
				conf->addr, conf->port);
		goto cleanup1;
	}

	cf_socket_disable_nagle(sock);

	conf->sock = sock;
	res = 0;
	goto cleanup0;

cleanup1:
	safe_close(sock.fd);

cleanup0:
	return res;
}

int32_t
cf_socket_init_client_nb(cf_sock_addr *addr, cf_socket *sock)
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

	cf_socket _sock = (cf_socket){ .fd = fd };
	fd = -1;

	// XXX - Why are we doing this?
	safe_fcntl(_sock.fd, F_SETFD, FD_CLOEXEC);

	cf_socket_disable_blocking(_sock);

	if (connect(_sock.fd, (struct sockaddr *)&sas, addr_len((struct sockaddr *)&sas)) < 0) {
		if (errno != EINPROGRESS) {
			cf_warning(CF_SOCKET, "Error while connecting socket to %s: %d (%s)",
					friendly, errno, cf_strerror(errno));
			goto cleanup1;
		}
	}

	*sock = _sock;
	res = 0;
	goto cleanup0;

cleanup1:
	safe_close(_sock.fd);

cleanup0:
	return res;
}

int32_t cf_socket_accept(cf_socket lsock, cf_socket *sock, cf_sock_addr *addr)
{
	int32_t res = -1;

	struct sockaddr_storage sas;
	struct sockaddr *sa = NULL;
	socklen_t sa_len = 0;

	if (addr != NULL) {
		sa = (struct sockaddr *)&sas;
		sa_len = sizeof sas;
	}

	int32_t fd = accept(lsock.fd, sa, &sa_len);

	if (fd < 0) {
		cf_debug(CF_SOCKET, "Error while accepting from FD %d: %d (%s)",
				lsock.fd, errno, cf_strerror(errno));
		goto cleanup0;
	}

	if (addr != NULL) {
		cf_sock_addr_from_native(sa, addr);
	}

	*sock = (cf_socket){ .fd = fd };
	res = 0;

cleanup0:
	return res;
}

typedef int32_t (*name_func)(int32_t fd, struct sockaddr *sa, socklen_t *sa_len);

static int32_t
x_name(name_func func, const char *which, int32_t fd, cf_sock_addr *addr)
{
	struct sockaddr_storage sas;
	socklen_t sas_len = sizeof sas;

	if (func(fd, (struct sockaddr *)&sas, &sas_len) < 0) {
		cf_warning(CF_SOCKET, "Error while getting %s name: %d (%s)",
				which, errno, cf_strerror(errno));
		return -1;
	}

	cf_sock_addr_from_native((struct sockaddr *)&sas, addr);
	return 0;
}

int32_t
cf_socket_remote_name(cf_socket sock, cf_sock_addr *addr)
{
	return x_name(getpeername, "remote", sock.fd, addr);
}

int32_t
cf_socket_local_name(cf_socket sock, cf_sock_addr *addr)
{
	return x_name(getsockname, "local", sock.fd, addr);
}

int32_t
cf_socket_available(cf_socket sock)
{
	int32_t size;
	safe_ioctl(sock.fd, FIONREAD, &size);
	return size;
}

int32_t
cf_socket_send_to(cf_socket sock, void *buff, size_t size, int32_t flags, cf_sock_addr *addr)
{
	struct sockaddr_storage sas;
	struct sockaddr *sa = NULL;
	socklen_t sa_len = 0;

	if (addr != NULL) {
		cf_sock_addr_to_native(addr, (struct sockaddr *)&sas);
		sa = (struct sockaddr *)&sas;
		sa_len = addr_len((struct sockaddr *)&sas);
	}

	int32_t res = sendto(sock.fd, buff, size, flags | MSG_NOSIGNAL, sa, sa_len);

	if (res < 0) {
		cf_debug(CF_SOCKET, "Error while sending on FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
	}

	return res;
}

int32_t
cf_socket_send(cf_socket sock, void *buff, size_t size, int32_t flags)
{
	return cf_socket_send_to(sock, buff, size, flags, NULL);
}

int32_t
cf_socket_recv_from(cf_socket sock, void *buff, size_t size, int32_t flags, cf_sock_addr *addr)
{
	struct sockaddr_storage sas;
	struct sockaddr *sa = NULL;
	socklen_t sa_len = 0;

	if (addr != NULL) {
		sa = (struct sockaddr *)&sas;
		sa_len = sizeof sas;
	}

	int32_t res = recvfrom(sock.fd, buff, size, flags | MSG_NOSIGNAL, sa, &sa_len);

	if (res < 0) {
		cf_debug(CF_SOCKET, "Error while receiving on FD %d: %d (%s)",
				sock.fd, errno, cf_strerror(errno));
	}
	else if (addr != NULL) {
		cf_sock_addr_from_native(sa, addr);
	}

	return res;
}

int32_t
cf_socket_recv(cf_socket sock, void *buff, size_t size, int32_t flags)
{
	return cf_socket_recv_from(sock, buff, size, flags, NULL);
}

static void
x_shutdown(cf_socket sock, int32_t how)
{
	if (shutdown(sock.fd, how) < 0) {
		if (errno != ENOTCONN) {
			cf_crash(CF_SOCKET, "shutdown() failed on FD %d: %d (%s)",
					sock.fd, errno, cf_strerror(errno));
		}
		else {
			cf_warning(CF_SOCKET, "shutdown() on disconnected FD %d: %d (%s)",
					sock.fd, errno, cf_strerror(errno));
		}
	}
}

void
cf_socket_write_shutdown(cf_socket sock)
{
	cf_debug(CF_SOCKET, "Shutting down write channel of FD %d", sock.fd);
	x_shutdown(sock, SHUT_WR);
}

void
cf_socket_shutdown(cf_socket sock)
{
	cf_debug(CF_SOCKET, "Shutting down FD %d", sock.fd);
	x_shutdown(sock, SHUT_RDWR);
}

void
cf_socket_close(cf_socket sock)
{
	cf_debug(CF_SOCKET, "Closing FD %d", sock.fd);
	safe_close(sock.fd);
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

	cf_socket_shutdown(sock);
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
	cf_socket_close(sock);
}

int32_t
cf_socket_mcast_init(cf_socket_mcast_cfg *mconf)
{
	static const int32_t yes = 1;
	static const int32_t no = 0;

	int32_t res = -1;

	cf_socket_cfg *conf = &mconf->conf;
	struct sockaddr_storage sas;
	cf_sock_addr addr;

	if (config_address(conf, (struct sockaddr *)&sas, &addr) < 0) {
		goto cleanup0;
	}

	cf_ip_addr _iaddr;
	cf_ip_addr *iaddr = NULL;

	if (mconf->if_addr != NULL) {
		if (cf_ip_addr_from_string(mconf->if_addr, &_iaddr) < 0) {
			cf_warning(CF_SOCKET, "Invalid multicast interface address: %s", mconf->if_addr);
			goto cleanup0;
		}

		iaddr = &_iaddr;
	}

	int32_t fd = socket(sas.ss_family, SOCK_DGRAM, 0);

	if (fd < 0) {
		cf_warning(CF_SOCKET, "Error while creating socket for %s:%d: %d (%s)",
				conf->addr, conf->port, errno, cf_strerror(errno));
		goto cleanup0;
	}

	cf_socket sock = (cf_socket){ .fd = fd };
	fd = -1;

	safe_setsockopt(sock.fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

	// XXX - Why are we doing this?
	safe_fcntl(sock.fd, F_SETFD, FD_CLOEXEC);

#ifdef IP_MULTICAST_ALL
	// [FYI:  This socket option has existed since the Linux 2.6.31 kernel.]

	// Only receive traffic from multicast groups this socket actually joins.
	// [Note:  Bind address filtering takes precedence, so this is simply an extra level of restriction.]
	safe_setsockopt(sock.fd, IPPROTO_IP, IP_MULTICAST_ALL, &no, sizeof no);
#endif

	if (iaddr != NULL) {
		char tmp[1000];
		cf_ip_addr_to_string(iaddr, tmp, sizeof tmp);
		cf_info(CF_SOCKET, "Setting multicast interface address: %s", tmp);

		if (cf_socket_mcast_set_inter(sock, iaddr) < 0) {
			cf_warning(CF_SOCKET, "Error while binding to interface %s", tmp);
			goto cleanup1;
		}
	}

	uint8_t ttl = mconf->ttl;

	if (ttl > 0) {
		cf_info(CF_SOCKET, "Setting multicast TTL: %d", ttl);
		safe_setsockopt(sock.fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof ttl);
	}

	while (bind(sock.fd, (struct sockaddr *)&sas, addr_len((struct sockaddr *)&sas)) < 0) {
		if (errno != EADDRINUSE) {
			cf_warning(CF_SOCKET, "Error while binding to %s:%d: %d (%s)",
					conf->addr, conf->port, errno, cf_strerror(errno));
			goto cleanup1;
		}

		cf_warning(CF_SOCKET, "Socket %s:%d in use, waiting", conf->addr, conf->port);
		usleep(5 * 1000 * 1000);
	}

	char tmp[1000];
	cf_ip_addr_to_string(&addr.addr, tmp, sizeof tmp);
	cf_info(CF_SOCKET, "Joining multicast group: %s", tmp);

	if (cf_socket_mcast_join_group(sock, iaddr, &addr.addr) < 0) {
		cf_warning(CF_SOCKET, "Error while joining multicast group %s", tmp);
		goto cleanup1;
	}

	conf->sock = sock;
	res = 0;
	goto cleanup0;

cleanup1:
	safe_close(sock.fd);

cleanup0:
	return res;
}

void
cf_socket_mcast_close(cf_socket_mcast_cfg *mconf)
{
	cf_socket_cfg *conf = &mconf->conf;
	safe_close(conf->sock.fd);
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
