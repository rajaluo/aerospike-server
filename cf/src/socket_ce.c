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

#include <netdb.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <arpa/inet.h>
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

	struct sockaddr_in *sai = (struct sockaddr_in *)&info->ai_addr;
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

	return 0;
}

int32_t
cf_ip_addr_from_binary(const uint8_t *binary, cf_ip_addr *addr, size_t size)
{
	if (size < 5) {
		cf_warning(CF_SOCKET, "Input buffer underflow");
		return -1;
	}

	if (binary[0] != AF_INET) {
		cf_crash(CF_SOCKET, "Invalid address family: %d", binary[0]);
	}

	memcpy(&addr->s_addr, binary + 1, 4);
	return 5;
}

int32_t
cf_ip_addr_to_binary(const cf_ip_addr *addr, uint8_t *binary, size_t size)
{
	if (size < 5) {
		cf_warning(CF_SOCKET, "Output buffer overflow");
		return -1;
	}

	binary[0] = AF_INET;
	memcpy(binary + 1, &addr->s_addr, 4);
	return 5;
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

	addr->sin_family = AF_INET;
	const char *host = safe_strndup(string, colon - string);

	if (cf_ip_addr_from_string(string, &addr->sin_addr) < 0) {
		cf_warning(CF_SOCKET, "Invalid host address '%s' in socket address '%s'", host, string);
		goto cleanup1;
	}

	cf_ip_port port;

	if (cf_ip_port_from_string(colon + 1, &port) < 0) {
		cf_warning(CF_SOCKET, "Invalid port '%s' in socket address '%s'", colon + 1, string);
		goto cleanup1;
	}

	addr->sin_port = htons(port);
	res = 0;

cleanup1:
	cf_free((void *)host);

cleanup0:
	return res;
}

void
cf_sock_addr_from_binary_legacy(const uint64_t *binary, cf_sock_addr *addr)
{
}

void
cf_sock_addr_to_binary_legacy(const cf_sock_addr *addr, uint64_t *binary)
{
}
