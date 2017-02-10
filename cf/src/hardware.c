/*
 * hardware.c
 *
 * Copyright (C) 2016-2017 Aerospike, Inc.
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

#include "hardware.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <syscall.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "fault.h"
#include "util.h"

#include "warnings.h"

typedef enum {
	CF_READ_FILE_OK,
	CF_READ_FILE_NOT_FOUND,
	CF_READ_FILE_ERROR
} cf_read_file_res;

typedef uint16_t cf_topo_os_numa_node_index;
typedef uint16_t cf_topo_os_package_index;
typedef uint16_t cf_topo_os_core_index;

#define CF_TOPO_INVALID_INDEX ((uint16_t)-1)

static uint16_t g_n_cores;
static uint16_t g_n_os_cpus;
static uint16_t g_n_cpus;

static cf_topo_os_cpu_index g_core_index_to_os_cpu_index[CPU_SETSIZE];
static cf_topo_os_cpu_index g_cpu_index_to_os_cpu_index[CPU_SETSIZE];
static cf_topo_cpu_index g_os_cpu_index_to_cpu_index[CPU_SETSIZE];

static cf_read_file_res
cf_read_file(const char *path, void *buff, size_t *limit)
{
	cf_detail(CF_MISC, "reading file %s with buffer size %zu", path, *limit);
	int32_t fd = open(path, O_RDONLY);

	if (fd < 0) {
		if (errno == ENOENT) {
			cf_detail(CF_MISC, "file %s not found", path);
			return CF_READ_FILE_NOT_FOUND;
		}

		cf_warning(CF_MISC, "error while opening file %s for reading: %d (%s)",
				path, errno, cf_strerror(errno));
		return CF_READ_FILE_ERROR;
	}

	size_t total = 0;

	while (total < *limit) {
		cf_detail(CF_MISC, "reading %zd byte(s) at offset %zu", *limit - total, total);
		ssize_t len = read(fd, (uint8_t *)buff + total, *limit - total);
		CF_NEVER_FAILS(len);

		if (len == 0) {
			cf_detail(CF_MISC, "EOF");
			break;
		}

		total += (size_t)len;
	}

	cf_detail(CF_MISC, "read %zu byte(s) from file %s", total, path);
	cf_read_file_res res;

	if (total == *limit) {
		cf_warning(CF_MISC, "read buffer too small for file %s", path);
		res = CF_READ_FILE_ERROR;
	}
	else {
		res = CF_READ_FILE_OK;
		*limit = total;
	}

	CF_NEVER_FAILS(close(fd));
	return res;
}

static void
mask_to_string(cpu_set_t *mask, char *buff, size_t limit)
{
	cf_topo_os_cpu_index max;

	for (max = CPU_SETSIZE - 1; max > 0; --max) {
		if (CPU_ISSET(max, mask)) {
			break;
		}
	}

	int32_t words = max / 32 + 1;
	size_t size = (size_t)words * 9;

	if (size > limit) {
		cf_crash(CF_MISC, "CPU mask buffer overflow: %zu vs. %zu", size, limit);
	}

	for (int32_t i = words - 1; i >= 0; --i) {
		uint32_t val = 0;

		for (int32_t k = 0; k < 32; ++k) {
			if (CPU_ISSET((size_t)(i * 32 + k), mask)) {
				val |= 1u << k;
			}
		}

		snprintf(buff, limit, "%08x", val);

		if (i > 0) {
			buff[8] = ',';
		}

		buff += 9;
		limit -= 9;
	}
}

static cf_read_file_res
read_index(const char *path, uint16_t *val)
{
	cf_detail(CF_MISC, "reading index from file %s", path);
	char buff[100];
	size_t limit = sizeof(buff);
	cf_read_file_res res = cf_read_file(path, buff, &limit);

	if (res != CF_READ_FILE_OK) {
		return res;
	}

	buff[limit - 1] = '\0';
	cf_detail(CF_MISC, "parsing index \"%s\"", buff);

	char *end;
	uint64_t x = strtoul(buff, &end, 10);

	if (*end != '\0' || x >= CPU_SETSIZE) {
		cf_warning(CF_MISC, "invalid index \"%s\" in %s", buff, path);
		return CF_READ_FILE_ERROR;
	}

	*val = (uint16_t)x;
	return CF_READ_FILE_OK;
}

static cf_read_file_res
read_list(const char *path, cpu_set_t *mask)
{
	cf_detail(CF_MISC, "reading list from file %s", path);
	char buff[1000];
	size_t limit = sizeof(buff);
	cf_read_file_res res = cf_read_file(path, buff, &limit);

	if (res != CF_READ_FILE_OK) {
		return res;
	}

	buff[limit - 1] = '\0';
	cf_detail(CF_MISC, "parsing list \"%s\"", buff);

	CPU_ZERO(mask);
	char *walker = buff;

	while (true) {
		char *delim;
		uint64_t from = strtoul(walker, &delim, 10);
		uint64_t thru;

		if (*delim == ',' || *delim == '\0'){
			thru = from;
		}
		else if (*delim == '-') {
			walker = delim + 1;
			thru = strtoul(walker, &delim, 10);
		}
		else {
			cf_warning(CF_MISC, "invalid list \"%s\" in %s", buff, path);
			return CF_READ_FILE_ERROR;
		}

		if (from >= CPU_SETSIZE || thru >= CPU_SETSIZE || from > thru) {
			cf_warning(CF_MISC, "invalid list \"%s\" in %s", buff, path);
			return CF_READ_FILE_ERROR;
		}

		cf_detail(CF_MISC, "marking %d through %d", (int32_t)from, (int32_t)thru);

		for (size_t i = from; i <= thru; ++i) {
			CPU_SET(i, mask);
		}

		if (*delim == '\0') {
			break;
		}

		walker = delim + 1;
	}

	char buff2[1000];
	mask_to_string(mask, buff2, sizeof(buff2));
	cf_detail(CF_MISC, "list \"%s\" -> mask %s", buff, buff2);

	return CF_READ_FILE_OK;
}

bool
cf_topo_init(cf_topo_numa_node_index a_numa_node, bool pin)
{
	if (!pin) {
		cf_detail(CF_MISC, "not bound to any NUMA node");
	}
	else {
		cf_detail(CF_MISC, "bound to NUMA node %hu", a_numa_node);
	}

	cf_detail(CF_MISC, "detecting online CPUs");
	cpu_set_t online;

	if (read_list("/sys/devices/system/cpu/online", &online) != CF_READ_FILE_OK) {
		cf_crash(CF_MISC, "error while reading list of online CPUs");
	}

	cf_detail(CF_MISC, "learning CPU topology");

	cf_topo_numa_node_index os_numa_node_index_map[CPU_SETSIZE];

	for (int32_t i = 0; i < CPU_SETSIZE; ++i) {
		g_core_index_to_os_cpu_index[i] = CF_TOPO_INVALID_INDEX;
		g_cpu_index_to_os_cpu_index[i] = CF_TOPO_INVALID_INDEX;
		g_os_cpu_index_to_cpu_index[i] = CF_TOPO_INVALID_INDEX;
	}

	cpu_set_t covered_numa_nodes;
	cpu_set_t covered_cores[CPU_SETSIZE]; // One mask per package.

	CPU_ZERO(&covered_numa_nodes);

	for (int32_t i = 0; i < CPU_SETSIZE; ++i) {
		CPU_ZERO(&covered_cores[i]);
	}

	uint16_t n_numa_nodes = 0;
	g_n_cores = 0;
	g_n_os_cpus = 0;
	g_n_cpus = 0;
	char path[1000];

	// Loop through all CPUs in the system by looping through OS CPU indexes.

	for (g_n_os_cpus = 0; g_n_os_cpus < CPU_SETSIZE; ++g_n_os_cpus) {
		cf_detail(CF_MISC, "querying OS CPU index %hu", g_n_os_cpus);

		// Let's look at the CPU's package.

		snprintf(path, sizeof(path),
				"/sys/devices/system/cpu/cpu%hu/topology/physical_package_id",
				g_n_os_cpus);
		cf_topo_os_package_index i_os_package;
		cf_read_file_res res = read_index(path, &i_os_package);

		// The entry doesn't exist. We've processed all available CPUs. Stop
		// looping through the CPUs.

		if (res == CF_READ_FILE_NOT_FOUND) {
			break;
		}

		if (res != CF_READ_FILE_OK) {
			cf_crash(CF_MISC, "error while reading OS package index from %s", path);
		}

		cf_detail(CF_MISC, "OS package index is %hu", i_os_package);

		// Only consider CPUs that are actually in use.

		if (!CPU_ISSET(g_n_os_cpus, &online)) {
			cf_detail(CF_MISC, "OS CPU index %hu is offline", g_n_os_cpus);
			continue;
		}

		// Let's look at the CPU's underlying core. In Hyper Threading systems,
		// two (logical) CPUs share one (physical) core.

		snprintf(path, sizeof(path),
				"/sys/devices/system/cpu/cpu%hu/topology/core_id",
				g_n_os_cpus);
		cf_topo_os_core_index i_os_core;
		res = read_index(path, &i_os_core);

		if (res != CF_READ_FILE_OK) {
			cf_crash(CF_MISC, "error while reading OS core index from %s", path);
		}

		cf_detail(CF_MISC, "OS core index is %hu", i_os_core);

		// Consider a core when we see it for the first time. In other words, we
		// consider the first Hyper Threading peer of each core to be that core.

		bool new_core;

		if (CPU_ISSET(i_os_core, &covered_cores[i_os_package])) {
			cf_detail(CF_MISC, "Core (%hu, %hu) already covered", i_os_core, i_os_package);
			new_core = false;
		}
		else {
			cf_detail(CF_MISC, "Core (%hu, %hu) is new", i_os_core, i_os_package);
			new_core = true;
			CPU_SET(i_os_core, &covered_cores[i_os_package]);
		}

		// Identify the NUMA node of the current CPU. We simply look for the
		// current CPU's topology info subtree in each NUMA node's subtree.
		// Specifically, we look for the current CPU's "core_id" entry.

		cf_topo_os_numa_node_index i_os_numa_node;

		for (i_os_numa_node = 0; i_os_numa_node < CPU_SETSIZE; ++i_os_numa_node) {
			snprintf(path, sizeof(path),
					"/sys/devices/system/cpu/cpu%hu/node%hu/cpu%hu/topology/core_id",
					g_n_os_cpus, i_os_numa_node, g_n_os_cpus);
			uint16_t dummy;
			res = read_index(path, &dummy);

			// We found the NUMA node that has the current CPU in its subtree.

			if (res == CF_READ_FILE_OK) {
				break;
			}

			if (res != CF_READ_FILE_NOT_FOUND) {
				cf_crash(CF_MISC, "error while reading core number from %s", path);
			}
		}

		// Some Docker installations seem to not have any NUMA information
		// in /sys. In this case, assume a system with a single NUMA node.

		if (i_os_numa_node == CPU_SETSIZE) {
			cf_detail(CF_MISC, "OS CPU index %hu does not have a NUMA node", g_n_os_cpus);
			i_os_numa_node = 0;
		}

		cf_detail(CF_MISC, "OS NUMA node index is %hu", i_os_numa_node);

		// Again, just like with cores, we consider a NUMA node when we encounter
		// it for the first time.

		bool new_numa_node;

		if (CPU_ISSET(i_os_numa_node, &covered_numa_nodes)) {
			cf_detail(CF_MISC, "OS NUMA node index %hu already covered", i_os_numa_node);
			new_numa_node = false;
		}
		else {
			cf_detail(CF_MISC, "OS NUMA node index %hu is new", i_os_numa_node);
			new_numa_node = true;
			CPU_SET(i_os_numa_node, &covered_numa_nodes);
		}

		// Now we know that the CPU is online and we know, whether it is in a newly
		// seen core (new_core) and/or a newly seen NUMA node (new_numa_node).

		cf_topo_numa_node_index i_numa_node;

		if (new_numa_node) {
			i_numa_node = n_numa_nodes;
			++n_numa_nodes;
			os_numa_node_index_map[i_os_numa_node] = i_numa_node;
			cf_detail(CF_MISC, "OS NUMA node index %hu -> new NUMA node index %hu",
					i_os_numa_node, i_numa_node);
		}
		else {
			i_numa_node = os_numa_node_index_map[i_os_numa_node];
			cf_detail(CF_MISC, "OS NUMA node index %hu -> existing NUMA node index %hu",
					i_os_numa_node, i_numa_node);
		}

		// If we're in NUMA mode and the CPU isn't on the NUMA mode that we're
		// running on, then ignore the CPU.

		if (pin && a_numa_node != i_numa_node) {
			cf_detail(CF_MISC, "Skipping unwanted NUMA node index %hu", i_numa_node);
			continue;
		}

		// If the CPU is a new core, then map a new core index to the OS CPU index.

		if (new_core) {
			g_core_index_to_os_cpu_index[g_n_cores] = g_n_os_cpus;
			cf_detail(CF_MISC, "Core index %hu -> OS CPU index %hu", g_n_cores, g_n_os_cpus);
			++g_n_cores;
		}

		// Map the OS CPU index to a new CPU index and vice versa.

		g_os_cpu_index_to_cpu_index[g_n_os_cpus] = g_n_cpus;
		g_cpu_index_to_os_cpu_index[g_n_cpus] = g_n_os_cpus;

		cf_detail(CF_MISC, "OS CPU index %hu <-> CPU index %hu", g_n_os_cpus, g_n_cpus);
		++g_n_cpus;
	}

	if (g_n_os_cpus == CPU_SETSIZE) {
		cf_crash(CF_MISC, "too many CPUs");
	}

	if (!pin) {
		cf_info(CF_MISC, "detected %hu CPU(s), %hu core(s), %hu NUMA node(s)",
				g_n_cpus, g_n_cores, n_numa_nodes);
		return true;
	}

	if (a_numa_node >= n_numa_nodes) {
		cf_topo_numa_node_index clamp = (cf_topo_numa_node_index)(a_numa_node % n_numa_nodes);
		cf_detail(CF_MISC, "invalid NUMA node index: %hu, clamping to %hu", a_numa_node, clamp);
		return cf_topo_init(clamp, pin);
	}

	cf_info(CF_MISC, "detected %hu CPU(s), %hu core(s) on NUMA node %hu",
			g_n_cpus, g_n_cores, a_numa_node);

	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);

	for (cf_topo_cpu_index i_cpu = 0; i_cpu < g_n_cpus; ++i_cpu) {
		cf_topo_os_cpu_index i_os_cpu = g_cpu_index_to_os_cpu_index[i_cpu];
		CPU_SET(i_os_cpu, &cpu_set);
	}

	char buff[1000];
	mask_to_string(&cpu_set, buff, sizeof(buff));
	cf_detail(CF_MISC, "NUMA node %hu CPU mask: %s", a_numa_node, buff);

	if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) < 0) {
		cf_crash(CF_MISC, "error while pinning thread to NUMA node %hu: %d (%s)",
				a_numa_node, errno, cf_strerror(errno));
	}

	return true;
}

static cf_topo_cpu_index
random_cpu()
{
	static __thread uint64_t state = 0;

	if (state == 0) {
		state = (uint64_t)syscall(SYS_gettid);
	}

	state = state * 6364136223846793005 + 1;

	if (state == 0) {
		state = 1;
	}

	return (cf_topo_cpu_index)((state >> 32) % g_n_cpus);
}

uint16_t
cf_topo_count_cores(void)
{
	return g_n_cores;
}

uint16_t
cf_topo_count_cpus(void)
{
	return g_n_cpus;
}

cf_topo_cpu_index cf_topo_current_cpu(void)
{
	cf_detail(CF_MISC, "getting current OS CPU index");
	int32_t os = sched_getcpu();

	if (os < 0) {
		cf_crash(CF_MISC, "error while getting OS CPU index: %d (%s)",
				errno, cf_strerror(errno));
	}

	return cf_topo_os_cpu_index_to_cpu_index((cf_topo_os_cpu_index)os);
}

static void
pin_to_os_cpu(cf_topo_os_cpu_index i_os_cpu)
{
	cf_detail(CF_MISC, "pinning to OS CPU index %hu", i_os_cpu);

	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	CPU_SET(i_os_cpu, &cpu_set);

	if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) < 0) {
		cf_crash(CF_MISC, "error while pinning thread to OS CPU %hu: %d (%s)",
				i_os_cpu, errno, cf_strerror(errno));
	}
}

void
cf_topo_pin_to_core(cf_topo_core_index i_core)
{
	cf_detail(CF_MISC, "pinning to core index %hu", i_core);

	if (i_core >= g_n_cores) {
		cf_crash(CF_MISC, "invalid core index %hu", i_core);
	}

	pin_to_os_cpu(g_core_index_to_os_cpu_index[i_core]);
}

void
cf_topo_pin_to_cpu(cf_topo_cpu_index i_cpu)
{
	cf_detail(CF_MISC, "pinning to CPU index %hu", i_cpu);

	if (i_cpu >= g_n_cpus) {
		cf_crash(CF_MISC, "invalid CPU index %hu", i_cpu);
	}

	pin_to_os_cpu(g_cpu_index_to_os_cpu_index[i_cpu]);
}

cf_topo_cpu_index
cf_topo_os_cpu_index_to_cpu_index(cf_topo_os_cpu_index i_os_cpu)
{
	cf_detail(CF_MISC, "translating OS CPU index %hu", i_os_cpu);

	if (i_os_cpu >= g_n_os_cpus) {
		cf_crash(CF_MISC, "invalid OS CPU index %hu", i_os_cpu);
	}

	cf_topo_cpu_index i_cpu = g_os_cpu_index_to_cpu_index[i_os_cpu];

	if (i_cpu == CF_TOPO_INVALID_INDEX) {
		cf_detail(CF_MISC, "randomly remapping foreign OS CPU index %hu", i_os_cpu);
		i_cpu = random_cpu();
	}

	cf_detail(CF_MISC, "CPU index is %hu", i_cpu);
	return i_cpu;
}
