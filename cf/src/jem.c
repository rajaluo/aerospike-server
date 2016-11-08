/*
 * jem.c
 *
 * Copyright (C) 2013-2014 Aerospike, Inc.
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
 * JEMalloc Interface.
 */

#include "jem.h"

#include <errno.h>
#include <jemalloc/jemalloc.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include "fault.h"

/* SYNOPSIS
 *  This is the implementation of a simple interface to JEMalloc.
 *  It provides a higher-level interface to working with JEMalloc arenas
 *  and the thread caching feature.
 *
 *  To enable these functions, first call "jem_init(true)".  Otherwise,
 *  and by default, the use of these JEMalloc features is disabled, and
 *  all of these functions do nothing but return a failure status code (-1),
 *  or in the case of "jem_allocate_in_arena()", will simply use "malloc(3)",
 *  which may be bound to JEMalloc's "malloc(3)", but will disregard the
 *  arguments other than size.
 *
 *  These functions use JEMalloc "MIB"s internally instead of strings for
 *  efficiency.
 */

/*
 *  Set the default JEMalloc configuration options.
 *
 *  Ideally, the nuber of arenas should be >= # threads so that every thread
 *   can have its own arena, and then we can use this JEM API to share arenas
 *   across threads as needed for guaranteeing data locality.
 *
 *  N.B.:  These default options can be overriden at run time via setting
 *          the "MALLOC_CONF" environment variable and/or the "name" of
 *          the "/etc/malloc.conf" symbolic link.
 */
JEMALLOC_ALIGNED(16) const char *malloc_conf = "narenas:150";

/*
 *  Is the JEMalloc interface enabled?  (By default, no.)
 */
static bool jem_enabled = false;

/*
 *  JEMalloc MIB values for the "arenas.extend" control.
 */
static size_t arenas_extend_mib[2], arenas_extend_miblen = sizeof(arenas_extend_mib);

/*
 *  JEMalloc MIB values for fragmentation statistics:
 *
 *  - epoch
 *  - stats.allocated
 *  - stats.active
 *  - stats.mapped
 */
static size_t epoch_mib[1], epoch_miblen = sizeof(epoch_mib);
static size_t stats_allocated_mib[2], stats_allocated_miblen = sizeof(stats_allocated_mib);
static size_t stats_active_mib[2], stats_active_miblen = sizeof(stats_active_mib);
static size_t stats_mapped_mib[2], stats_mapped_miblen = sizeof(stats_mapped_mib);

/*
 *  Initialize the interface to JEMalloc.
 *  If enable is true, the JEMalloc features will be enabled, otherwise they will be disabled.
 *  Returns 0 if successful, -1 otherwise.
 */
int jem_init(bool enable)
{
	if (enable) {
		// Initialize JEMalloc MIBs.
		char *mib = "arenas.extend";
		if (mallctlnametomib(mib, arenas_extend_mib, &arenas_extend_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		mib = "epoch";
		if (mallctlnametomib(mib, epoch_mib, &epoch_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		mib = "stats.allocated";
		if (mallctlnametomib(mib, stats_allocated_mib, &stats_allocated_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		mib = "stats.active";
		if (mallctlnametomib(mib, stats_active_mib, &stats_active_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		mib = "stats.mapped";
		if (mallctlnametomib(mib, stats_mapped_mib, &stats_mapped_miblen)) {
			cf_crash(CF_JEM, "Failed to get JEMalloc MIB for \"%s\" (errno %d)!", mib, errno);
		}

		// Open for business.
		jem_enabled = true;
	} else {
		// Don't use the JEMalloc APIs.
		jem_enabled = false;
	}

	return 0;
}

/*
 *  Create a new JEMalloc arena.
 *  Returns the arena index (>= 0) upon success or -1 upon failure.
 */
int jem_create_arena(void)
{
	int retval = -1;
	unsigned new_arena = 0;

	if (jem_enabled) {
		size_t len = sizeof(unsigned);
		if ((retval = mallctlbymib(arenas_extend_mib, arenas_extend_miblen, &new_arena, &len, NULL, 0))) {
			cf_warning(CF_JEM, "Failed to create a new JEMalloc arena (rv %d ; errno %d)!", retval, errno);
		} else {
			cf_debug(CF_JEM, "Created new JEMalloc arena #%d.", new_arena);
		}
	}

	return (!retval ? new_arena : retval);
}

/*
 *  Read the JEMalloc statistics required for calculating memory fragmentation.
 */
void jem_get_frag_stats(size_t *allocated, size_t *active, size_t *mapped)
{
	if (!jem_enabled) {
		cf_crash(CF_JEM, "JEMalloc is not enabled");
	}

	size_t len = sizeof(size_t);
	size_t epoch = 1;

	if (mallctlbymib(epoch_mib, epoch_miblen, &epoch, &len, &epoch, sizeof(epoch)) != 0) {
		cf_crash(CF_JEM, "Failed to retrieve JEMalloc's stats.allocated");
	}

	len = sizeof(size_t);

	if (mallctlbymib(stats_allocated_mib, stats_allocated_miblen, allocated, &len, NULL, 0) != 0) {
		cf_crash(CF_JEM, "Failed to retrieve JEMalloc's stats.allocated");
	}

	len = sizeof(size_t);

	if (mallctlbymib(stats_active_mib, stats_active_miblen, active, &len, NULL, 0) != 0) {
		cf_crash(CF_JEM, "Failed to retrieve JEMalloc's stats.active");
	}

	len = sizeof(size_t);

	if (mallctlbymib(stats_mapped_mib, stats_mapped_miblen, mapped, &len, NULL, 0) != 0) {
		cf_crash(CF_JEM, "Failed to retrieve JEMalloc's stats.mapped");
	}
}

/*
 *  Callback function for logging JEMalloc stats to an output file.
 */
static void my_malloc_message(void *cbopaque, const char *s)
{
	FILE *outfile = (FILE *) cbopaque;

	fprintf(outfile, "%s", s);
}

/*
 *  Log information about the state of JEMalloc to a file with the given options.
 *  Pass NULL for file or options to get the default behavior, e.g., logging to stderr.
 */
void jem_log_stats(char *file, char *options)
{
	if (jem_enabled) {
		if (!file) {
			malloc_stats_print(NULL, NULL, options);
		} else {
			FILE *outfile;

			if (!(outfile = fopen(file, "a"))) {
				cf_warning(CF_JEM, "Cannot write JEMalloc stats to file \"%s\"! (errno %d)", file, errno);
				return;
			}

			char datestamp[1024];
			struct tm nowtm;
			time_t now = time(NULL);
			gmtime_r(&now, &nowtm);
			strftime(datestamp, sizeof(datestamp), "%b %d %Y %T %Z:\n", &nowtm);

			fprintf(outfile, "%s", datestamp);
			malloc_stats_print(my_malloc_message, outfile, options);
			fprintf(outfile, "\n");

			fclose(outfile);
		}
	}
}
