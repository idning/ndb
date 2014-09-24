/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_UTIL_H_
#define _NC_UTIL_H_

#ifdef HAVE_DEBUG_LOG
# define NC_DEBUG_LOG 1
#endif

#ifdef HAVE_ASSERT_PANIC
# define NC_ASSERT_PANIC 1
#endif

#ifdef HAVE_ASSERT_LOG
# define NC_ASSERT_LOG 1
#endif

#ifdef HAVE_BACKTRACE
# define NC_HAVE_BACKTRACE 1
#endif

#define _FILE_OFFSET_BITS 64

typedef enum rstatus_s {
    NC_OK       = 0,
    NC_ERROR    = -1,
    NC_EAGAIN   = -2,
    NC_ENOMEM   = -3,
} rstatus_t;

typedef int err_t;     /* error type */

struct array;
struct string;
struct context;
struct conn;
struct conn_tqh;
struct mbuf;
struct mhdr;
struct conf;
struct stats;

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>

#include <unistd.h>
#include <inttypes.h>

#include <time.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <ctype.h>
#include <fcntl.h>
#include <netdb.h>
#include <getopt.h>

#include <sys/un.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include "nc_array.h"
#include "nc_md5.h"
#include "nc_rbtree.h"
#include "nc_string.h"
#include "nc_queue.h"
#include "nc_log.h"
#include "nc_file.h"
#include "nc_util.h"

#include "nc_event.h"
#include "nc_mbuf.h"
#include "nc_connection.h"
#include "nc_server.h"

#include "nc_conf.h"
#include "nc_signal.h"
#include "nc_misc.h"

#include "sds.h"

#endif
