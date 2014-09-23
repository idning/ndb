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

#ifndef _NC_ARRAY_H_
#define _NC_ARRAY_H_

#include "nc_util.h"

typedef int (*array_compare_t)(const void *, const void *);
typedef rstatus_t (*array_each_t)(void *, void *);

typedef struct array_s {
    uint32_t nelem;  /* # element */
    void     *elem;  /* element */
    size_t   size;   /* element size */
    uint32_t nalloc; /* # allocated element */
} array_t;

#define null_array { 0, NULL, 0, 0 }

static inline void
array_null(array_t *a)
{
    a->nelem = 0;
    a->elem = NULL;
    a->size = 0;
    a->nalloc = 0;
}

static inline void
array_set(array_t *a, void *elem, size_t size, uint32_t nalloc)
{
    a->nelem = 0;
    a->elem = elem;
    a->size = size;
    a->nalloc = nalloc;
}

static inline uint32_t
array_n(const array_t *a)
{
    return a->nelem;
}

array_t *array_create(uint32_t n, size_t size);
void array_destroy(array_t *a);

uint32_t array_idx(array_t *a, void *elem);
void *array_push(array_t *a);
void *array_pop(array_t *a);
void *array_get(array_t *a, uint32_t idx);
void *array_top(array_t *a);
void array_swap(array_t *a, array_t *b);
void array_sort(array_t *a, array_compare_t compare);
rstatus_t array_each(array_t *a, array_each_t func, void *data);

#endif
