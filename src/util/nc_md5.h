/*
 * file   : nc_md5.h
 * author : ning
 * date   : 2014-07-01 21:39:17
 */

#ifndef _NC_MD5_H_
#define _NC_MD5_H_

#include "nc_core.h"

typedef unsigned int MD5_u32plus;

typedef struct {
    MD5_u32plus lo, hi;
    MD5_u32plus a, b, c, d;
    unsigned char buffer[64];
    MD5_u32plus block[16];
} MD5_CTX;

void MD5_Init(MD5_CTX *ctx);
void MD5_Update(MD5_CTX *ctx, void *data, unsigned long size);
void MD5_Final(unsigned char *result, MD5_CTX *ctx);
void md5_signature(unsigned char *key, unsigned long length, unsigned char *result);
uint32_t hash_md5(const char *key, size_t key_length);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

