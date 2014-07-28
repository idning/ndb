/*
 * file   : nc_conf.h
 * author : ning
 * date   : 2014-06-29 21:01:29
 */

#ifndef _NC_LUACONF_H_
#define _NC_LUACONF_H_

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include "nc_util.h"

typedef struct nc_conf_s {
    lua_State  *L;
    const char *conf;
} nc_conf_t;

rstatus_t nc_conf_init(nc_conf_t *conf, const char *filename);
rstatus_t nc_conf_deinit(nc_conf_t *conf);

char *nc_conf_get_str(nc_conf_t *conf, const char *name, char *default_v);
int nc_conf_get_num(nc_conf_t *conf, const char *name, int default_v);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
