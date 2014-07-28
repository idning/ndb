/*
 * file   : nc_conf.c
 * author : ning
 * date   : 2014-06-29 21:10:59
 */

#include "nc_util.h"

rstatus_t
nc_conf_init(nc_conf_t *conf, const char *filename)
{
    lua_State       *L;

    L = lua_open();   /* opens Lua */
    if (L == NULL) {
        goto fail;
    }

    luaL_openlibs(L);

    if (luaL_loadfile(L, filename)) {
        log_stderr("luaL_loadfile on %s failed: %s",
                   filename, lua_tostring(L, -1));
        lua_pop(L, 1);
        goto fail;
    }

    if (lua_pcall(L, 0, 0, 0)) {
        log_stderr("lua_pcall on %s failed: %s",
                   filename, lua_tostring(L, -1));
        lua_pop(L, 1);
        goto fail;
    }

    conf->L = L;
    return NC_OK;

fail:
    if (L) {
        lua_close(L);
    }
    return NC_ERROR;
}

rstatus_t
nc_conf_deinit(nc_conf_t *conf)
{
    if (conf->L) {
        lua_close(conf->L);
        conf->L = NULL;
    }
    return NC_OK;
}

/**
 * eval a statement, leave value in the stack
 * like luaL_xxx functions, return 0 on success.
 */
static int
_lua_eval(lua_State *L, const char *expr)
{
    char       *buf;
    int         status;

    buf = nc_alloc(strlen(expr) + sizeof("return "));
    strcpy(buf, "return ");
    strcat(buf, expr);
    status = luaL_loadbuffer(L, buf, strlen(buf), "eval");
    if (status) {
        log_stderr("nc_conf: error on loadbuffer: %s", lua_tostring(L, -1));
        lua_pop(L, 1);
        goto out;
    }

    status = lua_pcall(L, 0, 1, 0);
    if (status) {
        log_stderr("nc_conf: error on lua_pcall: %s", lua_tostring(L, -1));
        lua_pop(L, 1);
        goto out;
    }

    if (lua_isnil(L, -1)) {
        log_stderr("nc_conf: value is nil: %s", expr);
        status = NC_ERROR;
    }
    status = NC_OK;

out:
    nc_free(buf);
    return status;
}

char *
nc_conf_get_str(nc_conf_t *conf, const char *name, char *default_value)
{
    rstatus_t         status;
    char             *ret;
    lua_State        *L = conf->L;

    status = _lua_eval(L, name);
    if (status) {
        log_stderr("nc_conf: error on _lua_eval: %s", name);
        ret = default_value;
        goto out;
    }

    /* lua_getglobal(conf->L, name); */
    if (!lua_isstring(L, -1)) {
        log_stderr("nc_conf: use default_value for option %s - %s", name, default_value);
        ret = default_value;
        goto out;
    }

    ret = strdup(lua_tostring(conf->L, -1));

out:
    lua_pop(L, lua_gettop(L));
    return ret;
}

int
nc_conf_get_num(nc_conf_t *conf, const char *name, int default_value)
{
    rstatus_t        status;
    int              ret;
    lua_State       *L = conf->L;

    status = _lua_eval(L, name);
    if (status) {
        log_stderr("nc_conf: error on _lua_eval: %s", name);
        ret = default_value;
        goto out;
    }

    if (!lua_isnumber(L, -1)) {
        log_stderr("nc_conf: use default_value for option %s - %d", name, default_value);
        ret = default_value;
        goto out;
    }

    ret = lua_tonumber(L, -1);

out:
    lua_pop(L, lua_gettop(L));
    return ret;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
