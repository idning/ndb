#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "hiredis.h"
#include "async.h"
#include "adapters/ae.h"

/* Put event loop in the global scope, so it can be explicitly stopped */
static aeEventLoop *loop;

void setCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL) return;
    int * pi = (int*) privdata;

    /*printf("argv[%d]: %s\n", *pi, reply->str);*/

    (*pi)++;
    if (*pi > 100*1000)
        exit(0);

    redisAsyncCommand(c, setCallback, (char*)pi, "SET thekey %s", "xxxxxxxxxxxxxx");
}

void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Disconnected...\n");
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    redisAsyncContext *c = redisAsyncConnect("127.0.0.1", 22000);
    if (c->err) {
        /* Let *c leak for now... */
        printf("Error: %s\n", c->errstr);
        return 1;
    }

    loop = aeCreateEventLoop(1000);
    redisAeAttach(loop, c);
    redisAsyncSetConnectCallback(c,connectCallback);
    redisAsyncSetDisconnectCallback(c,disconnectCallback);

    int i = 0;
    redisAsyncCommand(c, setCallback, (char*)&i, "SET thekey %s", "xxxxxxxxxxxxxx");
    aeMain(loop);
    return 0;
}

