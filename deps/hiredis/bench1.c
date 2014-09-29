#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hiredis.h"

int main(void) {
    unsigned int i;
    redisContext *c;
    redisReply *reply;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout((char*)"127.0.0.5", 22000, timeout);
    if (c->err) {
        printf("Connection error: %s\n", c->errstr);
        exit(1);
    }
    for(i=0; i<100*1000; i++){
        reply = redisCommand(c,"SET %s %s", "foo", "hello world");
        freeReplyObject(reply);
    }
    return 0;
}
