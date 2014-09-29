#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "hiredis.h"

/*int tread(int fd, char*buf, int len){*/
    /*int readed = 0;*/
    /*while(readed<len){*/
        /*read(fd, buf+readed, len-readed);*/
    /*}*/
    /*return len;*/
/*}*/

int tread(int fd, char*buf, int len){
    return read(fd, buf, len);
}

int twrite(int fd, char*buf, int len){
    return write(fd, buf, len);
}


int main(void) {
    unsigned int i;
    redisContext *c;
    redisReply *reply;
    int ret;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout((char*)"127.0.0.5", 22000, timeout);
    if (c->err) {
        printf("Connection error: %s\n", c->errstr);
        exit(1);
    }

    char *cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$9\r\nbarbarbar\r\n";
    int len = strlen(cmd);

    char buf[1024];
    for(i=0; i<100*1000; i++){
        ret = twrite(c->fd, cmd, len);
        assert(len == ret);

        /*fprintf(stderr, "read\n");*/
        ret = tread(c->fd, buf, 5);
        assert(5 == ret);

        buf[5] = 0;
        /*fprintf(stderr, "%d: %s\n", i, buf);*/
        /*assert(0 == strcmp(buf, "+OK\r\n"));*/
    }
    return 0;
}
