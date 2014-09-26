/*
 * file   : file.h
 * author : ning
 * date   : 2014-04-24 09:32:02
 */

#ifndef _FILE_H_
#define _FILE_H_

#include "nc_util.h"
#include "sds.h"

static inline bool
fs_exists(const char *filename)
{
	struct stat st;

	return stat(filename, &st) == 0;
}

static inline bool
fs_is_dir(const char *filename)
{
	struct stat st;

	if (stat(filename, &st) == -1) {
		return false;
	}
	return (bool)S_ISDIR(st.st_mode);
}

static inline bool
fs_is_file(const char *filename){
	struct stat st;

	if (stat(filename, &st) == -1) {
		return false;
	}

	return (bool)S_ISREG(st.st_mode);
}

static inline off_t
fs_file_size(const char *filename){
	struct stat st;

	if (stat(filename, &st) == -1) {
		return -1;
	}

	return st.st_size;
}

sds fs_file_content(const char *filename);

#if 0
static inline sds
fs_file_content(const char *filename){
    sds content;
    off_t size;
    off_t readed = 0;
    off_t n;
    int fd;

    size = fs_file_size(filename);
    if (size < 0) {
        return NULL;
    }

    fd = open(filename, O_RDONLY);
    if (fd < 0) {
        log_error("open(%s) failed: %s", filename, strerror(errno));
        return NULL;
    }

    content = sdsnewlen(NULL, size);

    while (readed < size) {
        n = read(fd, content + readed, size - readed);
        if (n < 0) {
            sdsfree(content);
            return NULL;
        }
        readed += n;
    }

    return content;
}
#endif

#endif
