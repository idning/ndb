/*
 * file   : file.h
 * author : ning
 * date   : 2014-04-24 09:32:02
 */

#ifndef _FILE_H_
#define _FILE_H_


#include "nc_util.h"

static
inline bool file_exists(const char *filename){
	struct stat st;
	return stat(filename, &st) == 0;
}

static
inline bool is_dir(const char *filename){
	struct stat st;
	if (stat(filename, &st) == -1) {
		return false;
	}
	return (bool)S_ISDIR(st.st_mode);
}

static
inline bool is_file(const char *filename){
	struct stat st;
	if (stat(filename, &st) == -1) {
		return false;
	}
	return (bool)S_ISREG(st.st_mode);
}

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */


