/*
 * file   : file.h
 * author : ning
 * date   : 2014-04-24 09:32:02
 */

#ifndef _FILE_H_
#define _FILE_H_

#include "nc_util.h"

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

static inline bool
fs_file_size(const char *filename){
	struct stat st;

	if (stat(filename, &st) == -1) {
		return false;
	}

	return (bool)S_ISREG(st.st_mode);
}

#endif
