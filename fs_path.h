/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing file system paths.
 *
 */

#ifndef __FS_PATH_H__
#define __FS_PATH_H__

#include "config.h"

typedef const void * fs_path_ref;

fs_path_ref fs_path_alloc(size_t capacity);
fs_path_ref fs_path_alloc_with_cstring(size_t capacity, const char *cstr);
fs_path_ref fs_path_alloc_with_cstring_const(const char *cstr_const);
fs_path_ref fs_path_alloc_with_characters(size_t capacity, const char *p, size_t p_len);
fs_path_ref fs_path_alloc_with_cstring_relative_to(fs_path_ref base_path, const char *cstr);
fs_path_ref fs_path_alloc_with_characters_relative_to(fs_path_ref base_path, const char *p, size_t p_len);
fs_path_ref fs_path_alloc_with_path(fs_path_ref a_path);

void fs_path_free(fs_path_ref a_path);

size_t fs_path_get_capacity(fs_path_ref a_path);
size_t fs_path_get_length(fs_path_ref a_path);
const char* fs_path_get_cstring(fs_path_ref a_path);

const char* fs_path_duplicate_cstring(fs_path_ref a_path);
const char* fs_path_copy(fs_path_ref a_path, const char *p, size_t p_capacity);

bool fs_path_push_cstring(fs_path_ref a_path, const char *cstr);
bool fs_path_push_characters(fs_path_ref a_path, const char *p, size_t p_len);
bool fs_path_pop(fs_path_ref a_path);

#endif /* __FS_PATH_H__ */
