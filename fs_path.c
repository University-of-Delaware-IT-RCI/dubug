/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing file system paths.
 *
 */

#include "fs_path.h"
#include "sbb.h"

//

#define A_PATH_IN       ((sbb_ref)a_path)
#define A_PATH_OUT(X)   ((fs_path_ref)(X))

//

fs_path_ref
fs_path_alloc(
    size_t  capacity
)
{
    return A_PATH_OUT(sbb_alloc(capacity + 1, 0));
}

//

fs_path_ref
fs_path_alloc_with_cstring(
    size_t      capacity,
    const char  *cstr
)
{
    size_t      cstr_len = strlen(cstr) + 1;
    sbb_ref     new_obj = sbb_alloc_with_bytes(capacity, 0, cstr, cstr_len);
    
    return A_PATH_OUT(new_obj);
}

//

fs_path_ref
fs_path_alloc_with_cstring_const(
    const char  *cstr_const
)
{
    size_t      cstr_const_len = strlen(cstr_const) + 1;
    sbb_ref     new_obj = sbb_alloc_with_const_buffer(0, cstr_const, cstr_const_len);
    
    return A_PATH_OUT(new_obj);
}

//

fs_path_ref
fs_path_alloc_with_characters(
    size_t      capacity,
    const char  *p,
    size_t      p_len
)
{
    sbb_ref     new_obj = sbb_alloc_with_bytes(capacity, 0, p, p_len);
    
    if ( new_obj ) {
        if ( ! sbb_append_uint8(new_obj, 0) ) {
            sbb_free(new_obj);
            new_obj = NULL;
        }
    }
    return A_PATH_OUT(new_obj);
}

//

fs_path_ref
fs_path_alloc_with_cstring_relative_to(
    fs_path_ref base_path,
    const char  *cstr
)
{
    return fs_path_alloc_with_characters_relative_to(base_path, cstr, strlen(cstr));
}

//

fs_path_ref
fs_path_alloc_with_characters_relative_to(
    fs_path_ref base_path,
    const char  *p,
    size_t      p_len
)
{
    size_t      base_path_len = fs_path_get_length(base_path);
    size_t      total_len = base_path_len + 1 + p_len + 1;
    fs_path_ref a_path;
    bool        base_ends_with_slash, p_starts_with_slash, add_slash;
    const char  *base_cstr = fs_path_get_cstring(base_path);
    
    base_ends_with_slash = ( (base_path_len > 0) && (base_cstr[base_path_len - 1] == '/') );
    p_starts_with_slash = ( (p_len > 0) && (*p == '/') );
    add_slash = ! (base_ends_with_slash || p_starts_with_slash);
    if ( ! add_slash ) total_len--;
    
    a_path = fs_path_alloc(total_len);
    if ( a_path ) {
        sbb_append_buffer(A_PATH_IN, base_cstr, base_path_len);
        if ( add_slash ) sbb_append_uint8(A_PATH_IN, '/');
        sbb_append_buffer(A_PATH_IN, p, p_len);
        sbb_append_uint8(A_PATH_IN, 0);
    }
    return a_path;
}

//

fs_path_ref
fs_path_alloc_with_path(
    fs_path_ref other_path
)
{
    size_t      other_path_len = fs_path_get_length(other_path);
    fs_path_ref a_path = fs_path_alloc(other_path_len);
    
    if ( a_path ) {
        if ( other_path_len > 0 ) {
            sbb_append_buffer(A_PATH_IN, fs_path_get_cstring(other_path), other_path_len);
            sbb_append_uint8(A_PATH_IN, 0);
        }
    }
    return a_path;
}

//

void
fs_path_free(
    fs_path_ref a_path
)
{
    //fprintf(stderr, "fs_path_free %p\n", sbb_get_buffer_ptr(A_PATH_IN));
    sbb_free(A_PATH_IN);
}

//

size_t
fs_path_get_capacity(
    fs_path_ref a_path
)
{
    return sbb_get_capacity(A_PATH_IN);
}

//

size_t
fs_path_get_length(
    fs_path_ref a_path
)
{
    size_t      l = sbb_get_length(A_PATH_IN);
    
    if ( l > 0 ) l--;
    return l;
}

//

const char*
fs_path_get_cstring(
    fs_path_ref a_path
)
{
    return (const char*)sbb_get_buffer_ptr(A_PATH_IN);
}

//

const char*
fs_path_duplicate_cstring(
    fs_path_ref a_path
)
{
    size_t      l = fs_path_get_length(a_path);
    char        *path = malloc(l + 1);
    
    if ( path ) {
        memcpy(path, fs_path_get_cstring(sbb_get_buffer_ptr), l);
        path[l] = '\0';
    }
    return (const char*)path;
}

//

const char*
fs_path_copy(
    fs_path_ref a_path,
    const char  *p,
    size_t      p_capacity
)
{
    size_t      cpy_len = fs_path_get_length(a_path);
    char        *P = (char*)p;
    
    if ( cpy_len > p_capacity ) cpy_len = p_capacity;
    if ( cpy_len ) {
        memcpy(P, fs_path_get_cstring(a_path), cpy_len);
        P += cpy_len;
    }
    if ( cpy_len < p_capacity ) *P++ = '\0';
    return (const char*)P;
}

//

bool
fs_path_push_cstring(
    fs_path_ref a_path,
    const char  *cstr
)
{
    return fs_path_push_characters(a_path, cstr, strlen(cstr));
}

//

bool
fs_path_push_characters(
    fs_path_ref a_path,
    const char  *p,
    size_t      p_len
)
{
    // We're going to append a slash followed by the incoming characters
    size_t      cur_length = sbb_get_length(A_PATH_IN);
    const char  *path = fs_path_get_cstring(a_path);
    
    // If the current path ends with a slash (or is empty) and the incoming
    // characters start with a slash, adjust accordingly:
    if ( (cur_length <= 1) || (path[cur_length - 1] == '/') ) {
        if ( *p == '/' ) p++, p_len--;
    }
    
    // Drop back onto the NUL character:
    if ( cur_length > 0 ) sbb_set_length(A_PATH_IN, cur_length - 1, 0);
    
    // Push the byte strings:
    if ( ! sbb_append_multiple_buffers(A_PATH_IN, "/", (size_t)1, p, (size_t)p_len, "\0", (size_t)1, NULL, 0) ) return false;
    
    return true;
}

//

bool
fs_path_pop(
    fs_path_ref a_path
)
{
    size_t      cur_length = fs_path_get_length(a_path);
    bool        rc = false;
    
    if ( cur_length > 0 ) {
        size_t      path_len = cur_length;
        const char  *path = fs_path_get_cstring(a_path);
        const char  *ppath = path + path_len;
        
        if ( (cur_length == 1) && (*path == '/') ) return false;
        
        rc = true;
        while ( path_len-- > 0 ) {
            if ( *(--ppath) == '/' ) {
                // Don't dump a leading slash on an absolute path:
                if ( path_len == 0 ) ppath++;
                break;
            }
        }
        if ( ppath > path ) {
            sbb_set_length(A_PATH_IN, ppath - path, 0);
        } else {
            sbb_set_length(A_PATH_IN, 0, 0);
            rc = false;
        }
        sbb_append_uint8(A_PATH_IN, 0);
    }
    return rc;
}
