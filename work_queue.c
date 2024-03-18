/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing scan paths.
 *
 */

#include "work_queue.h"
#include "simple_verbose_logging.h"

#ifndef ST_NBLOCKSIZE
# ifdef S_BLKSIZE
#  define ST_NBLOCKSIZE S_BLKSIZE
# else
#  define ST_NBLOCKSIZE 512
# endif
#endif

//

typedef struct work_queue {
    usage_tree_ref  by_uid, by_gid;
    unsigned int    usage_parameter;
    unsigned int    path_count, path_capacity;
    fs_path_ref     *paths;
} work_queue_t;

//

#ifndef WORK_QUEUE_PATH_GROW
#   define WORK_QUEUE_PATH_GROW 8
#endif

bool
__work_queue_grow(
    work_queue_t    *wqueue,
    unsigned int    capacity
)
{
    unsigned int    new_capacity = (capacity > 0) ? capacity : (wqueue->path_capacity + WORK_QUEUE_PATH_GROW);
    fs_path_ref     *new_paths = (fs_path_ref*)realloc(wqueue->paths, new_capacity * sizeof(fs_path_ref));
    
    if ( new_paths ) {
        wqueue->path_capacity = new_capacity;
        wqueue->paths = new_paths;
        return true;
    }
    return false;
}

//

fs_path_ref
__work_queue_dequeue(
    work_queue_t    *wqueue
)
{
    fs_path_ref     out_path = NULL;
    
    if ( wqueue->path_count ) {
        out_path = wqueue->paths[0];
        wqueue->path_count--;
        if ( wqueue->path_count ) memmove(&wqueue->paths[0], &wqueue->paths[1], wqueue->path_count * sizeof(fs_path_ref));
    }
    return out_path;
}

//

fs_path_ref
__work_queue_pop(
    work_queue_t    *wqueue
)
{
    if ( wqueue->path_count ) return wqueue->paths[--wqueue->path_count];
    return NULL;
}

//

bool
__work_queue_push(
    work_queue_t    *wqueue,
    fs_path_ref     new_path,
    bool            should_copy
)
{
    if ( (wqueue->path_count == wqueue->path_capacity) && ! __work_queue_grow(wqueue, 0) ) return false;
    wqueue->paths[wqueue->path_count++] = should_copy ? fs_path_alloc_with_path(new_path) : new_path;
    return true;
}

//

work_queue_ref
work_queue_alloc(
    unsigned int    usage_parameter
)
{
    work_queue_t    *new_queue = malloc(sizeof(work_queue_t));
    
    if ( new_queue ) {
        memset(new_queue, 0, sizeof(work_queue_t));
        new_queue->by_uid = usage_tree_alloc();
        new_queue->by_gid = usage_tree_alloc();
        new_queue->usage_parameter = usage_parameter;
    }
    return (work_queue_ref)new_queue;
}

//

work_queue_ref
work_queue_alloc_deserialize(
    sbb_ref     byte_stream
)
{
    work_queue_t    *new_queue = NULL;
    int             path_idx = 0;
    uint32_t        path_count, usage_parameter;
    
    SBB_DESERIALIZE_BEGIN(byte_stream)
        SBB_DESERIALIZE_UINT32(byte_stream, usage_parameter)
        SBB_DESERIALIZE_UINT32(byte_stream, path_count)
        new_queue = (work_queue_t*)work_queue_alloc((unsigned int)usage_parameter);
        if ( path_count > 0 ) {
            if ( ! __work_queue_grow(new_queue, path_count) ) break;
            
            // Begin reading-in each {path_len, path_cstr} tuple:
            while ( path_idx < path_count ) {
                uint64_t    path_len;
                char        *cstr;
                
                SBB_DESERIALIZE_UINT64(byte_stream, path_len)
                new_queue->paths[path_idx] = fs_path_alloc(path_len);
                if ( ! new_queue->paths[path_idx] ) {
                    svl_printf(verbosity_error, "Unable to allocate path %u in work queue deserialize", path_idx);
                    break;
                }
                cstr = (char*)fs_path_get_cstring(new_queue->paths[path_idx]);
                SBB_DESERIALIZE_BUFFER(byte_stream, cstr, path_len)
                cstr[path_len] = '\0';
                new_queue->path_count++;
                path_idx++;
            }
            // If we didn't finish reading paths there 
            if ( path_idx < path_count ) break;
        }
    SBB_DESERIALIZE_ON_ERROR(byte_stream)
        switch ( __byte_stream_field ) {
            case 0:
                svl_printf(verbosity_error, "Unable to decode usage parameter on work queue deserialize"); break;
            case 1:
                svl_printf(verbosity_error, "Unable to decode path count on work queue deserialize"); break;
            case 2:
                svl_printf(verbosity_error, "Unable to decode path count on work queue deserialize"); break;
            default:
                break;
        }
    SBB_DESERIALIZE_OTHERWISE(byte_stream)
    
    SBB_DESERIALIZE_END(byte_stream)
    
    return (work_queue_ref)new_queue;
}

//

void
work_queue_free(
    work_queue_ref  wqueue
)
{
    if ( wqueue ) {
        if ( wqueue->paths ) {
            while ( wqueue->path_count-- > 0 ) {
                fs_path_free(wqueue->paths[wqueue->path_count]);
            }
            free(wqueue->paths);
        }
        free(wqueue);
    }
}

//

unsigned int
work_queue_get_usage_parameter(
    work_queue_ref  wqueue
)
{
    return wqueue->usage_parameter;
}

//

usage_tree_ref
work_queue_get_by_uid_usage_tree(
    work_queue_ref  wqueue
)
{
    return wqueue->by_uid;
}

//

usage_tree_ref
work_queue_get_by_gid_usage_tree(
    work_queue_ref  wqueue
)
{
    return wqueue->by_gid;
}

//

unsigned int
work_queue_get_path_count(
    work_queue_ref  wqueue
)
{
    return wqueue->path_count;
}

//

fs_path_ref
work_queue_get_path_at_index(
    work_queue_ref  wqueue,
    unsigned int    path_idx
)
{
    if ( path_idx < wqueue->path_count ) return wqueue->paths[path_idx];
    return NULL;
}

//

const fs_path_ref*
work_queue_get_paths(
    work_queue_ref  wqueue
)
{
    return (const fs_path_ref*)wqueue->paths;
}

//

void
work_queue_delete(
    work_queue_ref  wqueue,
    unsigned int    path_idx,
    unsigned int    path_count
)
{
    if ( path_idx < wqueue->path_count ) {
        unsigned int    idx, idx_max;
        
        if ( path_idx + path_count > wqueue->path_count ) path_count = wqueue->path_count - path_idx;
        idx = path_idx, idx_max = path_idx + path_count;
        while ( idx < idx_max ) fs_path_free(wqueue->paths[idx++]);
        if ( idx_max < wqueue->path_count ) {
            memmove(&wqueue->paths[path_idx], &wqueue->paths[idx_max], (wqueue->path_count - idx_max) * sizeof(fs_path_ref));
        }
        wqueue->path_count -= path_count;
    }
}

//

void
work_queue_filter(
    work_queue_ref          wqueue,
    work_queue_filter_fn    filter_fn,
    const void              *context
)
{
    if ( wqueue->path_count > 0 ) {
        fs_path_ref         *target_list = wqueue->paths;
        fs_path_ref         *src_list = wqueue->paths;
        unsigned int        idx = 0, idx_max = wqueue->path_count;
        
        while ( idx < idx_max ) {
            if ( filter_fn(idx, *src_list, context) ) {
                if ( target_list != src_list ) *target_list = *src_list;
                target_list++;
            } else {
                // Remove the path:
                fs_path_free(*src_list);
                wqueue->path_count--;
            }
            idx++;
            src_list++;
        }
    }
}

//

bool
work_queue_build(
    work_queue_ref  wqueue,
    fs_path_ref     root,
    unsigned int    build_by,
    unsigned int    build_by_min_count
)
{
    bool            is_okay = true;
    
    switch ( build_by ) {
        case work_queue_build_by_path_count: {
            //
            // Add paths until the work queue is longer than build_by_min_count.
            //
            if ( wqueue->path_count < build_by_min_count ) {
                // Add the root path to the queue:
                if ( __work_queue_push(wqueue, root, true) ) {
                    // Continue running while the queue is too small:
                    while ( is_okay && wqueue->path_count && (wqueue->path_count < build_by_min_count) ) {
                        // We're going to expand each queued path into sub-paths by doing a non-recursive
                        // walk of the items therein.  Once we dequeue a path _we_ own it and must
                        // dispose of it:
                        fs_path_ref alt_root = __work_queue_dequeue(wqueue);
                        const char* fts_paths[2] = { fs_path_get_cstring(alt_root), NULL };
                        FTS         *fts_ptr = fts_open((char* const*)fts_paths, FTS_NOCHDIR | FTS_PHYSICAL, NULL);
                
                        if ( fts_ptr ) {
                            // Great, we got a handle.  Start walking:
                            FTSENT          *fts_item = fts_read(fts_ptr);
                    
                            // First item should be the root directory itself:
                            if ( fts_item ) {
                                uint64_t        size;
                                usage_record_t  *r;
                        
                                // At this point we account for the directory itself (bytes and inodes):
                                switch ( wqueue->usage_parameter ) {
                                    case usage_parameter_actual:
                                        size = fts_item->fts_statp->st_blocks * ST_NBLOCKSIZE;
                                        break;
                                    case usage_parameter_size:
                                        size = fts_item->fts_statp->st_size;
                                        break;
                                    case usage_parameter_blocks:
                                        size = fts_item->fts_statp->st_blocks;
                                        break;
                                }
                                r = usage_tree_lookup_or_add(wqueue->by_uid, fts_item->fts_statp->st_uid);
                                if ( r ) r->byte_usage += size, r->inode_usage++;
                                r = usage_tree_lookup_or_add(wqueue->by_gid, fts_item->fts_statp->st_gid);
                                if ( r ) r->byte_usage += size, r->inode_usage++;
                        
                                while ( is_okay && (fts_item = fts_read(fts_ptr)) ) {
                                    // Add item metadata to the usage trees:
                                    struct stat     *finfo = fts_item->fts_statp;
                        
                                    switch ( fts_item->fts_info ) {
                            
                                        case FTS_D: {
                                            // Do not descend into this directory, just add it to the work queue:
                                            fs_path_ref new_dir = fs_path_alloc_with_characters(fts_item->fts_pathlen, fts_item->fts_path, fts_item->fts_pathlen);
                                            if ( new_dir ) {
                                                svl_printf(verbosity_debug, "%s", fts_item->fts_path);
                                                if ( __work_queue_push(wqueue, new_dir, false) ) {
                                                    fts_set(fts_ptr, fts_item, FTS_SKIP);
                                                } else {
                                                    svl_printf(verbosity_warning, "Unable to push path '%s' to work queue during fts scan", fts_item->fts_path);
                                                    is_okay = false;
                                                }
                                            } else {
                                                svl_printf(verbosity_warning, "Unable to allocate path '%s' during fts scan", fts_item->fts_path);
                                                is_okay = false;
                                            }
                                            break;
                                        }
                                        case FTS_DEFAULT:
                                        case FTS_F:
                                        case FTS_SL:
                                        case FTS_SLNONE: {
                                            switch ( wqueue->usage_parameter ) {
                                                case usage_parameter_actual:
                                                    size = finfo->st_blocks * ST_NBLOCKSIZE;
                                                    break;
                                                case usage_parameter_size:
                                                    size = finfo->st_size;
                                                    break;
                                                case usage_parameter_blocks:
                                                    size = finfo->st_blocks;
                                                    break;
                                            }
                                            r = usage_tree_lookup_or_add(wqueue->by_uid, finfo->st_uid);
                                            if ( r ) r->byte_usage += size, r->inode_usage++;
                                            r = usage_tree_lookup_or_add(wqueue->by_gid, finfo->st_gid);
                                            if ( r ) r->byte_usage += size, r->inode_usage++;
                                            break;
                                        }
                                    }
                                }
                            }
                            fts_close(fts_ptr);
                        } else {
                            svl_printf(verbosity_warning, "Unable to open %s for fts scan (errno = %d)", fts_paths[0], errno);
                            is_okay = false;
                        }
                        fs_path_free(alt_root);
                    }
                }
            }
            break;
        }
        case work_queue_build_by_path_depth: {
            //
            // Add paths until the directory tree has been walked to the specified depth.
            //
            unsigned int        cur_depth = 0;
            
            // Add the root path to the queue:
            if ( __work_queue_push(wqueue, root, true) ) {
                // Continue running while the depth hasn't been reached:
                while ( is_okay && wqueue->path_count && (cur_depth < build_by_min_count) ) {
                    // We're going to expand each queued path into sub-paths by doing a non-recursive
                    // walk of the items therein.  Once we dequeue a path _we_ own it and must
                    // dispose of it:
                    fs_path_ref         copy_of_paths[wqueue->path_count];
                    unsigned int        path_idx = 0, path_count = wqueue->path_count;
                    
                    memcpy(copy_of_paths, wqueue->paths, path_count * sizeof(fs_path_ref));
                    wqueue->path_count = 0;
                    
                    while ( is_okay && (path_idx < path_count) ) {
                        fs_path_ref     alt_root = copy_of_paths[path_idx];
                        const char*     fts_paths[2] = { fs_path_get_cstring(alt_root), NULL };
                        FTS             *fts_ptr = fts_open((char* const*)fts_paths, FTS_NOCHDIR | FTS_PHYSICAL, NULL);
                        
                        if ( fts_ptr ) {
                            // Great, we got a handle.  Start walking:
                            FTSENT          *fts_item = fts_read(fts_ptr);
                
                            // First item should be the root directory itself:
                            if ( fts_item ) {
                                uint64_t        size;
                                usage_record_t  *r;
                    
                                // At this point we account for the directory itself (bytes and inodes):
                                switch ( wqueue->usage_parameter ) {
                                    case usage_parameter_actual:
                                        size = fts_item->fts_statp->st_blocks * ST_NBLOCKSIZE;
                                        break;
                                    case usage_parameter_size:
                                        size = fts_item->fts_statp->st_size;
                                        break;
                                    case usage_parameter_blocks:
                                        size = fts_item->fts_statp->st_blocks;
                                        break;
                                }
                                r = usage_tree_lookup_or_add(wqueue->by_uid, fts_item->fts_statp->st_uid);
                                if ( r ) r->byte_usage += size, r->inode_usage++;
                                r = usage_tree_lookup_or_add(wqueue->by_gid, fts_item->fts_statp->st_gid);
                                if ( r ) r->byte_usage += size, r->inode_usage++;
                    
                                while ( is_okay && (fts_item = fts_read(fts_ptr)) ) {
                                    // Add item metadata to the usage trees:
                                    struct stat     *finfo = fts_item->fts_statp;
                    
                                    switch ( fts_item->fts_info ) {
                        
                                        case FTS_D: {
                                            // Do not descend into this directory, just add it to the work queue:
                                            fs_path_ref new_dir = fs_path_alloc_with_characters(fts_item->fts_pathlen, fts_item->fts_path, fts_item->fts_pathlen);
                                            if ( new_dir ) {
                                                svl_printf(verbosity_debug, "%s", fts_item->fts_path);
                                                if ( __work_queue_push(wqueue, new_dir, false) ) {
                                                    fts_set(fts_ptr, fts_item, FTS_SKIP);
                                                } else {
                                                    svl_printf(verbosity_warning, "Unable to push path '%s' to work queue during fts scan", fts_item->fts_path);
                                                    is_okay = false;
                                                }
                                            } else {
                                                svl_printf(verbosity_warning, "Unable to allocate path '%s' during fts scan", fts_item->fts_path);
                                                is_okay = false;
                                            }
                                            break;
                                        }
                                        case FTS_DEFAULT:
                                        case FTS_F:
                                        case FTS_SL:
                                        case FTS_SLNONE: {
                                            switch ( wqueue->usage_parameter ) {
                                                case usage_parameter_actual:
                                                    size = finfo->st_blocks * ST_NBLOCKSIZE;
                                                    break;
                                                case usage_parameter_size:
                                                    size = finfo->st_size;
                                                    break;
                                                case usage_parameter_blocks:
                                                    size = finfo->st_blocks;
                                                    break;
                                            }
                                            r = usage_tree_lookup_or_add(wqueue->by_uid, finfo->st_uid);
                                            if ( r ) r->byte_usage += size, r->inode_usage++;
                                            r = usage_tree_lookup_or_add(wqueue->by_gid, finfo->st_gid);
                                            if ( r ) r->byte_usage += size, r->inode_usage++;
                                            break;
                                        }
                                    }
                                }
                            }
                            fts_close(fts_ptr);
                        } else {
                            svl_printf(verbosity_warning, "Unable to open %s for fts scan (errno = %d)", fts_paths[0], errno);
                            is_okay = false;
                        }
                        fs_path_free(alt_root);
                        path_idx++;
                    }
                    while (path_idx < path_count) fs_path_free(copy_of_paths[path_idx++]);
                    
                    // Increment depth:
                    cur_depth++;
                }
            }
            break;
        }
        default: {
            svl_printf(verbosity_error, "Unknown work queue build algorithm %u", build_by);
            is_okay = false;

        }
    }
    return is_okay;
}

//

bool
work_queue_complete(
    work_queue_ref  wqueue
)
{
    bool    is_okay = true;

    if ( wqueue->path_count ) {
        const char*     fts_paths[wqueue->path_count + 1];
        unsigned int    path_idx = 0;
        FTS             *fts_ptr;
        fs_path_ref     a_path;
        
        while ( path_idx < wqueue->path_count ) {
            fts_paths[path_idx] = fs_path_get_cstring(wqueue->paths[path_idx]);
            path_idx++;
        }
        fts_paths[path_idx] = NULL;
        
        fts_ptr = fts_open((char* const*)fts_paths, FTS_NOCHDIR | FTS_PHYSICAL, NULL);
        if ( fts_ptr ) {
            // Great, we got a handle.  Start walking:
            FTSENT          *fts_item = fts_read(fts_ptr);
            uint64_t        size;
            usage_record_t  *r;
            
            if ( ! fts_item ) {
                svl_printf(verbosity_error, "No paths enumerated in fts walk");
                is_okay = false;
            }
            while ( fts_item ) {
                // Add item metadata to the usage trees:
                struct stat     *finfo = fts_item->fts_statp;
                
                switch ( fts_item->fts_info ) {
                    case FTS_DNR:
                        svl_printf(verbosity_error, "Directory '%s' unreadable (fts_errno = %d)", fts_item->fts_path, fts_item->fts_errno);
                        is_okay = false;
                        break;
                    case FTS_NS:
                        svl_printf(verbosity_error, "Unable to stat '%s' (fts_errno = %d)", fts_item->fts_path, fts_item->fts_errno);
                        is_okay = false;
                        break;
                    case FTS_ERR:
                        svl_printf(verbosity_error, "Generic fts error on '%s' (fts_errno = %d)", fts_item->fts_path, fts_item->fts_errno);
                        is_okay = false;
                        break;
                    case FTS_D:                        
                        svl_printf(verbosity_debug, "%s", fts_item->fts_path);
                    case FTS_DEFAULT:
                    case FTS_F:
                    case FTS_SL:
                    case FTS_SLNONE: {
                        switch ( wqueue->usage_parameter ) {
                            case usage_parameter_actual:
                                size = finfo->st_blocks * ST_NBLOCKSIZE;
                                break;
                            case usage_parameter_size:
                                size = finfo->st_size;
                                break;
                            case usage_parameter_blocks:
                                size = finfo->st_blocks;
                                break;
                        }
                        r = usage_tree_lookup_or_add(wqueue->by_uid, finfo->st_uid);
                        if ( r ) r->byte_usage += size, r->inode_usage++;
                        r = usage_tree_lookup_or_add(wqueue->by_gid, finfo->st_gid);
                        if ( r ) r->byte_usage += size, r->inode_usage++;
                        break;
                    }
                }
                fts_item = fts_read(fts_ptr);
            }
            fts_close(fts_ptr);
        } else {
            svl_printf(verbosity_error, "Unable to open %s for fts scan (errno = %d)", fts_paths[0], errno);
            is_okay = false;
        }
        
        while ( a_path = __work_queue_pop(wqueue) ) fs_path_free(a_path);
    }
    return is_okay;
}

//

unsigned int
__work_queue_random_idx(
    unsigned int    idx_max
)
{
    static bool     is_inited = false;
    static char     state_array[512];
    
    if ( ! is_inited ) {
        int         urandom_fd = open("/dev/urandom", 0);
        
        if ( urandom_fd >= 0 ) {
            unsigned int    rnd_seed;
            
            if ( read(urandom_fd, &rnd_seed, sizeof(rnd_seed)) >= sizeof(rnd_seed) ) {
                initstate(rnd_seed, state_array, sizeof(state_array));
                is_inited = true;
            }
            close(urandom_fd);
        }
        if ( ! is_inited ) {
            svl_printf(verbosity_critical, "Failed to initialized random number generator");
            exit(1);
        }
    }
    return random() % idx_max;
}

void
work_queue_randomize(
    work_queue_ref  wqueue,
    unsigned int    num_passes
)
{
    while ( num_passes-- > 0 ) {
        unsigned int    idx_max = wqueue->path_count;
    
        //
        // Starting with the full length of the path list, choose a random index
        // and move the item at that index to the end of the list; shrink the index range
        // by one and repeat until the index range as been exhausted.
        //
        while ( idx_max > 1 ) {
            unsigned int    idx = __work_queue_random_idx(idx_max);
            unsigned int    path_count = wqueue->path_count - idx - 1;
            fs_path_ref     path_at_idx = wqueue->paths[idx];
        
            if ( path_count > 0 ) memmove(&wqueue->paths[idx], &wqueue->paths[idx+1], path_count * sizeof(fs_path_ref));
            wqueue->paths[wqueue->path_count - 1] = path_at_idx;
            idx_max--;
        }
    }
}

//

sbb_ref
work_queue_serialize(
    work_queue_ref  wqueue
)
{
    return work_queue_serialize_range(wqueue, 0, wqueue->path_count);
}

//

sbb_ref
work_queue_serialize_range(
    work_queue_ref  wqueue,
    unsigned int    path_idx,
    unsigned int    path_count
)
{
    sbb_ref         byte_stream = NULL;
    
    if ( path_idx < wqueue->path_count ) {
        unsigned int    path_idx_end = path_idx + path_count;
        
        if ( path_idx_end > wqueue->path_count ) {
            path_idx_end = wqueue->path_count;
            path_count = path_idx_end - path_idx;
        }
        byte_stream = sbb_alloc(sizeof(uint32_t), sbb_options_byte_swap);
        if ( byte_stream ) {
            bool        is_okay = false;
            
            if ( sbb_append_uint32(byte_stream, (uint32_t)wqueue->usage_parameter) && sbb_append_uint32(byte_stream, (uint32_t)path_count) ) {
                while ( path_idx < path_idx_end ) {
                    uint64_t        path_len = fs_path_get_length(wqueue->paths[path_idx]);
                    
                    if ( ! sbb_append_uint64(byte_stream, path_len) ) break;
                    if ( ! sbb_append_buffer(byte_stream, fs_path_get_cstring(wqueue->paths[path_idx]), path_len) ) break;
                    path_idx++;
                }
                is_okay = (path_idx >= path_idx_end);
            }
            if ( ! is_okay ) {
                sbb_free(byte_stream);
                byte_stream = NULL;
            }
        }
    }
    return byte_stream;
}

//

sbb_ref
work_queue_serialize_index_and_stride(
    work_queue_ref  wqueue,
    unsigned int    path_idx,
    unsigned int    path_stride,
    unsigned int    *path_count
)
{
    sbb_ref         byte_stream = NULL;
    
    if ( path_idx < wqueue->path_count ) {
        *path_count = (wqueue->path_count - path_idx) / path_stride;
        
        byte_stream = sbb_alloc(sizeof(uint32_t), sbb_options_byte_swap);
        if ( byte_stream ) {
            bool        is_okay = false;
            
            if ( sbb_append_uint32(byte_stream, (uint32_t)wqueue->usage_parameter) && sbb_append_uint32(byte_stream, (uint32_t)*path_count) ) {
                unsigned int    paths_remaining = *path_count;
                
                while ( paths_remaining ) {
                    uint64_t        path_len = fs_path_get_length(wqueue->paths[path_idx]);
                    
                    if ( ! sbb_append_uint64(byte_stream, path_len) ) break;
                    if ( ! sbb_append_buffer(byte_stream, fs_path_get_cstring(wqueue->paths[path_idx]), path_len) ) break;
                    path_idx += path_stride;
                    paths_remaining--;
                }
                is_okay = (paths_remaining <= 0);
            }
            if ( ! is_okay ) {
                sbb_free(byte_stream);
                byte_stream = NULL;
            }
        }
    }
    return byte_stream;
}

//

const char*
work_queue_to_csv_string(
    work_queue_ref  wqueue
)
{
    size_t          wqstr_len = 0;
    unsigned int    idx = 0;
    
    while ( idx < wqueue->path_count ) {
        wqstr_len += snprintf(NULL, 0,
                            "%s%s",
                            fs_path_get_cstring(wqueue->paths[idx]),
                            (idx < (wqueue->path_count - 1)) ? ", " : ""
                        );
        idx++;
    }
    if ( wqstr_len > 0 ) {
        char        *outstr = malloc(1 + wqstr_len);
        
        if ( outstr ) {
            char    *outstr_ptr = outstr, *outstr_end = outstr + wqstr_len + 1;
            
            idx = 0;
            while ( (idx < wqueue->path_count) && (outstr_ptr < outstr_end) ) {
                outstr_ptr += snprintf(outstr_ptr, (outstr_end - outstr_ptr),
                                "%s%s",
                                fs_path_get_cstring(wqueue->paths[idx]),
                                (idx < (wqueue->path_count - 1)) ? ", " : ""
                            );
                idx++;
            }
        }
        return (const char*)outstr;
    }
    return NULL;
}

//

void
work_queue_summary(
    work_queue_ref  wqueue
)
{
    unsigned int    i = 0;
    
    printf("work_queue@%p <usage_parameter=%u,path_count=%u,path_capacity=%u> {\n",
            wqueue, wqueue->usage_parameter, wqueue->path_count, wqueue->path_capacity
        );
    while ( i < wqueue->path_count ) {
        printf("%8u: \"%s\"%s",
                i, fs_path_get_cstring(wqueue->paths[i]),
                (i+1 < wqueue->path_count) ? ",\n" : "\n"
            );
        i++;
    }
    printf("}\n");
}
