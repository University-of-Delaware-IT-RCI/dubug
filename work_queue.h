/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing scan paths.
 *
 */

#ifndef __WORK_QUEUE_H__
#define __WORK_QUEUE_H__

#include "config.h"
#include "usage_tree.h"
#include "fs_path.h"
#include "sbb.h"

typedef struct work_queue * work_queue_ref;

work_queue_ref work_queue_alloc(unsigned int usage_parameter);
work_queue_ref work_queue_alloc_deserialize(sbb_ref byte_stream);

void work_queue_free(work_queue_ref wqueue);

unsigned int work_queue_get_usage_parameter(work_queue_ref wqueue);

usage_tree_ref work_queue_get_by_uid_usage_tree(work_queue_ref wqueue);
usage_tree_ref work_queue_get_by_gid_usage_tree(work_queue_ref wqueue);

unsigned int work_queue_get_path_count(work_queue_ref wqueue);
fs_path_ref work_queue_get_path_at_index(work_queue_ref wqueue, unsigned int path_idx);
const fs_path_ref* work_queue_get_paths(work_queue_ref wqueue);

void work_queue_delete(work_queue_ref wqueue, unsigned int path_idx, unsigned int path_count);

<<<<<<< HEAD
typedef bool (*work_queue_filter_fn)(unsigned int idx, fs_path_ref path, const void *context);

void work_queue_filter(work_queue_ref wqueue, work_queue_filter_fn filter_fn, const void *context);

enum {
    work_queue_build_by_path_count = 0,
    work_queue_build_by_path_depth
};

bool work_queue_build(work_queue_ref wqueue, fs_path_ref root, unsigned int build_by, unsigned int build_by_min_count);
bool work_queue_complete(work_queue_ref wqueue);

void work_queue_randomize(work_queue_ref wqueue, unsigned int num_passes);

sbb_ref work_queue_serialize(work_queue_ref wqueue);
sbb_ref work_queue_serialize_range(work_queue_ref wqueue, unsigned int path_idx, unsigned int path_count);
sbb_ref work_queue_serialize_index_and_stride(work_queue_ref wqueue, unsigned int path_idx, unsigned int path_stride, unsigned int *path_count);

const char* work_queue_to_csv_string(work_queue_ref wqueue);
=======
bool work_queue_build(work_queue_ref wqueue, fs_path_ref root, unsigned int count_min);
bool work_queue_complete(work_queue_ref wqueue);

sbb_ref work_queue_serialize(work_queue_ref wqueue);
sbb_ref work_queue_serialize_range(work_queue_ref wqueue, unsigned int path_idx, unsigned int path_count);
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70

void work_queue_summary(work_queue_ref wqueue);

#endif /* __WORK_QUEUE_H__ */
