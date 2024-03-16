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

bool work_queue_build(work_queue_ref wqueue, fs_path_ref root, unsigned int count_min);
bool work_queue_complete(work_queue_ref wqueue);

sbb_ref work_queue_serialize(work_queue_ref wqueue);
sbb_ref work_queue_serialize_range(work_queue_ref wqueue, unsigned int path_idx, unsigned int path_count);

void work_queue_summary(work_queue_ref wqueue);

#endif /* __WORK_QUEUE_H__ */
