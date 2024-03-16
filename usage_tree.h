/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing a tree of by-id byte usage.
 *
 */

#ifndef __USAGE_RECORD_H__
#define __USAGE_RECORD_H__

#include "config.h"

enum {
    usage_parameter_actual = 0,
    usage_parameter_size = 1,
    usage_parameter_blocks = 2,
    usage_parameter_max = 3
};

unsigned int usage_parameter_from_string(const char* param_str);
const char *usage_parameter_to_string(unsigned int param_id);

enum {
    usage_option_human_readable = 1 << 0
};

typedef const char* (*entity_id_to_name_fn)(int32_t entity_id);

typedef struct usage_record {
    int32_t     entity_id;
    uint64_t    byte_usage;
    uint64_t    inode_usage;
} usage_record_t;

typedef struct usage_tree * usage_tree_ref;

typedef enum {
    tree_by_entity_id = 0,
    tree_by_byte_usage = 1,
    tree_by_inode_usage = 2,
    tree_by_native_order = 3
} tree_order_t;

usage_tree_ref usage_tree_alloc(void);
void usage_tree_destroy(usage_tree_ref a_tree);

int usage_tree_get_size(usage_tree_ref a_tree);

usage_record_t* usage_tree_lookup(usage_tree_ref a_tree, int32_t entity_id);
usage_record_t* usage_tree_lookup_or_add(usage_tree_ref a_tree, int32_t entity_id);

bool usage_tree_update(usage_tree_ref update_this_tree, usage_tree_ref with_this_tree);

void usage_tree_sort(usage_tree_ref a_tree);

void usage_tree_calculate_totals(usage_tree_ref a_tree);
uint64_t usage_tree_get_total_bytes(usage_tree_ref a_tree);
uint64_t usage_tree_get_total_inodes(usage_tree_ref a_tree);

void usage_tree_summarize(usage_tree_ref a_tree, entity_id_to_name_fn entity_to_name, tree_order_t ordering, unsigned int options, unsigned int parameter);

#ifdef HAVE_MPI

bool usage_tree_reduce(usage_tree_ref a_tree, int root);

#endif

#endif /* __USAGE_RECORD_H__ */
