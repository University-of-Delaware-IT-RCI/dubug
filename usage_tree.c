/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * API for managing a tree of by-id byte usage.
 *
 */

#include "usage_tree.h"
#include "byte_count_to_string.h"
#include "simple_verbose_logging.h"

static const char* usage_parameter_names[] = {
    "actual",
    "st_size",
    "st_blocks",
    NULL
};

//

typedef struct usage_record_priv {
    usage_record_t  usage;
    
    struct usage_record_priv *left[2], *middle[2], *right[2];
    struct usage_record_priv *list;
} usage_record_priv_t;

typedef struct usage_tree {
    usage_record_priv_t     *as_list;
    usage_record_priv_t     *by_entity_id_root;
    usage_record_priv_t     *by_byte_usage_root;
    usage_record_priv_t     *by_inode_usage_root;
    
    uint64_t                total_bytes;
    uint64_t                total_inodes;
} usage_tree_t;

//

unsigned int
usage_parameter_from_string(
    const char*     param_str
)
{
    const char*     *P = usage_parameter_names;
    unsigned int    i = usage_parameter_actual;
    
    while ( *P ) {
        if ( strcasecmp(param_str, *P) == 0 ) break;
        i++;
        P++;
    }
    return i;
}

const char*
usage_parameter_to_string(
    unsigned int    param_id
)
{
    if ( param_id >= usage_parameter_actual && param_id < usage_parameter_max ) return usage_parameter_names[param_id];
    return NULL;
}

//

usage_tree_ref 
usage_tree_alloc(void)
{
    usage_tree_t    *new_tree = (usage_tree_t*)malloc(sizeof(usage_tree_t));

    if ( ! new_tree ) {
        svl_printf(verbosity_error, "Unable to allocate new usage tree");
        exit(ENOMEM);
    }
    memset(new_tree, 0, sizeof(*new_tree));
    return (usage_tree_ref)new_tree;
}

//

void
usage_tree_destroy(
    usage_tree_ref  a_tree
)
{
    // Walk the as_list and remove records:
    usage_record_priv_t  *r = a_tree->as_list;

    while ( r ) {
        usage_record_priv_t  *next = r->list;\
        free((void*)r);
        r = next;
    }

    // Remove the tree itself:
    free((void*)a_tree);
}

//

int
usage_tree_get_size(
    usage_tree_ref  a_tree
)
{
    usage_record_priv_t *r = a_tree->as_list;
    int                 tree_size = 0;
    
    while ( r ) tree_size++, r = r->list;
    return tree_size;
}

//

usage_record_priv_t*
__usage_record_create(
    int32_t         entity_id
)
{
    usage_record_priv_t *new_record = (usage_record_priv_t*)malloc(sizeof(usage_record_priv_t));

    if ( ! new_record ) {
        svl_printf(verbosity_error, "Unable to allocate new usage record");
        exit(ENOMEM);
    }
    memset(new_record, 0, sizeof(*new_record));
    new_record->usage.entity_id = entity_id;
    return new_record;
}

//

usage_record_t*
usage_tree_lookup(
    usage_tree_ref  a_tree,
    int32_t         entity_id
)
{
    usage_record_priv_t *root_record = a_tree->by_entity_id_root;

    while ( root_record ) {
        if ( entity_id == root_record->usage.entity_id ) break;
        if ( entity_id < root_record->usage.entity_id ) root_record = root_record->left[0];
        root_record = root_record->right[0];
    }
    return &root_record->usage;
}

//

usage_record_t*
usage_tree_lookup_or_add(
    usage_tree_ref  a_tree,
    int32_t         entity_id
)
{
    usage_record_priv_t *root_record = a_tree->by_entity_id_root;

    if ( ! root_record ) {
        a_tree->by_entity_id_root = __usage_record_create(entity_id);
        a_tree->as_list = a_tree->by_entity_id_root;
        return &a_tree->by_entity_id_root->usage;
    }
    while ( root_record ) {
        if ( entity_id == root_record->usage.entity_id ) break;
        else if ( entity_id < root_record->usage.entity_id ) {
            if ( root_record->left[0] ) {
                root_record = root_record->left[0];
            } else {
                root_record = root_record->left[0] = __usage_record_create(entity_id);
                root_record->list = a_tree->as_list;
                a_tree->as_list = root_record;
                break;
            }
        }
        else if ( root_record->right[0] ) {
            root_record = root_record->right[0];
        }
        else {
            root_record = root_record->right[0] = __usage_record_create(entity_id);
            root_record->list = a_tree->as_list;
            a_tree->as_list = root_record;
            break;
        }
    }
    return &root_record->usage;
}

//

bool
usage_tree_update(
    usage_tree_ref  update_this_tree,
    usage_tree_ref  with_this_tree
)
{
    bool            is_okay = true;
    
    if ( with_this_tree && with_this_tree->as_list ) {
        usage_record_priv_t  *r = with_this_tree->as_list;
        
        while ( r ) {
            usage_record_t  *our_rec = usage_tree_lookup_or_add(update_this_tree, r->usage.entity_id);
            
            if ( ! our_rec ) {
                svl_printf(verbosity_error, "Failed to find/add record for entity id %d", r->usage.entity_id);
                is_okay = false;
                break;
            }
            our_rec->byte_usage += r->usage.byte_usage;
            our_rec->inode_usage += r->usage.inode_usage;
            r = r->list;
        }
    }
    return is_okay;
}

//

void
usage_tree_sort(
    usage_tree_ref  a_tree
)
{
    // We're going to traverse the list of records and add to secondary trees
    // based on byte_usage and inode_usage:
    usage_record_priv_t *record = a_tree->as_list;
    
    a_tree->by_byte_usage_root = record;
    if ( record ) {
        record = record->list;
        while ( record ) {
            usage_record_priv_t  *root;
            
            root = a_tree->by_byte_usage_root;
            while ( root ) {
                if ( record->usage.byte_usage == root->usage.byte_usage ) {
                    if ( root->middle[1] ) {
                        root = root->middle[1];
                    } else {
                        root->middle[1] = record;
                        break;
                    }
                }
                else if ( record->usage.byte_usage > root->usage.byte_usage ) {
                    if ( root->left[1] ) {
                        root = root->left[1];
                    } else {
                        root->left[1] = record;
                        break;
                    }
                }
                else if ( root->right[1] ) {
                    root = root->right[1];
                } else {
                    root->right[1] = record;
                    break;
                }
            }
            
            root = a_tree->by_inode_usage_root;
            while ( root ) {
                if ( record->usage.inode_usage == root->usage.inode_usage ) {
                    if ( root->middle[1] ) {
                        root = root->middle[1];
                    } else {
                        root->middle[1] = record;
                        break;
                    }
                }
                else if ( record->usage.inode_usage > root->usage.inode_usage ) {
                    if ( root->left[1] ) {
                        root = root->left[1];
                    } else {
                        root->left[1] = record;
                        break;
                    }
                }
                else if ( root->right[1] ) {
                    root = root->right[1];
                } else {
                    root->right[1] = record;
                    break;
                }
            }
            record = record->list;
        }
    }
}

//

void
usage_tree_calculate_totals(
    usage_tree_ref  a_tree
)
{
    usage_record_priv_t *r = a_tree->as_list;
    uint64_t            total_bytes = 0, total_inodes = 0;

    while ( r ) {
        total_bytes += r->usage.byte_usage;
        total_inodes += r->usage.inode_usage;
        r = r->list;
    }
    a_tree->total_bytes = total_bytes;
    a_tree->total_inodes = total_inodes;
}

//

uint64_t
usage_tree_get_total_bytes(
    usage_tree_ref  a_tree
)
{
    return a_tree->total_bytes;
}

//

uint64_t
usage_tree_get_total_inodes(
    usage_tree_ref  a_tree
)
{
    return a_tree->total_inodes;
}

//

struct usage_record_summary_context {
    tree_order_t            ordering;
    entity_id_to_name_fn    entity_to_name;
    unsigned int            options;
    unsigned int            parameter;
    uint64_t                total_byte_usage;
    uint64_t                total_inode_usage;
};

void
__usage_record_summarize_native(
    usage_record_priv_t                 *root,
    struct usage_record_summary_context *context
)
{
#   define ordering context->ordering
#   define entity_to_name context->entity_to_name
#   define options context->options
#   define parameter context->parameter
#   define total_byte_usage context->total_byte_usage
#   define total_inode_usage context->total_inode_usage

    char        bytes_str[64], bytes_per_inode_str[64];
    
    while ( root ) {
        // Display this record:
        const char  *name = NULL;
        double      byte_percentage = 100.0 * (double)root->usage.byte_usage / (double)total_byte_usage;
        double      bytes_per_inode = (double)root->usage.byte_usage / (double)root->usage.inode_usage;
        double      inode_percentage = 100.0 * (double)root->usage.inode_usage / (double)total_inode_usage;
        
        if ( entity_to_name ) name = entity_to_name(root->usage.entity_id);
        switch ( parameter ) {
            case usage_parameter_actual:
            case usage_parameter_size:
                if ( (options & usage_option_human_readable) ) {
                    byte_count_to_string_buffer(bytes_per_inode, bytes_per_inode_str, sizeof(bytes_per_inode_str));
                    byte_count_to_string_buffer((double)root->usage.byte_usage, bytes_str, sizeof(bytes_str));
                    if ( name ) {
                        printf("%20s %24s (%6.2f%%)  %24llu (%6.2f%%) @ %s/inode\n", name, bytes_str, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode_str);
                    } else {
                        printf("%20d %24s (%6.2f%%)  %24llu (%6.2f%%) @ %s/inode\n", root->usage.entity_id, bytes_str, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode_str);
                    }
                } else {
                    if ( name ) {
                        printf("%20s %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf B/inode\n", name, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                    } else {
                        printf("%20d %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf B/inode\n", root->usage.entity_id, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                    }
                }
                break;
            
            case usage_parameter_blocks:
                if ( name ) {
                    printf("%20s %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf block/inode\n", name, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                } else {
                    printf("%20d %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf block/inode\n", root->usage.entity_id, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                }
                break;
        }
        root = root->list;
    }
    
#   undef ordering
#   undef entity_to_name
#   undef options
#   undef parameter
#   undef total_byte_usage
#   undef total_inode_usage
}

void
__usage_record_summarize_bytes(
    usage_record_priv_t                 *root,
    struct usage_record_summary_context *context
)
{
#   define ordering context->ordering
#   define entity_to_name context->entity_to_name
#   define options context->options
#   define parameter context->parameter
#   define total_byte_usage context->total_byte_usage
#   define total_inode_usage context->total_inode_usage

    char        bytes_str[64], bytes_per_inode_str[64];
    
    if ( root ) {
        // Go to the left first:
        if ( root->left[ordering] ) __usage_record_summarize_bytes(root->left[ordering], context);

        // Display this record:
        const char  *name = NULL;
        double      byte_percentage = 100.0 * (double)root->usage.byte_usage / (double)total_byte_usage;
        double      bytes_per_inode = (double)root->usage.byte_usage / (double)root->usage.inode_usage;
        double      inode_percentage = 100.0 * (double)root->usage.inode_usage / (double)total_inode_usage;
        
        if ( entity_to_name ) name = entity_to_name(root->usage.entity_id);
        switch ( parameter ) {
            case usage_parameter_actual:
            case usage_parameter_size:
                if ( (options & usage_option_human_readable) ) {
                    byte_count_to_string_buffer(bytes_per_inode, bytes_per_inode_str, sizeof(bytes_per_inode_str));
                    byte_count_to_string_buffer((double)root->usage.byte_usage, bytes_str, sizeof(bytes_str));
                    if ( name ) {
                        printf("%20s %24s (%6.2f%%)  %24llu (%6.2f%%) @ %s/inode\n", name, bytes_str, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode_str);
                    } else {
                        printf("%20d %24s (%6.2f%%)  %24llu (%6.2f%%) @ %s/inode\n", root->usage.entity_id, bytes_str, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode_str);
                    }
                } else {
                    if ( name ) {
                        printf("%20s %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf B/inode\n", name, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                    } else {
                        printf("%20d %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf B/inode\n", root->usage.entity_id, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                    }
                }
                break;
            
            case usage_parameter_blocks:
                if ( name ) {
                    printf("%20s %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf block/inode\n", name, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                } else {
                    printf("%20d %24llu (%6.2f%%)  %24llu (%6.2f%%) @ %.0lf block/inode\n", root->usage.entity_id, (unsigned long long)root->usage.byte_usage, byte_percentage, root->usage.inode_usage, inode_percentage, bytes_per_inode);
                }
                break;
        }

        // Go down the middle, too:
        if ( root->middle[ordering] ) __usage_record_summarize_bytes(root->middle[ordering], context);

        // Finally, go to the right:
        if ( root->right[ordering] ) __usage_record_summarize_bytes(root->right[ordering], context);
    }
    
#   undef ordering
#   undef entity_to_name
#   undef options
#   undef parameter
#   undef total_byte_usage
#   undef total_inode_usage
}

void
__usage_record_summarize_inodes(
    usage_record_priv_t                 *root,
    struct usage_record_summary_context *context
)
{
#   define ordering context->ordering
#   define entity_to_name context->entity_to_name
#   define options context->options
#   define parameter context->parameter
#   define total_byte_usage context->total_byte_usage
#   define total_inode_usage context->total_inode_usage

    if ( root ) {
        // Go to the left first:
        if ( root->left[ordering] ) __usage_record_summarize_inodes(root->left[ordering], context);

        // Display this record:
        const char  *name = NULL;
        double      percentage = 100.0 * (double)root->usage.inode_usage / (double)total_inode_usage;

        if ( entity_to_name ) {
            name = entity_to_name(root->usage.entity_id);
            printf("%20s %24llu (%6.2f%%)\n", name, (unsigned long long)root->usage.inode_usage, percentage);
        } else {
            printf("%20d %24s (%6.2f%%)\n", root->usage.entity_id, byte_count_to_string(root->usage.inode_usage), percentage);
        }
        // Go down the middle, too:
        if ( root->middle[ordering] ) __usage_record_summarize_inodes(root->middle[ordering], context);

        // Finally, go to the right:
        if ( root->right[ordering] ) __usage_record_summarize_inodes(root->right[ordering], context);
    }
    
#   undef ordering
#   undef entity_to_name
#   undef options
#   undef parameter
#   undef total_byte_usage
#   undef total_inode_usage
}

void
usage_tree_summarize(
    usage_tree_ref          a_tree,
    entity_id_to_name_fn    entity_to_name,
    tree_order_t            ordering,
    unsigned int            options,
    unsigned int            parameter
)
{
    struct usage_record_summary_context context = {
                    .ordering = ordering,
                    .entity_to_name = entity_to_name,
                    .options = options,
                    .parameter = parameter,
                    .total_byte_usage = a_tree->total_bytes,
                    .total_inode_usage = a_tree->total_inodes
                };
    switch ( ordering ) {
        case tree_by_entity_id:
            __usage_record_summarize_bytes(a_tree->by_entity_id_root, &context);
            break;
        case tree_by_byte_usage:
            __usage_record_summarize_bytes(a_tree->by_byte_usage_root, &context);
            break;
        case tree_by_inode_usage:
            __usage_record_summarize_inodes(a_tree->by_inode_usage_root, &context);
            break;
        case tree_by_native_order:
            __usage_record_summarize_native(a_tree->as_list, &context);
            break;
        default:
            fprintf(stderr, "ERROR:  invalid tree ordering to usage_tree_summarize()\n");
            exit(EINVAL);
    }
}

//

#ifdef HAVE_MPI

#include "mpi.h"

static int __usage_record_t_fields = 3;
static int __usage_record_t_blocklengths[] = { 1, 1, 1 };
static MPI_Aint __usage_record_t_offsets[] = { 
                        offsetof(usage_record_t, entity_id),
                        offsetof(usage_record_t, byte_usage),
                        offsetof(usage_record_t, inode_usage)
                    };
static MPI_Datatype __usage_record_t_types[] = {
                        MPI_INT32_T,
                        MPI_UINT64_T,
                        MPI_UINT64_T
                    };

static int __usage_tree_mpi_rank = 0;
static int __usage_tree_mpi_size = 1;
static MPI_Datatype __usage_record_datatype;

void
__usage_tree_MPI_Init(void)
{
    static int      is_inited = 0;
    
    if ( ! is_inited ) {
        MPI_Comm_rank(MPI_COMM_WORLD, &__usage_tree_mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &__usage_tree_mpi_size);
        
        // Register our {entity id, byte usage} data type:
        MPI_Type_create_struct(
            __usage_record_t_fields,
            __usage_record_t_blocklengths,
            __usage_record_t_offsets,
            __usage_record_t_types,
            &__usage_record_datatype);
        MPI_Type_commit(&__usage_record_datatype);
        
        is_inited = 1;
    }
}

/*
 * Info from all other ranks must be reduced down into the root rank's usage tree.
 * There are two possible algorithms as I see it:
 *
 * 1. MPI reduction
 *    a. Non-root ranks form list of entity ids, send entity id count and list to root
 *    b. Root rank receives from each, produces an ordered list of all unique entity ids
 *    c. Root rank broadcasts length of unique entity id list followed by list itself
 *    d. Non-root ranks receive list length and list
 *    e. All ranks fill-in each list slot with local byte count for that entity id
 *    f. Root rank does an MPI_Reduce(MPI_SUM) on the list
 *    g. Root rank updates its usage tree with results
 * 2. Explicit reduction
 *    a. Non-root ranks form list of {entity id, byte usage} pairs, send count and list to root
 *    b. Root rank receives from each, updates its usage tree with results
 *
 * Option 1 requires no creation of an MPI type and leverages any aggregation accelerations
 * available to the fabric et al.  It is more complicated in its implementation and any
 * MPI_Reduce() performance benefits would likely be counterbalanced by the additional
 * communications.
 *
 * Option 2 requires that an MPI type representing the {entity id, byte usage} pairs be
 * registered, but is then a single sequence of MPI_Recv()'s from each non-root rank.  Most of
 * the work is done in the root rank with repeated tree updates, but option 1 does that once
 * in its final step with additional overhead to eliminate the loop over non-root ranks.  Overall,
 * I think option 2 is the most straightforward and probably most optimal -- so it will be
 * implemented first.
 *
 */
bool
usage_tree_reduce(
    usage_tree_ref  a_tree,
    int             root
)
{
    bool            is_okay = true;
    
    __usage_tree_MPI_Init();
    
    if ( __usage_tree_mpi_rank == root ) {
        int             i_rank = 0, entity_list_len = 0;
        usage_record_t  *entity_list = NULL;
        int             entity_record_size;
        MPI_Status      mpi_err;
        
        while ( is_okay && (i_rank < __usage_tree_mpi_size) ) {
            if ( i_rank != root ) {
                int                 rc, rank_list_len = 0;
                
                // How many entries in the other rank's list?
                rc = MPI_Recv(
                            &rank_list_len, 1, MPI_INT,
                            i_rank,
                            20,
                            MPI_COMM_WORLD,
                            &mpi_err
                        );
                if ( rc != MPI_SUCCESS ) {
                    svl_printf(verbosity_error, "Receiving list size from rank %d by rank %d failed (rc = %d)", i_rank, root, rc);
                    is_okay = false;
                    break;
                }
                svl_printf(verbosity_debug, "Received list size %d from rank %d by rank %d", rank_list_len, i_rank, root);
                
                if ( rank_list_len > 0 ) {
                    usage_record_t  *entity_list_p;
                    
                    // Allocate more room for the local copy if necessary:
                    if ( entity_list_len < rank_list_len ) {
                        usage_record_t  *new_entity_list = (usage_record_t*)realloc(entity_list, rank_list_len * sizeof(usage_record_t));
                    
                        if ( ! new_entity_list ) {
                            svl_printf(verbosity_error, "Failed to extend entity list from %d to %d (errno = %d)", entity_list_len, rank_list_len, errno);
                            is_okay = false;
                            break;
                        }
                        entity_list_len = rank_list_len;
                        entity_list = new_entity_list;
                    }
                
                    // Receive the other rank's list:
                    svl_printf(verbosity_debug, "Receiving %d record(s) from rank %d to rank %d", rank_list_len, i_rank, root);
                    rc = MPI_Recv(
                                entity_list, rank_list_len, __usage_record_datatype,
                                i_rank,
                                21,
                                MPI_COMM_WORLD,
                                &mpi_err
                            );
                    if ( rc != MPI_SUCCESS ) {
                        svl_printf(verbosity_error, "Receive of list from rank %d by rank %d failed (rc = %d)", i_rank, root, rc);
                        is_okay = false;
                        break;
                    }
                    svl_printf(verbosity_debug, "Received %d record(s) from rank %d to rank %d", rank_list_len, i_rank, root);
                
                    // Loop through the list, updating our tree:
                    entity_list_p = entity_list;
                    int i = 0;
                    while ( rank_list_len > 0 ) {
                        usage_record_t  *entity_rec;
                        
                        entity_rec = usage_tree_lookup_or_add(a_tree, entity_list_p->entity_id);
                        if ( ! entity_rec ) {
                            svl_printf(verbosity_error, "Failed to find/add record for entity id %d", entity_list_p->entity_id);
                            is_okay = false;
                            break;
                        }
                        entity_rec->byte_usage += entity_list_p->byte_usage;
                        entity_rec->inode_usage += entity_list_p->inode_usage;
                        entity_list_p++;
                        rank_list_len--;
                    }
                }
            }
            i_rank++;
        }
        if ( entity_list ) {
            free(entity_list);
        }
    } else {
        int             rc, entity_list_len = usage_tree_get_size(a_tree);
        usage_record_t  *entity_list = NULL;
        
        while ( 1 ) {
            // Send the list length:
            rc = MPI_Send(
                        &entity_list_len, 1, MPI_INT,
                        root,
                        20,
                        MPI_COMM_WORLD
                    );
            if ( rc != MPI_SUCCESS ) {
                svl_printf(verbosity_error, "MPI send of list size from rank %d to rank %d failed (rc = %d)", __usage_tree_mpi_rank, root, rc);
                is_okay = false;
                break;
            }
            svl_printf(verbosity_debug, "MPI send of list size %d from rank %d to rank %d", entity_list_len, __usage_tree_mpi_rank, root);
            
            // Continue only if we have a list to send:
            if ( entity_list_len > 0 ) {
                usage_record_t      *entity_list_p;
                usage_record_priv_t *r = a_tree->as_list;
                
                entity_list = (usage_record_t*)malloc(entity_list_len * sizeof(usage_record_t));
                if ( ! entity_list ) {
                    svl_printf(verbosity_error, "Failed to allocate entity list in rank %d (errno = %d)", __usage_tree_mpi_rank, errno);
                    is_okay = false;
                    break;
                }
                entity_list_p = entity_list;
                
                // Fill-in the list:
                while ( r ) {
                    entity_list_p->entity_id = r->usage.entity_id;
                    entity_list_p->byte_usage = r->usage.byte_usage;
                    entity_list_p->inode_usage = r->usage.inode_usage;
                    entity_list_p++;
                    r = r->list;
                }
                
                // Send the list:
                svl_printf(verbosity_debug, "Sending %d record(s) from rank %d to rank %d", entity_list_len, __usage_tree_mpi_rank, root);
                rc = MPI_Send(
                            entity_list, entity_list_len, __usage_record_datatype,
                            root,
                            21,
                            MPI_COMM_WORLD
                        );
                if ( rc != MPI_SUCCESS ) {
                    svl_printf(verbosity_error, "Send of list from rank %d to rank %d failed (rc = %d)", __usage_tree_mpi_rank, root, rc);
                    is_okay = false;
                    break;
                }
            }
            break;
        }   
        if ( entity_list ) {
            free(entity_list);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    return is_okay;
}

#endif