/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * Summarize per-user and per-group usage in a directory hierarchy.
 *
 * Specific to Linux thanks to the "ftw" functionality.
 *
 */

#define _XOPEN_SOURCE 500
#include <ftw.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>

#ifndef ST_NBLOCKSIZE
# ifdef S_BLKSIZE
#  define ST_NBLOCKSIZE S_BLKSIZE
# else
#  define ST_NBLOCKSIZE 512
# endif
#endif

//

struct option cli_options[] = {
        { "help",               no_argument,        NULL,   'h' },
        { "quiet",              no_argument,        NULL,   'q' },
        { "verbose",            no_argument,        NULL,   'v' },
        { "human-readable",     no_argument,        NULL,   'H' },
        { "numeric",            no_argument,        NULL,   'n' },
        { "progress",           no_argument,        NULL,   'p' },
        { "progress-stride",    required_argument,  NULL,   'l' },
        { "unsorted",           no_argument,        NULL,   'S' },
        { "parameter",          required_argument,  NULL,   'P' },
        { NULL,                 0,                  NULL,    0  }
    };
const char *cli_options_str = "hqvHnpl:SP:";

//

typedef const char* (*entity_id_to_name_fn)(int32_t entity_id);

//

typedef struct usage_record {
    int32_t     entity_id;
    uint64_t    byte_usage;

    struct usage_record *left[2], *middle[2], *right[2];
    struct usage_record *list;
} usage_record_t;

typedef struct usage_tree {
    usage_record_t          *as_list;
    usage_record_t          *by_entity_id_root;
    usage_record_t          *by_byte_usage_root;

    entity_id_to_name_fn    entity_to_name;
} usage_tree_t;

typedef enum {
    tree_by_entity_id = 0,
    tree_by_byte_usage = 1
} tree_order_t;

//

enum {
    verbosity_quiet = 0,
    verbosity_error = 1,
    verbosity_warning = 2,
    verbosity_info = 3,
    verbosity_debug = 4
};

enum {
    parameter_actual = 0,
    parameter_size = 1,
    parameter_blocks = 2,
    parameter_max = 3
};

const char* parameter_names[] = {
    "actual",
    "st_size",
    "st_blocks",
    NULL
};

#ifndef DEFAULT_PROGRESS_STRIDE
#define DEFAULT_PROGRESS_STRIDE  10000
#endif

static usage_tree_t     *by_uid = NULL;
static usage_tree_t     *by_gid = NULL;
static uint64_t         total_usage = 0;
static uint64_t         item_count = 0;
static int              verbosity = 1;
static bool             should_show_human_readable = false;
static bool             should_show_numeric_entity_ids = false;
static bool             should_show_progress = false;
static uint64_t         progress_stride = DEFAULT_PROGRESS_STRIDE;
static bool             should_sort = true;
static unsigned int     parameter = parameter_actual;


//

const char*
byte_count_to_string(
    uint64_t    bytes
)
{
    static const char* units[] = { "  B", "KiB", "MiB", "GiB", "TiB", "PiB", NULL };
    static char outbuffer[64];

    float       bytecount = (float)bytes;
    int         unit = 0;

    while ( bytecount > 1024.0 ) {
        if ( ! units[unit + 1] ) break;
        bytecount /= 1024.0;
        unit++;
    }
    snprintf(outbuffer, sizeof(outbuffer), "%.2f %s", bytecount, units[unit]);
    return outbuffer;
}

//

bool
is_verbose(
    int         level
)
{
    return ( level <= verbosity );
}

//

bool
set_parameter(
    const char      *parameter_name
)
{
    const char*     *P = parameter_names;
    unsigned int    i = parameter_actual;
    
    while ( *P ) {
        if ( strcasecmp(parameter_name, *P) == 0 ) {
            parameter = i;
            return true;
        }
        i++;
        P++;
    }
    return false;
}

//

bool
set_progress_stride(
    const char      *progress_stride_str
)
{
    char                    *endptr = NULL;
    unsigned long long int  value = strtoull(progress_stride_str, &endptr, 0);
    
    if ( ! value || (endptr == progress_stride_str) || (*endptr) ) return false;
    
    progress_stride = value;
    return true;
}

//

usage_tree_t*
usage_tree_create(
    entity_id_to_name_fn    entity_to_name
)
{
    usage_tree_t        *new_tree = (usage_tree_t*)malloc(sizeof(usage_tree_t));

    if ( ! new_tree ) {
        perror("Unable to allocate new usage tree");
        exit(ENOMEM);
    }
    memset(new_tree, 0, sizeof(*new_tree));
    new_tree->entity_to_name = entity_to_name;
    return new_tree;
}

//

void
usage_tree_destroy(
    usage_tree_t    *a_tree
)
{
    // Walk the as_list and remove records:
    usage_record_t  *r = a_tree->as_list;

    while ( r ) {
        usage_record_t  *next = r->list;

        free((void*)r);
        r = next;
    }

    // Remove the tree itself:
    free((void*)a_tree);
}

//

usage_record_t*
__usage_record_create(
    int32_t         entity_id
)
{
    usage_record_t  *new_record = (usage_record_t*)malloc(sizeof(usage_record_t));

    if ( ! new_record ) {
        perror("Unable to allocate new usage record");
        exit(ENOMEM);
    }
    memset(new_record, 0, sizeof(*new_record));
    new_record->entity_id = entity_id;
    return new_record;
}

//

usage_record_t*
usage_tree_lookup(
    usage_tree_t    *a_tree,
    int32_t         entity_id
)
{
    usage_record_t  *root_record = a_tree->by_entity_id_root;

    while ( root_record ) {
        if ( entity_id == root_record->entity_id ) break;
        if ( entity_id < root_record->entity_id ) root_record = root_record->left[0];
        root_record = root_record->right[0];
    }
    return root_record;
}

//

usage_record_t*
usage_tree_lookup_or_add(
    usage_tree_t    *a_tree,
    int32_t         entity_id
)
{
    usage_record_t  *root_record = a_tree->by_entity_id_root;

    if ( ! root_record ) {
        a_tree->by_entity_id_root = __usage_record_create(entity_id);
        a_tree->as_list = a_tree->by_entity_id_root;
        return a_tree->by_entity_id_root;
    }
    while ( root_record ) {
        if ( entity_id == root_record->entity_id ) break;
        else if ( entity_id < root_record->entity_id ) {
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
    return root_record;
}

//

void
usage_tree_sort_by_byte_usage(
    usage_tree_t    *a_tree
)
{
    // We're going to traverse the list of records and add to a secondary tree
    // based on byte_usage:
    usage_record_t  *record = a_tree->as_list;

    a_tree->by_byte_usage_root = record;
    if ( record ) {
        record = record->list;
        while ( record ) {
            usage_record_t  *root = a_tree->by_byte_usage_root;

            while ( root ) {
                if ( record->byte_usage == root->byte_usage ) {
                    if ( root->middle[1] ) {
                        root = root->middle[1];
                    } else {
                        root->middle[1] = record;
                        break;
                    }
                }
                else if ( record->byte_usage > root->byte_usage ) {
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
__usage_record_summarize(
    usage_record_t          *root,
    tree_order_t            ordering,
    entity_id_to_name_fn    entity_to_name
)
{
    if ( root ) {
        // Go to the left first:
        if ( root->left[ordering] ) __usage_record_summarize(root->left[ordering], ordering, entity_to_name);

        // Display this record:
        const char  *name = NULL;
        double      percentage = 100.0 * (double)root->byte_usage / (double)total_usage;

        if ( entity_to_name ) name = entity_to_name(root->entity_id);

        switch ( parameter ) {
            case parameter_actual:
            case parameter_size:
                if ( should_show_human_readable ) {
                    if ( name ) {
                        printf("%20s %24s (%6.2f%%)\n", name, byte_count_to_string(root->byte_usage), percentage);
                    } else {
                        printf("%20d %24s (%6.2f%%)\n", root->entity_id, byte_count_to_string(root->byte_usage), percentage);
                    }
                } else {
                    if ( name ) {
                        printf("%20s %24llu (%6.2f%%)\n", name, (unsigned long long)root->byte_usage, percentage);
                    } else {
                        printf("%20d %24llu (%6.2f%%)\n", root->entity_id, (unsigned long long)root->byte_usage, percentage);
                    }
                }
                break;
            
            case parameter_blocks:
                if ( name ) {
                    printf("%20s %24llu (%6.2f%%)\n", name, (unsigned long long)root->byte_usage, percentage);
                } else {
                    printf("%20d %24llu (%6.2f%%)\n", root->entity_id, (unsigned long long)root->byte_usage, percentage);
                }
                break;
        }

        // Go down the middle, too:
        if ( root->middle[ordering] ) __usage_record_summarize(root->middle[ordering], ordering, entity_to_name);

        // Finally, go to the right:
        if ( root->right[ordering] ) __usage_record_summarize(root->right[ordering], ordering, entity_to_name);
    }
}

void
usage_tree_summarize(
    usage_tree_t    *a_tree,
    tree_order_t    ordering
)
{
    switch ( ordering ) {
        case tree_by_entity_id:
            __usage_record_summarize(a_tree->by_entity_id_root, ordering, a_tree->entity_to_name);
            break;
        case tree_by_byte_usage:
            __usage_record_summarize(a_tree->by_byte_usage_root, ordering, a_tree->entity_to_name);
            break;
        default:
            fprintf(stderr, "ERROR:  invalid tree ordering to usage_tree_summarize()\n");
            exit(EINVAL);
    }
}

//

const char*
gid_to_gname(
    int32_t gid
)
{
    struct group    *gentry = getgrgid((gid_t)gid);

    if ( gentry ) return gentry->gr_name;
    return NULL;
}

//

const char*
uid_to_uname(
    int32_t uid
)
{
    struct passwd   *uentry = getpwuid((uid_t)uid);

    if ( uentry ) return uentry->pw_name;
    return NULL;
}

//

int
nftw_callback(
    const char          *fpath,
    const struct stat   *finfo,
    int                 typeflag,
    struct FTW          *ftw_info
)
{
    usage_record_t      *r;

    if ( typeflag == FTW_DNR ) {
        if ( is_verbose(verbosity_warning) ) fprintf(stderr, "[WARNING] cannot descend into directory: %s\n", fpath);
        return 0;
    }
    if ( typeflag == FTW_NS ) {
        if ( is_verbose(verbosity_warning) ) fprintf(stderr, "[WARNING] unresolvable symlink: %s\n", fpath);
        return 0;
    }

    if ( is_verbose(verbosity_debug) && typeflag == FTW_D ) fprintf(stderr, "[DEBUG] %s\n", fpath);
    
    uint64_t        size;
    
    switch ( parameter ) {
        case parameter_actual:
            size = finfo->st_blocks * ST_NBLOCKSIZE;
            break;
        case parameter_size:
            size = finfo->st_size;
            break;
        case parameter_blocks:
            size = finfo->st_blocks;
            break;
    }
    total_usage += size;
    item_count++;
    r = usage_tree_lookup_or_add(by_uid, finfo->st_uid);
    if ( r ) r->byte_usage += size;
    r = usage_tree_lookup_or_add(by_gid, finfo->st_gid);
    if ( r ) r->byte_usage += size;

    return 0;
}

//

int
nftw_progress_callback(
    const char          *fpath,
    const struct stat   *finfo,
    int                 typeflag,
    struct FTW          *ftw_info
)
{
    usage_record_t      *r;

    if ( typeflag == FTW_DNR ) {
        if ( is_verbose(verbosity_warning) ) fprintf(stderr, "[WARNING] cannot descend into directory: %s\n", fpath);
        return 0;
    }
    if ( typeflag == FTW_NS ) {
        if ( is_verbose(verbosity_warning) ) fprintf(stderr, "[WARNING] unresolvable symlink: %s\n", fpath);
        return 0;
    }

    if ( is_verbose(verbosity_debug) && typeflag == FTW_D ) fprintf(stderr, "[DEBUG] %s\n", fpath);
    
    uint64_t        size;
    
    switch ( parameter ) {
        case parameter_actual:
            size = finfo->st_blocks * ST_NBLOCKSIZE;
            break;
        case parameter_size:
            size = finfo->st_size;
            break;
        case parameter_blocks:
            size = finfo->st_blocks;
            break;
    }
    total_usage += size;
    item_count++;
    r = usage_tree_lookup_or_add(by_uid, finfo->st_uid);
    if ( r ) r->byte_usage += size;
    r = usage_tree_lookup_or_add(by_gid, finfo->st_gid);
    if ( r ) r->byte_usage += size;

    if ( (item_count % progress_stride) == 0 ) {
        if ( is_verbose(verbosity_info) ) {
            fprintf(stderr, "[INFO]   %12llu items scanned...\n", (unsigned long long)item_count);
        } else {
            printf("... %llu items scanned...\n", (unsigned long long)item_count);
        }
    }
    return 0;
}

//

void
usage(
    const char  *exe
)
{
    printf(
            "usage -- [du] [b]y [u]ser and [g]roup\n\n"
            "    %s {options} <path> {<path> ..}\n\n"
            "  options:\n\n"
            "    --help/-h                show this help info\n"
            "    --verbose/-v             increase amount of output shown during execution\n"
            "    --quiet/-q               decrease amount of output shown during execution\n"
            "    --human-readable/-H      display usage with units, not as bytes\n"
            "    --numeric/-n             do not resolve numeric uid/gid to names\n"
            "    --progress/-p            display item-scanned counts as the traversal is\n"
            "                             executing\n"
            "    --progress-stride/-l #   display progress every # items processed\n"
            "                             (default: %llu)\n"
            "    --unsorted/-S            do not sort by byte usage before summarizing\n"
            "    --parameter/-P <param>   sizing field over which to sum:\n\n"
            "                                 actual      bytes on disk (the default)\n"
            "                                 size        nominal size (possibly sparse)\n"
            "                                 blocks      block count\n"
            "\n"
            "  <path> can be an absolute or relative file system path to a directory or\n"
            "  file (not very interesting), and for each <path> the traversal is repeated\n"
            "  (rather than aggregating the sum over the paths).\n"
            "\n",
            exe,
            (unsigned long long int)DEFAULT_PROGRESS_STRIDE            
        );
}

//

int
main(
    int             argc,
    char            **argv
)
{
    int             opt, rc = 0;

    while ( (opt = getopt_long(argc, argv, cli_options_str, cli_options, NULL)) != -1 ) {
        switch ( opt ) {
            case 'h':
                usage(argv[0]);
                exit(0);

            case 'q':
                verbosity--;
                break;

            case 'v':
                verbosity++;
                break;

            case 'H':
                should_show_human_readable = true;
                break;

            case 'n':
                should_show_numeric_entity_ids = true;
                break;

            case 'p':
                should_show_progress = true;
                break;
            
            case 'l':
                if ( ! set_progress_stride(optarg) ) {
                    if ( is_verbose(verbosity_error) ) fprintf(stderr, "[ERROR] Invalid argument to --progress-stride/-l: %s\n", optarg);
                    exit(EINVAL);
                }
                break;

            case 'S':
                should_sort = false;
                break;
            
            case 'P':
                if ( ! set_parameter(optarg) ) {
                    if ( is_verbose(verbosity_error) ) fprintf(stderr, "[ERROR] Invalid argument to --parameter/-P: %s\n", optarg);
                    exit(EINVAL);
                }
                break;

        }
    }

    // Increase our nice level (lowest priority possible, please):
    if ( geteuid() != 0 ) nice(999);

    while ( (rc == 0) && (optind < argc) ) {
        const char      *root_path = argv[optind];
        struct timespec start_time, end_time;

        // Initialize the two summary trees:
        if ( is_verbose(verbosity_debug) ) fprintf(stderr, "[DEBUG] Allocating by-uid tree\n");
        by_uid = usage_tree_create(should_show_numeric_entity_ids ? NULL : uid_to_uname);

        if ( is_verbose(verbosity_debug) ) fprintf(stderr, "[DEBUG] Allocating by-gid tree\n");
        by_gid = usage_tree_create(should_show_numeric_entity_ids ? NULL : gid_to_gname);

        // Initialize global counters, too:
        total_usage = 0;
        item_count = 0;

        // Walk the directory hierarchy:
        if ( is_verbose(verbosity_info) ) fprintf(stderr, "[INFO] Starting traversal of %s\n", root_path);
        clock_gettime(CLOCK_BOOTTIME, &start_time);
        rc = nftw(root_path, (should_show_progress ? nftw_progress_callback : nftw_callback), 100, FTW_MOUNT | FTW_PHYS);
        clock_gettime(CLOCK_BOOTTIME, &end_time);
        if ( is_verbose(verbosity_info) ) {
            double      seconds = (end_time.tv_sec - start_time.tv_sec) + 1e-9 * (end_time.tv_nsec - start_time.tv_nsec);

            fprintf(stderr, "[INFO] Completed traversal of %s\n", root_path);
            fprintf(stderr, "[INFO]   %llu files/directories in %.3f seconds\n", (unsigned long long int)item_count, seconds);
            fprintf(stderr, "[INFO]   %12.0f files/directories per second\n", (double)item_count / seconds);
        }
        if ( is_verbose(verbosity_error) && (rc != 0) ) fprintf(stderr, "[ERROR] Directory walk exited early due to internal failure\n");

        // Sumarize:
        printf("Total usage:\n");
        switch ( parameter ) {
        
            case parameter_actual:
            case parameter_size:
                if ( should_show_human_readable ) {
                    printf(
                           "%20s %24s\n",
                           "", byte_count_to_string(total_usage)
                        );
                } else {
                    printf(
                           "%20s %24llu\n",
                           "", (unsigned long long)total_usage
                        );
                }
                break;
            
            case parameter_blocks:
                printf(
                       "%20s %24llu\n",
                       "", (unsigned long long)total_usage
                    );
                break;
        }
        if ( should_sort ) {
            if ( is_verbose(verbosity_debug) ) fprintf(stderr, "[DEBUG] Sorting by-uid tree by byte usage\n");
            usage_tree_sort_by_byte_usage(by_uid);
            if ( is_verbose(verbosity_debug) ) fprintf(stderr, "[DEBUG] Sorting by-gid tree by byte usage\n");
            usage_tree_sort_by_byte_usage(by_gid);

            printf("Usage by-user for %s:\n", root_path);
            usage_tree_summarize(by_uid, tree_by_byte_usage);
            printf("\nUsage by-group for %s:\n", root_path);
            usage_tree_summarize(by_gid, tree_by_byte_usage);
        } else {
            printf("Usage by-user for %s:\n", root_path);
            usage_tree_summarize(by_uid, tree_by_entity_id);
            printf("\nUsage by-group for %s:\n", root_path);
            usage_tree_summarize(by_gid, tree_by_entity_id);
        }

        // Destroy the summary trees:
        if ( is_verbose(verbosity_debug) ) fprintf(stderr, "[DEBUG] Deallocating by-uid tree\n");
        usage_tree_destroy(by_uid);
        if ( is_verbose(verbosity_debug) ) fprintf(stderr, "[DEBUG] Deallocating by-gid tree\n");
        usage_tree_destroy(by_gid);

        // Move on to the next path to scan:
        optind++;
        if ( optind < argc ) printf("\n");
    }
    return rc;
}
