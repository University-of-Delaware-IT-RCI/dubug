/*!
 * dubug - [du] [b]y [u]ser and [g]roup
 *
 * Summarize per-user and per-group usage in a directory hierarchy.
 *
 * Specific to Linux thanks to the "ftw" functionality.
 *
 */

#include "config.h"
#include "usage_tree.h"
#include "byte_count_to_string.h"
#include "simple_verbose_logging.h"
#include "fs_path.h"
#include "work_queue.h"

#ifdef HAVE_MPI
# include "mpi.h"
#endif

static const char *dubug_version_string = DUBUG_VERSION;

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
        { "version",            no_argument,        NULL,   'V' },
        { "quiet",              no_argument,        NULL,   'q' },
        { "verbose",            no_argument,        NULL,   'v' },
        { "human-readable",     no_argument,        NULL,   'H' },
        { "numeric",            no_argument,        NULL,   'n' },\
        { "unsorted",           no_argument,        NULL,   'S' },
        { "parameter",          required_argument,  NULL,   'P' },
#ifdef HAVE_MPI
        { "max-depth",          required_argument,  NULL,   'D' },
#endif
        { NULL,                 0,                  NULL,    0  }
    };
const char *cli_options_str = "hVqvHnSP:"
#ifdef HAVE_MPI
                "D:"
#endif
;

//

#ifndef DEFAULT_PROGRESS_STRIDE
#define DEFAULT_PROGRESS_STRIDE  10000
#endif

static usage_tree_ref   by_uid = NULL;
static usage_tree_ref   by_gid = NULL;
static bool             should_show_human_readable = false;
static bool             should_show_numeric_entity_ids = false;
static bool             should_sort = true;
static unsigned int     parameter = usage_parameter_actual;

//

bool
set_parameter(
    const char      *parameter_name
)
{
    parameter = usage_parameter_from_string(parameter_name);
    return (parameter != usage_parameter_max);
}

//

const char*
gid_to_gname(
    int32_t gid
)
{
    struct group    gentry, *gentry_ptr;
    static char     gentry_str_buffer[1024];
    
    if ( getgrgid_r((gid_t)gid, &gentry, gentry_str_buffer, sizeof(gentry_str_buffer), &gentry_ptr) == 0 ) {
        if ( gentry_ptr ) return gentry_ptr->gr_name;
    }
    return "<unknown>";
}

//

const char*
uid_to_uname(
    int32_t uid
)
{
    struct passwd   uentry, *uentry_ptr;
    static char     uentry_str_buffer[1024];
    
    if ( getpwuid_r((uid_t)uid, &uentry, uentry_str_buffer, sizeof(uentry_str_buffer), &uentry_ptr) == 0 ) {
        if ( uentry_ptr ) return uentry_ptr->pw_name;
    }
    return "<unknown>";
}

//

void
usage(
    const char  *exe
)
{
    printf(
#ifdef HAVE_MPI
            "usage -- [du] [b]y [u]ser and [g]roup (with MPI parallelism)\n\n"
#else
            "usage -- [du] [b]y [u]ser and [g]roup\n\n"
#endif
            "    %s {options} <path> {<path> ..}\n\n"
            "  options:\n\n"
            "    --help/-h                show this help info\n"
            "    --version/-V             show the program version\n"
            "    --verbose/-v             increase amount of output shown during execution\n"
            "    --quiet/-q               decrease amount of output shown during execution\n"
            "    --human-readable/-H      display usage with units, not as bytes\n"
            "    --numeric/-n             do not resolve numeric uid/gid to names\n"
            "    --unsorted/-S            do not sort by byte usage before summarizing\n"
            "    --parameter/-P <param>   sizing field over which to sum:\n"
            "\n"
            "                                 actual      bytes on disk (the default)\n"
            "                                 st_size     nominal size (possibly sparse)\n"
            "                                 st_blocks   block count\n"
            "\n"
#ifdef HAVE_MPI
            "  parallel options:\n\n"
            "    --max-depth/-D #         maximum depth to descend to produce distributed\n"
            "                             workload\n"
            "\n"
#endif
            "  <path> can be an absolute or relative file system path to a directory or\n"
            "  file (not very interesting), and for each <path> the traversal is repeated\n"
            "  (rather than aggregating the sum over the paths).\n"
            "\n",
            exe,
            (unsigned long long int)DEFAULT_PROGRESS_STRIDE
        );
}

//

void
version(
    const char   *exe
)
{
    const char  *basename = exe, *slash = NULL;

    // Isolate the program basename:
    while ( *basename ) {
        if ( *basename == '/' ) slash = basename;
        basename++;
    }
    printf(
        "%s version %s"
#ifdef HAVE_MPI
        " (mpi parallelism)"
#endif
        "\n"
        ,
        ( slash ? (slash + 1) : exe ),
        dubug_version_string
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
#ifdef HAVE_MPI
    int             thread_mode, mpi_rank, mpi_size;
    
    rc = MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thread_mode);
    if ( rc != MPI_SUCCESS ) {
        svl_printf(verbosity_critical, "unable to initialize MPI (rc = %d)", rc);
        exit(1);
    }
    rc = MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    if ( rc != MPI_SUCCESS ) {
        svl_printf(verbosity_critical, "unable to determine MPI rank (rc = %d)", rc);
        MPI_Finalize();
        exit(1);
    }
    rc = MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    if ( rc != MPI_SUCCESS ) {
        svl_printf(verbosity_critical, "unable to determine MPI size (rc = %d)", rc);
        MPI_Finalize();
        exit(1);
    }
#endif

    while ( (opt = getopt_long(argc, argv, cli_options_str, cli_options, NULL)) != -1 ) {
        switch ( opt ) {
            case 'h':
#ifdef HAVE_MPI
                if ( mpi_rank == 0 ) usage(argv[0]);
                MPI_Finalize();
#else
                usage(argv[0]);
#endif
                exit(0);

            case 'V':
#ifdef HAVE_MPI
                if ( mpi_rank == 0 ) version(argv[0]);
                MPI_Finalize();
#else
                version(argv[0]);
#endif
                exit(0);

            case 'q':
                svl_dec_verbosity();
                break;

            case 'v':
                svl_inc_verbosity();
                break;

            case 'H':
                should_show_human_readable = true;
                break;

            case 'n':
                should_show_numeric_entity_ids = true;
                break;

            case 'S':
                should_sort = false;
                break;
            
            case 'P':
                if ( ! set_parameter(optarg) ) {
                    svl_printf(verbosity_error, "Invalid argument to --parameter/-P: %s", optarg);
                    exit(EINVAL);
                }
                break;

        }
    }

#ifdef HAVE_MPI
    svl_printf(verbosity_info, "MPI rank %d of %d initialized", mpi_rank, mpi_size);
#endif

    // Increase our nice level (lowest priority possible, please):
    if ( geteuid() != 0 ) nice(999);

    while ( (rc == 0) && (optind < argc) ) {
        unsigned int    usage_options = (should_show_human_readable ? usage_option_human_readable : 0);
        work_queue_ref  wqueue = work_queue_alloc(parameter);
        fs_path_ref     root = fs_path_alloc_with_cstring_const(argv[optind]);
        
        if ( wqueue && root ) {
            // Initialize the work queue:
            struct timespec start_time, end_time;
            bool            is_okay = true;
            usage_tree_ref  by_uid, by_gid;
            
            clock_gettime(CLOCK_BOOTTIME, &start_time);

#ifdef HAVE_MPI
            if ( mpi_rank == 0 ) {
                svl_printf(verbosity_info, "Building work queue from path %s", argv[optind]);
                is_okay = work_queue_build(wqueue, root, mpi_size);
                if ( is_okay ) {
                    unsigned int    queue_depth = work_queue_get_path_count(wqueue);
                    unsigned int    base_paths_per_rank = queue_depth / mpi_size;
                    unsigned int    n_ranks_extra_path = queue_depth % mpi_size;
                    unsigned int    queue_idx = 0;
                    int             target_rank = 1;
                    
                    svl_printf(verbosity_info, "- Initial work queue of size %u generated", queue_depth);
                    if ( n_ranks_extra_path == 0 ) {
                        svl_printf(verbosity_info, "- Each rank will handle %u paths", base_paths_per_rank);
                    } else {
                        svl_printf(verbosity_info, "- Each rank will handle at least %u paths", base_paths_per_rank);
                        svl_printf(verbosity_info, "- %u rank%s will handle an extra path", n_ranks_extra_path, (n_ranks_extra_path == 1) ? "" : "s");
                    }
                    
                    if ( mpi_size > 1 ) {
                        // We will distribute work to all other ranks first, leaving the root
                        // rank for last.  If we run out of work then some ranks will have nothing to
                        // do -- including the root rank:
                        while ( (queue_idx < queue_depth) && (target_rank < mpi_size) ) {
                            unsigned int    queue_count = base_paths_per_rank + (mpi_rank < n_ranks_extra_path);
                            sbb_ref         serialized_queue = work_queue_serialize_range(wqueue, queue_idx, queue_count);
                            if ( serialized_queue ) {
                                uint64_t    serialized_queue_len = sbb_get_length(serialized_queue);
                                
                                svl_printf(verbosity_debug, "Sending work queue size %llu to rank %d", serialized_queue_len, target_rank);
                                MPI_Send(&serialized_queue_len, 1, MPI_UINT64_T, target_rank, 10, MPI_COMM_WORLD);
                                MPI_Send(sbb_get_buffer_ptr(serialized_queue), serialized_queue_len, MPI_UINT8_T, target_rank, 11, MPI_COMM_WORLD);
                                sbb_free(serialized_queue);
                                target_rank++;
                            }
                            queue_idx += queue_count;
                        }
                        
                        // Remove all paths that have been distributed:
                        work_queue_delete(wqueue, 0, queue_idx);
                        
                        // Any remaining ranks should be told they have nothing to do:
                        while ( target_rank < mpi_size ) {
                            uint64_t    serialized_queue_len = 0;
                            svl_printf(verbosity_debug, "Sending non-work to rank %d", target_rank);
                            MPI_Send(&serialized_queue_len, 1, MPI_UINT64_T, target_rank++, 10, MPI_COMM_WORLD);
                        }
                    }
                    
                    // If there's anything left in the work queue, handle that now:
                    svl_printf(verbosity_debug, "Starting traversal of work queue");
                    work_queue_complete(wqueue);
                    svl_printf(verbosity_info, "Completed work queue");
                    
                    // Gather results from everyone else:
                    MPI_Barrier(MPI_COMM_WORLD);
                    svl_printf(verbosity_debug, "Reducing over uid usage");
                    usage_tree_reduce(work_queue_get_by_uid_usage_tree(wqueue), 0);
                    svl_printf(verbosity_debug, "Reducing over gid usage");
                    usage_tree_reduce(work_queue_get_by_gid_usage_tree(wqueue), 0);
                }
                clock_gettime(CLOCK_BOOTTIME, &end_time);
            } else {
                uint64_t        serialized_queue_len;
                
                
                // Try to receive the serialized work queue size:
                svl_printf(verbosity_debug, "Receiving work queue size");
                MPI_Recv(&serialized_queue_len, 1, MPI_UINT64_T, 0, 10, MPI_COMM_WORLD, NULL);
                svl_printf(verbosity_debug, "Received size %llu", serialized_queue_len);
                if ( serialized_queue_len > 0 ) {
                    sbb_ref     serialized_queue = sbb_alloc(serialized_queue_len, sbb_options_byte_swap);
                    
                    sbb_set_length(serialized_queue, serialized_queue_len, 0);
                    svl_printf(verbosity_debug, "Receiving work queue");
                    MPI_Recv((void*)sbb_get_buffer_ptr(serialized_queue), serialized_queue_len, MPI_UINT8_T, 0, 11, MPI_COMM_WORLD, NULL);
                    svl_printf(verbosity_debug, "Received work queue");
                    sbb_summary(serialized_queue);
                    
                    // We don't need the existing work queue:
                    work_queue_free(wqueue);
                    wqueue = work_queue_alloc_deserialize(serialized_queue);
                    if ( wqueue ) {
                        unsigned int    queue_depth = work_queue_get_path_count(wqueue);
                        
                        svl_printf(verbosity_debug, "Received work queue is %u path(s)", queue_depth);
                    }
                }

                // Handle the work queue now; if the parent didn't hand us anything then we have
                // an empty wqueue that was created at the start of this program block:
                svl_printf(verbosity_debug, "Starting traversal of work queue");
                work_queue_complete(wqueue);
                svl_printf(verbosity_info, "Completed work queue");
                
                // Send results to root:
                MPI_Barrier(MPI_COMM_WORLD);
                svl_printf(verbosity_debug, "Reducing over uid usage");
                usage_tree_reduce(work_queue_get_by_uid_usage_tree(wqueue), 0);
                svl_printf(verbosity_debug, "Reducing over gid usage");
                usage_tree_reduce(work_queue_get_by_gid_usage_tree(wqueue), 0);
            }
            if ( is_okay && (mpi_rank == 0) ) {
#else
            is_okay = work_queue_build(wqueue, root, 1);
            
            if ( is_okay ) {
                svl_printf(verbosity_info, "Starting traversal of work queue");
                work_queue_complete(wqueue);
                svl_printf(verbosity_info, "Completed work queue");
                
                clock_gettime(CLOCK_BOOTTIME, &end_time);
#endif

                // Calculate totals on the two trees:
                usage_tree_calculate_totals((by_uid = work_queue_get_by_uid_usage_tree(wqueue)));
                usage_tree_calculate_totals((by_gid = work_queue_get_by_gid_usage_tree(wqueue)));
        
                // Temporal summary?
                if ( svl_is_verbosity(verbosity_info) ) {
                    double      seconds = (end_time.tv_sec - start_time.tv_sec) + 1e-9 * (end_time.tv_nsec - start_time.tv_nsec);
                    uint64_t    total_inodes = usage_tree_get_total_inodes(by_uid);
            
                    svl_printf(verbosity_info, "Completed traversal of %s", argv[optind]);
                    svl_printf(verbosity_info, "  %llu files/directories in %.3f seconds", (unsigned long long int)total_inodes, seconds);
                    svl_printf(verbosity_info, "  %12.0f files/directories per second", (double)total_inodes / seconds);
                }
            
                // Sumarize:
                printf("Total usage:\n");
                switch ( parameter ) {
        
                    case usage_parameter_actual:
                    case usage_parameter_size: {
                        uint64_t    total_bytes = usage_tree_get_total_bytes(by_uid);
                
                        if ( should_show_human_readable ) {
                            printf(
                                   "%20s %24s\n",
                                   "", byte_count_to_string(total_bytes)
                                );
                        } else {
                            printf(
                                   "%20s %24llu\n",
                                   "", (unsigned long long)total_bytes
                                );
                        }
                        break;
                    }
            
                    case usage_parameter_blocks: {
                        uint64_t    total_bytes = usage_tree_get_total_bytes(by_uid);
                
                        printf(
                               "%20s %24llu\n",
                               "", (unsigned long long)total_bytes
                            );
                        break;
                    }
                }
                if ( should_sort ) {
                    svl_printf(verbosity_debug, "Sorting by-uid tree by byte usage");
                    usage_tree_sort(by_uid);
                    svl_printf(verbosity_debug, "Sorting by-gid tree by byte usage\n");
                    usage_tree_sort(by_gid);

                    printf("Usage by-user for %s:\n", argv[optind]);
                    usage_tree_summarize(by_uid, (should_show_numeric_entity_ids ? NULL : uid_to_uname), tree_by_byte_usage, usage_options, parameter);
                    printf("\nUsage by-group for %s:\n", argv[optind]);
                    usage_tree_summarize(by_gid, (should_show_numeric_entity_ids ? NULL : gid_to_gname), tree_by_byte_usage, usage_options, parameter);
                } else {
                    printf("Usage by-user for %s:\n", argv[optind]);
                    usage_tree_summarize(by_uid, (should_show_numeric_entity_ids ? NULL : uid_to_uname), tree_by_entity_id, usage_options, parameter);
                    printf("\nUsage by-group for %s:\n", argv[optind]);
                    usage_tree_summarize(by_gid, (should_show_numeric_entity_ids ? NULL : gid_to_gname), tree_by_entity_id, usage_options, parameter);
                }
            }
        }
        if ( root ) fs_path_free(root);
        if ( wqueue ) work_queue_free(wqueue);

#ifdef HAVE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        if ( mpi_rank == 0 ) printf("\n");
#else
        printf("\n");
#endif

        // Move on to the next path to scan:
        optind++;
    }

#ifdef HAVE_MPI
    MPI_Finalize();
#endif

    return rc;
}
