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
<<<<<<< HEAD
        { "work-queue-size",    required_argument,  NULL,   'Q' },
        { "work-queue-split",   required_argument,  NULL,   'd' },
        { "work-queue-summary", no_argument,        NULL,   'w' },
=======
        { "max-depth",          required_argument,  NULL,   'D' },
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
#endif
        { NULL,                 0,                  NULL,    0  }
    };
const char *cli_options_str = "hVqvHnSP:"
#ifdef HAVE_MPI
<<<<<<< HEAD
                "Q:d:w"
#endif
;

//

bool
parse_work_queue_size(
    int             mpi_rank,
    const char      *work_queue_size_str,
    unsigned int    *work_queue_by,
    unsigned int    *work_queue_by_min_count
)
{
    bool            is_okay = false;
    char            *p;
    
    if ( (p = strstr(work_queue_size_str, "path-count")) == work_queue_size_str ) {
        p += strlen("path-count");
        switch ( *p ) {
            case '=': {
                char*   p_end;
                long    i = strtol(++p, &p_end, 0);
                
                if ( (p_end > p) && (i >= 0) ) {
                    *work_queue_by_min_count = i;
                } else {
                    if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Invalid work queue size specification (bad path count): %s", work_queue_size_str);
                    is_okay = false;
                    break;
                }
            }
            case '\0':
                *work_queue_by = work_queue_build_by_path_count;
                is_okay = true;
                break;
            default:
                if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Invalid work queue size specification: %s", work_queue_size_str);
                break;
        }
    }
    else if ( (p = strstr(work_queue_size_str, "depth")) == work_queue_size_str ) {
        p += strlen("depth");
        switch ( *p ) {
            case '=': {
                char*   p_end;
                long    i = strtol(++p, &p_end, 0);
                
                if ( (p_end > p) && (i >= 0) ) {
                    *work_queue_by_min_count = i;
                } else {
                    if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Invalid work queue size specification (bad depth): %s", work_queue_size_str);
                    is_okay = false;
                    break;
                }
                *work_queue_by = work_queue_build_by_path_depth;
                is_okay = true;
                break;
            }
            case '\0':
                if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Inavlid work queue size specification (depth requires count): %s", work_queue_size_str);
                break;
            default:
                if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Invalid work queue size specification: %s", work_queue_size_str);
                break;
        }
    }
    else {
        if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Invalid work queue size specification: %s", work_queue_size_str);
    }
    return is_okay;
}

//

enum {
    work_queue_split_contiguous = 0,
    work_queue_split_strided,
    work_queue_split_randomized
};

bool
parse_work_queue_split(
    int             mpi_rank,
    const char      *work_queue_split_str,
    unsigned int    *work_queue_split
)
{
    bool            is_okay = true;
    
    if ( ! strcmp(work_queue_split_str, "default") || ! strcmp(work_queue_split_str, "contiguous") ) {
        *work_queue_split = work_queue_split_contiguous;
    }
    else if ( ! strcmp(work_queue_split_str, "strided") ) {
        *work_queue_split = work_queue_split_strided;
    }
    else if ( ! strcmp(work_queue_split_str, "randomized") ) {
        *work_queue_split = work_queue_split_randomized;
    }
    else {
        if ( mpi_rank == 0 ) svl_printf(verbosity_error, "Invalid work queue split: %s", work_queue_split_str);
        is_okay = false;
    }
    return is_okay;
}
=======
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
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70

//

bool
<<<<<<< HEAD
parse_paramter(
    const char      *parameter_name,
    unsigned int    *parameter
)
{
    unsigned int    p = usage_parameter_from_string(parameter_name);
    if (p != usage_parameter_max) {
        *parameter = p;
        return true;
    }
    return false;
=======
set_parameter(
    const char      *parameter_name
)
{
    parameter = usage_parameter_from_string(parameter_name);
    return (parameter != usage_parameter_max);
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
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
<<<<<<< HEAD
            "    --help/-h                    show this help info\n"
            "    --version/-V                 show the program version\n"
            "    --verbose/-v                 increase amount of output shown during execution\n"
            "    --quiet/-q                   decrease amount of output shown during execution\n"
            "    --human-readable/-H          display usage with units, not as bytes\n"
            "    --numeric/-n                 do not resolve numeric uid/gid to names\n"
            "    --unsorted/-S                do not sort by byte usage before summarizing\n"
            "    --parameter/-P <param>       sizing field over which to sum:\n"
            "\n"
            "                                     actual      bytes on disk (the default)\n"
            "                                     st_size     nominal size (possibly sparse)\n"
            "                                     st_blocks   block count\n"
            "\n"
#ifdef HAVE_MPI
            "  parallel options:\n\n"
            "    --work-queue-size/-Q <arg>   select work queue filling algorithm:\n"
            "\n"
            "                                     path-count{=<N>}  minimum number of paths\n"
            "                                                       (default <N> = MPI ranks)\n"
            "                                     depth=<N>         minimum directory depth\n"
            "\n"
            "                                 path-count is the default algorithm\n"
            "\n"
            "    --work-queue-split/-d <arg>  select the method for distributing the work queue\n"
            "                                 to MPI ranks:\n"
            "\n"
            "                                     contiguous (default)\n"
            "                                     strided\n"
            "                                     randomized\n"
            "\n"
            "    --work-queue-summary/-w      each rank displays a CSV list of the directories\n"
            "                                 it will process\n"
            "\n"
=======
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
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
#endif
            "  <path> can be an absolute or relative file system path to a directory or\n"
            "  file (not very interesting), and for each <path> the traversal is repeated\n"
            "  (rather than aggregating the sum over the paths).\n"
            "\n",
<<<<<<< HEAD
            exe
=======
            exe,
            (unsigned long long int)DEFAULT_PROGRESS_STRIDE
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
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

<<<<<<< HEAD
#ifdef HAVE_MPI

bool
work_queue_filter_index_and_stride(
    unsigned int    idx,
    fs_path_ref     path,
    const void      *context
)
{
    unsigned int    *stride = (unsigned int*)context;
    return ((idx % *stride) != 0) ? true : false;
}

#endif

//

=======
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
int
main(
    int             argc,
    char            **argv
)
{
    int             opt, rc = 0;
<<<<<<< HEAD
    bool            should_show_human_readable = false;
    bool            should_show_numeric_entity_ids = false;
    bool            should_sort = true;
    unsigned int    parameter = usage_parameter_actual;
#ifdef HAVE_MPI
    bool            should_summarize_work_queue = false;
    int             thread_mode, mpi_rank, mpi_size;
    unsigned int    work_queue_build_by = work_queue_build_by_path_count;
    unsigned int    work_queue_build_by_min_count = 0;
    unsigned int    work_queue_split = work_queue_split_contiguous;
=======
#ifdef HAVE_MPI
    int             thread_mode, mpi_rank, mpi_size;
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
    
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
<<<<<<< HEAD
    work_queue_build_by_min_count = mpi_size;
=======
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
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
<<<<<<< HEAD
                break;
            
            case 'P':
                if ( ! parse_paramter(optarg, &parameter) ) {
                    svl_printf(verbosity_error, "Invalid argument to --parameter/-P: %s", optarg);
                    exit(EINVAL);
                }
                break;

#ifdef HAVE_MPI
            case 'Q':
                if ( ! parse_work_queue_size(mpi_rank, optarg, &work_queue_build_by, &work_queue_build_by_min_count) ) exit(EINVAL);
                break;
            
            case 'd':
                if ( ! parse_work_queue_split(mpi_rank, optarg, &work_queue_split) ) exit(EINVAL);
=======
                break;
            
            case 'P':
                if ( ! set_parameter(optarg) ) {
                    svl_printf(verbosity_error, "Invalid argument to --parameter/-P: %s", optarg);
                    exit(EINVAL);
                }
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
                break;
            
            case 'w':
                should_summarize_work_queue = true;
                break;
#endif

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
<<<<<<< HEAD
                is_okay = work_queue_build(wqueue, root, work_queue_build_by, work_queue_build_by_min_count);
=======
                is_okay = work_queue_build(wqueue, root, mpi_size);
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
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
<<<<<<< HEAD
                        switch ( work_queue_split ) {
                            case work_queue_split_randomized: {
                                // Randomize the path list before proceeding:
                                work_queue_randomize(wqueue, 3);
                            }
                            case work_queue_split_contiguous: {
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
                                break;
                            }
                            case work_queue_split_strided: {
                                unsigned int        path_count, stride = mpi_size;
                                
                                // We will distribute work to all other ranks first, leaving the root
                                // rank for last.  If we run out of work then some ranks will have nothing to
                                // do -- including the root rank:
                                while ( ((path_count = work_queue_get_path_count(wqueue)) > 0)  && (target_rank < mpi_size) ) {
                                    unsigned int    out_queue_size;
                                    sbb_ref         serialized_queue = work_queue_serialize_index_and_stride(wqueue, 0, stride, &out_queue_size);
                                    
                                    svl_printf(verbosity_debug, "Distribution pass with path_count = %u, stride = %u\n", path_count, stride);
                                    if ( serialized_queue ) {
                                        uint64_t    serialized_queue_len = sbb_get_length(serialized_queue);
                                
                                        svl_printf(verbosity_debug, "Sending work queue size %llu to rank %d", serialized_queue_len, target_rank);
                                        MPI_Send(&serialized_queue_len, 1, MPI_UINT64_T, target_rank, 10, MPI_COMM_WORLD);
                                        MPI_Send(sbb_get_buffer_ptr(serialized_queue), serialized_queue_len, MPI_UINT8_T, target_rank, 11, MPI_COMM_WORLD);
                                        sbb_free(serialized_queue);
                                        
                                        // Filter-out what we just sent:
                                        work_queue_filter(wqueue, work_queue_filter_index_and_stride, &stride);
                                        target_rank++;
                                        stride--;
                                    }
                                    queue_idx++;
                                }
                                svl_printf(verbosity_debug, "Work remaining for root rank is %u path(s)\n", work_queue_get_path_count(wqueue));
                                
                        
                                // Any remaining ranks should be told they have nothing to do:
                                while ( target_rank < mpi_size ) {
                                    uint64_t    serialized_queue_len = 0;
                                    svl_printf(verbosity_debug, "Sending non-work to rank %d", target_rank);
                                    MPI_Send(&serialized_queue_len, 1, MPI_UINT64_T, target_rank++, 10, MPI_COMM_WORLD);
                                }
                                break;
                            }
                        }
                    }
                    // Summarize what's in our queue if requested:
                    if ( should_summarize_work_queue ) {
                        const char  *to_csv = work_queue_to_csv_string(wqueue);
                        
                        if ( to_csv ) {
                            printf("Rank %d work queue:  %s\n", mpi_rank, to_csv);
                            free((void*)to_csv);
                        } else {
                            svl_printf(verbosity_error, "Unable to summarize work queue");
=======
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
>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
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
<<<<<<< HEAD
                // Summarize what's in our queue if requested:
                if ( should_summarize_work_queue ) {
                    const char  *to_csv = work_queue_to_csv_string(wqueue);
                    
                    if ( to_csv ) {
                        printf("Rank %d work queue:  %s\n", mpi_rank, to_csv);
                        free((void*)to_csv);
                    } else {
                        svl_printf(verbosity_error, "Unable to summarize work queue");
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
            is_okay = work_queue_build(wqueue, root, work_queue_build_by_path_depth, 1);
            
            if ( is_okay ) {
                svl_printf(verbosity_info, "Starting traversal of work queue");
                work_queue_complete(wqueue);
                svl_printf(verbosity_info, "Completed work queue");
                
                clock_gettime(CLOCK_BOOTTIME, &end_time);
#endif

=======

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

>>>>>>> 374ad3da638feb4cfd5fe549b1095b61767b1d70
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
