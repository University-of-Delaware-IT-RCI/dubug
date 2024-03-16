
#include "work_queue.h"
#include "simple_verbose_logging.h"

#ifdef HAVE_MPI
# include "mpi.h"
#endif

int
main(
    int             argc,
    char            **argv
)
{
    int         rc = 0, argn = 1;
#ifdef HAVE_MPI
    int             thread_mode, mpi_rank, mpi_size;
    
    rc = MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thread_mode);
    if ( rc != MPI_SUCCESS ) {
        svl_printf(verbosity_error, "unable to initialize MPI (rc = %d)", rc);
        exit(1);
    }
    rc = MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    if ( rc != MPI_SUCCESS ) {
        svl_printf(verbosity_error, "unable to determine MPI rank (rc = %d)", rc);
        MPI_Finalize();
        exit(1);
    }
    rc = MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    if ( rc != MPI_SUCCESS ) {
        svl_printf(verbosity_error, "unable to determine MPI size (rc = %d)", rc);
        MPI_Finalize();
        exit(1);
    }
    svl_printf(verbosity_info, "MPI rank %d of %d initialized", mpi_rank, mpi_size);
#endif

    while ( argn < argc ) {
        work_queue_ref  wq = work_queue_alloc(usage_parameter_actual);
        int             queue_depth = strtol(argv[argn++], NULL, 10);
        fs_path_ref     root = fs_path_alloc_with_cstring_const(argv[argn]);
        
        if ( work_queue_build(wq, root, queue_depth) ) {
            printf("Work queue of minimum size %d generated for %s:\n", queue_depth, argv[argn]);
            work_queue_summary(wq);
            
            svl_printf(verbosity_debug, "  Sorting by-uid tree by byte usage");
            usage_tree_sort(work_queue_get_by_uid_usage_tree(wq));
            svl_printf(verbosity_debug, "  Sorting by-gid tree by byte usage\n");
            usage_tree_sort(work_queue_get_by_gid_usage_tree(wq));
            
            usage_tree_calculate_totals(work_queue_get_by_uid_usage_tree(wq));
            usage_tree_calculate_totals(work_queue_get_by_gid_usage_tree(wq));

            printf("  Usage by-user for %s:\n", argv[argn]);
            usage_tree_summarize(work_queue_get_by_uid_usage_tree(wq), NULL, tree_by_byte_usage, 0, usage_parameter_actual);
            printf("\n  Usage by-group for %s:\n", argv[argn]);
            usage_tree_summarize(work_queue_get_by_gid_usage_tree(wq), NULL, tree_by_byte_usage, 0, usage_parameter_actual);
            
            work_queue_complete(wq);
            printf("Work queue file system traversal complete:\n");
            work_queue_summary(wq);
            
            svl_printf(verbosity_debug, "  Sorting by-uid tree by byte usage");
            usage_tree_sort(work_queue_get_by_uid_usage_tree(wq));
            svl_printf(verbosity_debug, "  Sorting by-gid tree by byte usage\n");
            usage_tree_sort(work_queue_get_by_gid_usage_tree(wq));
            
            usage_tree_calculate_totals(work_queue_get_by_uid_usage_tree(wq));
            usage_tree_calculate_totals(work_queue_get_by_gid_usage_tree(wq));

            printf("  Usage by-user for %s:\n", argv[argn]);
            usage_tree_summarize(work_queue_get_by_uid_usage_tree(wq), NULL, tree_by_byte_usage, 0, usage_parameter_actual);
            printf("\n  Usage by-group for %s:\n", argv[argn]);
            usage_tree_summarize(work_queue_get_by_gid_usage_tree(wq), NULL, tree_by_byte_usage, 0, usage_parameter_actual);
        } else {
            printf("Cest la vie...\n");
            work_queue_summary(wq);
        }
        fs_path_free(root);
        argn++;
    }

#ifdef HAVE_MPI
    MPI_Finalize();
#endif

    return rc;
}
