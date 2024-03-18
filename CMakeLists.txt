cmake_minimum_required(VERSION 3.12)

project(DUBUG
    VERSION 1.0.0
    DESCRIPTION "[du] [b]y [u]ser and [g]roup"
    LANGUAGES C
  )

include(GNUInstallDirs)

option(ENABLE_MPI_PARALLEL "Enable MPI parallelism" OFF)
if (ENABLE_MPI_PARALLEL)
    find_package(MPI REQUIRED)
endif ()

option(ENABLE_SBB_TEST "Enable compile of sbb_test program" OFF)
option(ENABLE_FS_PATH_TEST "Enable compile of fs_path_test program" OFF)
option(ENABLE_WORK_QUEUE_TEST "Enable compile of work_queue_test program" OFF)

add_executable(dubug
        byte_count_to_string.c
        simple_verbose_logging.c
        sbb.c
        fs_path.c
        usage_tree.c
        work_queue.c
        dubug.c
    )
target_include_directories(dubug PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
if (MPI_FOUND)
    set(HAVE_MPI 1)
    target_link_libraries(dubug ${MPI_C_LIBRARIES})
    target_include_directories(dubug PRIVATE ${MPI_C_HEADER_DIR})
endif ()
configure_file(config.h.in config.h)
install(TARGETS dubug RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

if (ENABLE_SBB_TEST)
    add_executable(sbb_test
            simple_verbose_logging.c
            sbb.c
            sbb_test.c
        )
    if (MPI_FOUND)
        target_link_libraries(sbb_test ${MPI_C_LIBRARIES})
        target_include_directories(sbb_test PRIVATE ${MPI_C_HEADER_DIR})
    endif ()
    target_include_directories(sbb_test PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
endif ()
if (ENABLE_FS_PATH_TEST)
    add_executable(fs_path_test
            simple_verbose_logging.c
            sbb.c
            fs_path.c
            fs_path_test.c
        )
    if (MPI_FOUND)
        target_link_libraries(fs_path_test ${MPI_C_LIBRARIES})
        target_include_directories(fs_path_test PRIVATE ${MPI_C_HEADER_DIR})
    endif ()
    target_include_directories(fs_path_test PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
endif()
if (ENABLE_WORK_QUEUE_TEST)
    add_executable(work_queue_test
            byte_count_to_string.c
            simple_verbose_logging.c
            sbb.c
            fs_path.c
            usage_tree.c
            work_queue.c
            work_queue_test.c
        )
    if (MPI_FOUND)
        target_link_libraries(work_queue_test ${MPI_C_LIBRARIES})
        target_include_directories(work_queue_test PRIVATE ${MPI_C_HEADER_DIR})
    endif ()
    target_include_directories(work_queue_test PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
endif()
