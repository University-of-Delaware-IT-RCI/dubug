# dubug

The **dubug** program — an acronym for [du] [b]y [u]ser and [g]roup — walks one or more file paths and accumulates usage by the owning user (uid) and group (gid).  It then summarizes disk usage (in descending order) for the users and groups discovered during the traversal:

```
$ dubug --numeric /a/directory
Total usage:
                                  16650290176
Usage by-user for /a/directory:
                1001              16576545792 ( 99.56%)                     96844 (100.00%) @ 171168 B/inode
                   0                 73744384 (  0.44%)                         3 (  0.00%) @ 24581461 B/inode

Usage by-group for /a/directory:
                 900              10140531200 ( 60.90%)                     95932 ( 99.06%) @ 105705 B/inode
                1001               6436014592 ( 38.65%)                       912 (  0.94%) @ 7057034 B/inode
                   0                 73744384 (  0.44%)                         3 (  0.00%) @ 24581461 B/inode
```

There are varying levels of verbosity available.  By default (without `--numeric`) the uid and gid numbers are mapped to names.  Built-in help is available:

```
$ dubug --help
usage -- [du] [b]y [u]ser and [g]roup

    ./dubug {options} <path> {<path> ..}

  options:

    --help/-h                    show this help info
    --version/-V                 show the program version
    --verbose/-v                 increase amount of output shown during execution
    --quiet/-q                   decrease amount of output shown during execution
    --human-readable/-H          display usage with units, not as bytes
    --numeric/-n                 do not resolve numeric uid/gid to names
    --unsorted/-S                do not sort by byte usage before summarizing
    --parameter/-P <param>       sizing field over which to sum:

                                     actual      bytes on disk (the default)
                                     st_size     nominal size (possibly sparse)
                                     st_blocks   block count

```

## Parallel scanning

The program can be compiled with MPI parallelism in order to split the scan workload across multiple ranks concurrently.  This can significantly accelerate the processing of large directory hierarchies, especially for parallel file systems like Lustre.

In serial (and single-rank MPI) mode, the code starts with a *work queue* containing only the base path provided on the command line.  This single-path work queue is then processed using the Linux `fts` API for file system traversal:  for each file system record returned, the byte and inode usage are tallied for the owning uid and gid.  When the entire hierarchy rooted at that directory has been traversed, the total usage breakdown is complete.

In parallel mode, the root rank starts with the same single path in the *work queue*.  However, it generates additional work by removing the path from the work queue and using `fts` to enumerate the file system records in that path, without descending into any subdirectories:  all records are tallied as before, but each directory is added back to the work queue.  If the work queue has been filled sufficiently (according to some condition), the procedure ends here.  Otherwise, it is repeated, with each directory in the work queue being removed, scanned, and subdirectories of it added back into the work queue.

There are two types of filling condition on the work queue:

- The number of paths present in the work queue exceeds some minimum threshold
    - At least equal to the number of MPI ranks
- The number of directory levels traversed exceeds a threshold
    - 0 = the base directory only (the serial case)
    - 1 = all directories inside the base directory
    - etc.

Once the root rank has generated a work queue it has also accumulated some amount of usage information already:  those paths are **not** revisited during the remainder of the algorithm.  The next step is to distribute the work queue to the MPI ranks.  This is accomplished in one of three ways:

- The default, contiguous assignment, splits the list into N<sub>rank</sub> blocks
    - The first block is given to rank 1, the second to rank 2, and the final block to rank 0
    - If not enough paths were produced for all ranks, any deficient ranks are give no work (they will simply block awaiting the others)
- The work queue is randomly sorted so that subsequent paths are likely to be more dissimilar
    - After the sort, the same contiguous assignment algorithm is applied
- Work queue paths are chosen on a stride of N<sub>rank</sub>
    - Rank 1 is assigned paths at index 0, N<sub>rank</sub>, 2N<sub>rank</sub>, etc.
    - Rank 2 is assigned paths at index 1, 1+N<sub>rank</sub>, 1+2N<sub>rank</sub>, etc.
    - Rank 0 is assigned paths at index (N<sub>rank</sub>-1), (2N<sub>rank</sub>-1), etc.
    - If not enough paths were produced for all ranks, any deficient ranks are give no work (they will simply block awaiting the others)

With each MPI rank having its own work queue — comprising a subset of the original work queue — the same `fts` full-depth scan can be done on each path by each rank, accumulating its own usage data.  Note that for rank 0 this will augment whatever usage data was accumulated during the scan that produced the work queue.

Finally, the usage data from each MPI rank must be merged with that held by the root rank.  This is not a simple `MPI_Reduce()` because the data are essentially key-value pairs, and each rank may have different keys.  The most straightforward method is for each non-root rank to generate an array of structured values:

- entity id (either a uid or gid number)
- total byte usage
- total inode usage

The root rank must then do the following:

- For each non-root rank, receive the array size followed by the array of records
    - For each record, lookup the usage info for the given entity id (or create a new usage info record if entity id not present)
        - Aggregate the total byte and inode usage into the usage info record

Once this process is done, the non-root ranks have completed their work.  The root rank can sort its aggregated data and summarize it.

The MPI variant of the program includes documentation for the additional command line options:

```
parallel options:

  --work-queue-size/-Q <arg>   select work queue filling algorithm:

                                   path-count{=<N>}  minimum number of paths
                                                     (default <N> = MPI ranks)
                                   depth=<N>         minimum directory depth

                               path-count is the default algorithm

  --work-queue-split/-d <arg>  select the method for distributing the work queue
                               to MPI ranks:

                                   contiguous (default)
                                   strided
                                   randomized

    --work-queue-summary/-w      each rank displays a CSV list of the directories
                                 it will process
```

## Building the Program

A CMake build configuration is present:

```
$ mkdir build ; cd build
$ cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..
   :
$ make
   :
$ make install
```

If MPI parallelism is desired, be sure the current shell environment has an MPI library available before running the CMake setup:

```
$ cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DENABLE_MPI_PARALLEL=On ..
```
