# dubug

The **dubug** program — an acronym for [du] [b]y [u]ser and [g]roup — walks one or more file paths and accumulates usage by the owning user (uid) and group (gid).  It then summarizes disk usage (in descending order) for the users and groups discovered during the traversal:

```
$ dubug --numeric --progress /a/directory
... 8192 items scanned...
... 16384 items scanned...
... 24576 items scanned...
Total usage:
                                   2606144363
Usage by-user for /a/directory:
                   0                        5
                1001               2606144358

Usage by-group for /a/directory:
                1001               2606144363
```

There are varying levels of verbosity available.  By default (without `--numeric`) the uid and gid numbers are mapped to names.  Built-in help is available:

```
$ dubug --help
usage -- [du] [b]y [u]ser and [g]roup

    ./dubug {options} <path> {<path> ..}

  options:

    --help/-h              show this help info
    --verbose/-v           increase amount of output shown during execution
    --quiet/-q             decrease amount of output shown during execution
    --human-readable/-H    display usage with units, not as bytes
    --numeric/-n           do not resolve numeric uid/gid to names
    --progress/-p          display item-scanned counts as the traversal is
                           executing

```

## Building the Program

The program consists of a single source file, so feel free to just do

```
$ cc -o dubug dubug.c
```

A CMake build configuration is present, as well:

```
$ mkdir build ; cd build
$ cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..
   :
$ make
   :
$ make install
```
