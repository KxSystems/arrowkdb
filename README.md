# arrowkdb

![Arrow](apache_arrow.png)

[![GitHub release (latest by date)](https://img.shields.io/github/v/release/kxsystems/arrowkdb?include_prereleases)](https://github.com/kxsystems/arrowkdb/releases) [![Travis (.com) branch](https://travis-ci.com/KxSystems/arrowkdb.svg?branch=main)](https://travis-ci.com/KxSystems/arrowkdb)


## Introduction

This interface allows kdb+ to users read and write Apache Arrow data stored in:

- Apache Parquet file format
- Arrow IPC record batch file format
- Arrow IPC record batch stream format

This is part of the [*Fusion for kdb+*](http://code.kx.com/q/interfaces/fusion/) interface collection.



## New to kdb+ ?

Kdb+ is the world's fastest time-series database, optimized for  ingesting, analyzing and storing massive amounts of structured data. To  get started with kdb+, please visit https://code.kx.com/q/ for downloads and developer information. For general information, visit https://kx.com/



## New to Apache Arrow?

Apache Arrow is a software development platform for building high performance applications that process and transport large data sets. It is designed to both improve the performance of analytical algorithms and the efficiency of moving data from one system (or programming language to another).

A critical component of Apache Arrow is its **in-memory columnar format**, a standardized, language-agnostic specification for representing structured, table-like datasets in-memory. This data format has a rich data type system (included nested data types) designed to support the needs of analytic database systems, data frame libraries, and more.



## What is the difference between Apache Arrow and Apache Parquet?

Parquet is a storage format designed for maximum space efficiency, using advanced compression and encoding techniques. It is ideal when wanting  to minimize disk usage while storing gigabytes of data, or perhaps more. This efficiency comes at the cost of relatively expensive reading into memory, as Parquet data cannot be directly operated on but must be  decoded in large chunks.

Conversely, Arrow is an in-memory format meant for direct and efficient use for computational purposes. Arrow data is not compressed but laid out in  natural format for the CPU, so that data can be accessed at arbitrary places at full speed.  Therefore, Arrow and Parquet complement each other with Arrow being used as the in-memory data structure for deserializing Parquet data.



## Installation

### Requirements

- kdb+ ≥ 3.5 64-bit (Linux/MacOS/Windows)
- Apache Arrow ≥ 9.0.0 (or ≥ 6.0.0 if building `arrowkdb` from source)
- C++14 or later
- CMake ≥ 3.1.3

> :warning: If using the packaged version of `arrowkdb` you should install version 9.0.0 of Apache Arrow


### Third-party library installation

#### Linux

Follow the instructions [here](https://arrow.apache.org/install/#c-and-glib-c-packages-for-debian-gnulinux-ubuntu-and-centos) to install `libarrow-dev` and `libparquet-dev` from Apache's APT or Yum repositories.

Note: If using the packaged version of `arrowkdb` you should install version 9.0.0 of both:

```bash
sudo apt install -y -V libarrow-dev=9.0.0-1
sudo apt install -y -V libparquet-dev=9.0.0-1
```

#### macOS

Follow the instructions [here](https://arrow.apache.org/install/#c-and-glib-c-packages-on-homebrew) to install `apache-arrow` using Homebrew.

#### Windows

On Windows it is necessary to build Arrow from source.  Full details are provided [here](https://arrow.apache.org/docs/developers/cpp/windows.html) but the basic steps are as follows.

From a Visual Studio command prompt, clone the Arrow source from github:

```bash
C:\Git> git clone https://github.com/apache/arrow.git
C:\Git> cd arrow
```

Switch to the `9.0.0` tag:

```bash
C:\Git\arrow> git checkout refs/tags/apache-arrow-9.0.0 --
C:\Git> cd cpp
```

Create an install directory and set an environment variable to this directory (substituting the correct absolute path as appropriate).  This environment variable is used again later when building `arrowkdb`:

```bash
C:\Git\arrow\cpp> mkdir install
C:\Git\arrow\cpp> set ARROW_INSTALL=C:\Git\arrow\cpp\install
```

Create the CMake build directory and generate the build files (this will default to using the Visual Studio CMake generator when run from a VS command prompt):

```bash
C:\Git\arrow\cpp> mkdir build
C:\Git\arrow\cpp> cd build
C:\Git\arrow\cpp\build> cmake .. -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON -D ARROW_WITH_BZ2=ON -DARROW_BUILD_STATIC=OFF -DARROW_COMPUTE=OFF -DARROW_DEPENDENCY_USE_SHARED=OFF -DCMAKE_INSTALL_PREFIX=%ARROW_INSTALL%
```

Build and install Arrow:

```bash
C:\Git\arrow\cpp\build> cmake --build . --config Release
C:\Git\arrow\cpp\build> cmake --build . --config Release --target install
```

Copy the Arrow, Parquet and compression DLLs in the `%QHOME%\w64` directory:

```bash
C:\Git\arrow\cpp\build> copy release\Release\*.dll %QHOME%\w64
```



### Installing a release

It is recommended that a user install this interface through a release. This is completed in a number of steps:

1. Ensure you have downloaded/installed the Arrow C++ API following the [instructions](#third-party-library-installation).
2. [Download a release](https://github.com/KxSystems/arrowkdb/releases) for your system architecture.
3. Install script `arrowkdb.q` to `$QHOME`, and binary file `lib/arrowkdb.(so|dll)` to `$QHOME/[mlw](64)`, by executing the following from the Release directory:

```bash
## Linux/macOS
chmod +x install.sh && ./install.sh

## Windows
install.bat
```



### Building and installing from source

In order to successfully build and install this interface from source, the following environment variables must be set:

1. `ARROW_INSTALL` = Location of the Arrow C++ API release (only required if Arrow is not installed globally on the system, e.g. on Windows where Arrow was built from source)
2. `QHOME` = Q installation directory (directory containing `q.k`)

From a shell prompt (on Linux/macOS) or Visual Studio command prompt (on Windows), clone the `arrowkdb` source from github:

```bash
git clone https://github.com/KxSystems/arrowkdb.git
cd arrowkdb
```

Create the CMake build directory and generate the build files (this will use the system's default CMake generator):

```bash
mkdir build
cd build

## Linux/MacOS
cmake ..

## Windows (using the Arrow installation which was build from source as above)
cmake .. -DARROW_INSTALL=%ARROW_INSTALL%
```

Start the build:

```bash
cmake --build . --config Release
```

Create the install package and deploy:

```bash
cmake --build . --config Release --target install
```



## Documentation

Documentation outlining the functionality available for this interface can be found in the [`docs`](docs/index.md) folder.



## Status

The arrowkdb interface is provided here under an Apache 2.0 license.

If you find issues with the interface or have feature requests, please consider [raising an issue](https://github.com/KxSystems/arrowkdb/issues).

If you wish to contribute to this project, please follow the [contribution guide](CONTRIBUTING.md).
