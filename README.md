# arrowkdb

## Introduction

This interface allows kdb+ to users read and write Apache Arrow data stored in:

- Arrow IPC record batch file format
- Arrow IPC record batch stream format
- Apache Parquet file format

This is part of the [*Fusion for kdb+*](http://code.kx.com/q/interfaces/fusion/) interface collection.



## New to kdb+ ?

Kdb+ is the world's fastest time-series database, optimized for  ingesting, analyzing and storing massive amounts of structured data. To  get started with kdb+, please visit https://code.kx.com/q/learn/ for downloads and developer information. For general information, visit https://kx.com/



## New to Apache Arrow?

Apache Arrow is a software development platform for building high performance applications that process and transport large data sets. It is designed to both improve the performance of analytical algorithms and the efficiency of moving data from one system (or programming language to another).

A critical component of Apache Arrow is its **in-memory columnar format**, a standardized, language-agnostic specification for representing structured, table-like datasets in-memory. This data format has a rich data type system (included nested data types) designed to support the needs of analytic database systems, data frame libraries, and more.



## What is the difference between Apache Arrow and Apache Parquet?

Parquet is a storage format designed for maximum space efficiency, using advanced compression and encoding techniques. It is ideal when wanting  to minimize disk usage while storing gigabytes of data, or perhaps more. This efficiency comes at the cost of relatively expensive reading into memory, as Parquet data cannot be directly operated on but must be  decoded in large chunks.

Conversely, Arrow is an in-memory format meant for direct and efficient use for computational purposes. Arrow data is not compressed but laid out in  natural format for the CPU, so that data can be accessed at arbitrary places at full speed.  Therefore, Arrow and Parquet complement each other with Arrow being used as the in-memory data structure for deserializing Parquet data.



## Installation

### Requirements

- kdb+ ≥ 3.5 64-bit (Linux/MacOS/Windows)
- Apache Arrow ≥ 2.0.0
- C++11 or later [^1]
- CMake [^1]

[^1]: Required when building from source



### Third-Party Library Installation

#### Linux

Follow the instructions [here](https://arrow.apache.org/install/#c-and-glib-c-packages-for-debian-gnulinux-ubuntu-and-centos) to install `libarrow-dev` and `libparquet-dev` from Apache's APT or Yum repositories.

#### MacOS

Follow the instructions [here](https://arrow.apache.org/install/#c-and-glib-c-packages-on-homebrew) to install `apache-arrow` using Homebrew.

#### Windows

Unfortunately the `vckpg` installation currently builds the legacy Arrow version 0.17.1 which is not compatible with the required 2.0.0.  Therefore it is is necessary to build Arrow from source.  Full details are provided [here](https://arrow.apache.org/docs/developers/cpp/windows.html) but the basic steps are as follows:

##### Snappy

First download and build snappy which is required by Parquet.  From a Visual Studio command prompt:

```bash
C:\Git> git clone https://github.com/google/snappy.git
C:\Git> cd snappy
```

Create an install directory and set an environment variable to this directory (substituting the correct absolute path as appropriate).  This environment variable is used again later when building Arrow:

```bash
C:\Git\snappy> mkdir install
C:\Git\snappy> set SNAPPY_INSTALL=C:\Git\snappy\install
```

Create the CMake build directory and generate the build files (this will default to using the Visual Studio CMake generator when run from a VS command prompt):

```bash
C:\Git\snappy> mkdir build
C:\Git\snappy> cd build
C:\Git\snappy\build> cmake -DCMAKE_INSTALL_PREFIX=%SNAPPY_INSTALL% -DSNAPPY_BUILD_BENCHMARKS:BOOL=0 -DSNAPPY_BUILD_TESTS:BOOL=0 ..
```

Build and install snappy:

```bash
C:\Git\snappy\build> cmake --build . --config Release
C:\Git\snappy\build> cmake --build . --config Release --target install
```

##### Arrow

From a Visual Studio command prompt, clone the Arrow source from github:

```bash
C:\Git> git clone https://github.com/apache/arrow.git
C:\Git> cd arrow\cpp
```

Create an install directory and set an environment variable to this directory (substituting the correct absolute path as appropriate).  This environment variable is used again later when building `arrowkdb`:

```bash
C:\Git\arrow\cpp> mkdir install
C:\Git\arrow\cpp> set BUILD_HOME=C:\Git\arrow\cpp\install
```

Create the CMake build directory and generate the build files (this will default to using the Visual Studio CMake generator when run from a VS command prompt), specifying the location of the snappy build as setup above:

```bash
C:\Git\arrow\cpp> mkdir build
C:\Git\arrow\cpp> cd build
C:\Git\arrow\cpp\build> cmake .. -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_BUILD_STATIC=OFF -DSnappy_LIB=%SNAPPY_INSTALL%\lib\snappy.lib -DSnappy_INCLUDE_DIR=%SNAPPY_INSTALL%\include -DCMAKE_INSTALL_PREFIX=%BUILD_HOME% 
```

Build and install Arrow:

```bash
C:\Git\arrow\cpp\build> cmake --build . --config Release
C:\Git\arrow\cpp\build> cmake --build . --config Release --target install
```

Create symlinks to the arrow and parquet DLLs in the `%QHOME%\w64` directory:

```
C:\Git\arrow\cpp\build> MKLINK %QHOME%\w64\arrow.dll %BUILD_HOME%\bin\arrow.dll
C:\Git\arrow\cpp\build> MKLINK %QHOME%\w64\parquet.dll %BUILD_HOME%\bin\parquet.dll
```



### Installing a release

It is recommended that a user install this interface through a release. This is completed in a number of steps:

1. Ensure you have downloaded/installed the Arrow C++ API following the instructions [here](https://github.com/KxSystems/arrowkdb#third-party-library-installation).
2. Download a release from [here](https://github.com/KxSystems/arrowkdb/releases) for your system architecture.
3. Install script `arrowkdb.q` to `$QHOME`, and binary file `lib/arrowkdb.(so|dll)` to `$QHOME/[mlw](64)`, by executing the following from the Release directory:

```
## Linux/MacOS
chmod +x install.sh && ./install.sh

## Windows
install.bat
```



### Building and installing from source

In order to successfully build and install this interface from source, the following environment variables must be set:

1. `BUILD_HOME` = Location of the Arrow C++ API release (only required if Arrow is not installed globally on the system, e.g. on Windows where Arrow was built from source)
2. `QHOME` = Q installation directory (directory containing `q.k`)

From a shell prompt (on Linux/MacOS) or Visual Studio command prompt (on Windows), clone the `arrowkdb` source from github:

```bash
git clone https://github.com/KxSystems/arrowkdb.git
cd arrowkdb
```

Create the CMake build directory and generate the build files (this will use the system's default CMake generator):

```bash
mkdir build
cd build
cmake ..
```

Start the build:

```bash
cmake --build . --config Release
```

Create the install package and deploy:

```bash
cmake --build . --config Release --target install
```



## Building an Arrow Table

An Arrow table is built from a defined schema and the table's data:

- Currently Arrow supports over 35 datatypes including concrete, parameterised and nested datatypes
- A field describes a column in the table and is composed of a datatype and a string field name
- A schema is built up from a list of fields
- The array data for each column in the table is then populated using a builder object specific to that field's datatype
- Similarly datatype specific reader objects are used to interpret and inspect the array data for each column in the table



## Arrow Datatypes and kdb+ mappings

Currently Arrow supports over 35 datatypes including concrete, parameterised and nested datatypes.

Similar to pyarrow, `arrowkdb` exposes the Arrow datatype constructors to q.  When one of these constructors is called it will return an integer datatype identifier which can then be passed to other functions, e.g. when creating a field.

### Concrete Datatypes

These cover the datatypes where there is a single fixed representation of that datatype.

| **Arrow Datatype** | **Description**                                         | **Kdb+ representation of Arrow array**              |
| ------------------ | ------------------------------------------------------- | --------------------------------------------------- |
| null               | NULL type having no physical storage                    | Mixed list of empty lists                           |
| boolean            | Boolean as 1 bit, LSB bit-packed ordering               | KB                                                  |
| uint8              | Unsigned 8-bit little-endian integer                    | KG                                                  |
| int8               | Signed 8-bit little-endian integer                      | KG                                                  |
| uint16             | Unsigned 16-bit little-endian integer                   | KH                                                  |
| int16              | Signed 16-bit little-endian integer                     | KH                                                  |
| uint32             | Unsigned 32-bit little-endian integer                   | KI                                                  |
| int32              | Signed 32-bit little-endian integer                     | KI                                                  |
| uint64             | Unsigned 64-bit little-endian integer                   | KJ                                                  |
| int64              | Signed 64-bit little-endian integer                     | KJ                                                  |
| float16            | 2-byte floating point value (populated from uint16_t)   | KH                                                  |
| float32            | 4-byte floating point value                             | KE                                                  |
| float64            | 8-byte floating point value                             | KF                                                  |
| utf8               | UTF8 variable-length string                             | Mixed list of KC lists                              |
| large_utf8         | Large UTF8 variable-length string                       | Mixed list of KC lists                              |
| binary             | Variable-length bytes (no guarantee of UTF8-ness)       | Mixed list of KG lists                              |
| large_binary       | Large variable-length bytes (no guarantee of UTF8-ness) | Mixed list of KG lists                              |
| date32             | int32_t days since the UNIX epoch                       | KD (with automatic epoch offsetting)                |
| date64             | int64_t milliseconds since the UNIX epoch               | KP (with automatic epoch offsetting and ms scaling) |
| month_interval     | Interval described as a number of months                | KM                                                  |
| day_time_interval  | Interval described as number of days and milliseconds   | KN (with automatic ns scaling)                      |



### Parameterised Datatypes

These represent multiple logical interpretations of the underlying physical data, where each parameterised interpretation is a distinct datatype in its own right.

| **Arrow Datatype**            | **Description**                                              | **Kdb+ representation of Arrow array**                    |
| ----------------------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| fixed_size_binary(byte_width) | Fixed-size binary. Each value occupies the same number of bytes. | Mixed list of KG lists.                                   |
| timestamp(time_unit)          | Exact timestamp encoded with int64_t (as number of seconds, milliseconds, microseconds or nanoseconds since UNIX epoch) | KP (with automatic epoch offsetting and TimeUnit scaling) |
| time32(time_unit)             | Time as signed 32-bit integer, representing either seconds or milliseconds since midnight | KT (with automatic TimeUnit scaling)                      |
| time64(time_unit)             | Time as signed 64-bit integer, representing either microseconds or nanoseconds since midnight | KN (with automatic TimeUnit scaling)                      |
| duration(time_unit)           | Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds | KN (with automatic TimeUnit scaling)                      |
| decimal128(precision, scale)  | Precision- and scale-based signed 128-bit integer in two's complement | Mixed list of KG lists (each of length 16)                |
| decimal256(precision, scale)  | Precision- and scale-based signed 256-bit integer in two's complement | Mixed list of KG lists (each of length 32)                |



### Nested Datatypes

These are used to define higher level groupings of either the child datatypes or its constituent fields (a field specifies its datatype and the field's name).

| **Arrow Datatype**                      | **Description**                                              | **Kdb+ representation of Arrow array**                       |
| --------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| list(datatype_id)                       | List datatype specified in terms of its child datatype       | Mixed list for the parent list array containing a set of sub-lists (of type determined by the child datatype), one for each of the list value sets |
| large_list(datatype_id)                 | Large list datatype specified in terms of its child datatype | Mixed list for the parent list array containing a set of sub-lists (of type determined by the child datatype), one for each of the list value sets |
| fixed_size_list(datatype_id, list_size) | Fixed size list datatype specified in terms of its child datatype and the fixed size of each of the child lists | Same as variable length lists, except each of the sub-lists must be of length equal to the list_size |
| map(key_datatype_id, item_datatype_id)  | Map datatype specified in terms of its key and item child datatypes | Mixed list for the parent map array, with a dictionary for each map value set |
| struct(field_ids)                       | Struct datatype specified in terms of a list of its constituent child field identifiers | Mixed list for the parent struct array, containing child lists for each field in the struct |
| sparse_union(field_ids)                 | Union datatype specified in terms of a list of its constituent child field identifiers | Similar to a struct array except the mixed list has an additional type_id array (KH list) at the start which identifies the live field in each union value set |
| dense_union(field_ids)                  | Union datatype specified in terms of a list of its constituent child field identifiers | Similar to a struct array except the mixed list has an additional type_id array (KH list) at the start which identifies the live field in each union value set |



## Arrow Fields

An Arrow field describes a column in the table and is composed of a datatype and a string field name.

Similar to pyarrow, `arrowkdb` exposes the Arrow field constructor to q.  The field constructor takes the field name and its datatype identifier and will return an integer field identifier which can then be passed to other functions, e.g. when creating a schema.



## Arrow Schemas

An Arrow schema is built up from a list of fields and is used when working with table data.  The datatype of each field in the schema determines the array data layout for that column in the table.

Similar to pyarrow, `arrowkdb` exposes the Arrow schema constructor to q.  The schema constructor takes a list of field identifiers and will return an integer schema identifier which can then be passed to other functions, e.g. when writing Arrow or Parquet files.



## Arrow Tables

An Arrow table is composed from a schema and a mixed list of Arrow array data kdb objects:

- The array data for each column in the table is then populated using a builder object specific to that field's datatype
- Similarly datatype specific reader objects are used to interpret and inspect the array data for each column in the table

The mixed list of Arrow array data kdb objects should be ordered in schema field number.  Each kdb object representing one of the arrays must be structured according to the field's datatype.  This required array data structure is detailed above for each of the datatypes.



## Function Reference

### Datatype Constructors

#### **`null`**

*Create a NULL datatype*

```q
.arrowkdb.null[]
```

Returns the datatype identifier

### **`boolean`**

*Create a boolean datatype*

```q
.arrowkdb.boolean[]
```

Returns the datatype identifier

### **`uint8`**

*Create an uint8 datatype*

```q
.arrowkdb.uint8[]
```

Returns the datatype identifier

### **`int8`**

*Create an int8 datatype*

```q
.arrowkdb.int8[]
```

Returns the datatype identifier

### **`uint16`**

*Create an uint16 datatype*

```q
.arrowkdb.uint16[]
```

Returns the datatype identifier

### **`int16`**

*Create an int16 datatype*

```q
.arrowkdb.int16[]
```

Returns the datatype identifier

### **`uint32`**

*Create an uint32 datatype*

```q
.arrowkdb.uint32[]
```

Returns the datatype identifier

### **`int32`**

*Create an int32 datatype*

```q
.arrowkdb.int32[]
```

Returns the datatype identifier

### **`uint64`**

*Create an uint64 datatype*

```q
.arrowkdb.uint64[]
```

Returns the datatype identifier

### **`int64`**

*Create an int64 datatype*

```q
.arrowkdb.int64[]
```

Returns the datatype identifier

### **`float16`**

*Create a float16 (represented as uint16_t) datatype*

```q
.arrowkdb.float16[]
```

Returns the datatype identifier

### **`float32`**

*Create a float32 datatype*

```q
.arrowkdb.float32[]
```

Returns the datatype identifier

### **`float64`**

*Create a float64 datatype*

```q
.arrowkdb.float64[]
```

Returns the datatype identifier

### **`utf8`**

*Create a UTF8 variable length string datatype*

```q
.arrowkdb.utf8[]
```

Returns the datatype identifier

### **`large_utf8`**

*Create a large (64 bit offsets) UTF8 variable length string datatype*

```q
.arrowkdb.large_utf8[]
```

Returns the datatype identifier

### **`binary`**

*Create a variable length bytes datatype*

```q
.arrowkdb.binary[]
```

Returns the datatype identifier

### **`large_binary`**

*Create a large (64 bit offsets) variable length bytes datatype*

```q
.arrowkdb.large_binary[]
```

Returns the datatype identifier

### **`fixed_size_binary`**

*Create a fixed width bytes datatype*

```q
.arrowkdb.fixed_size_binary[byte_width]
```

Where `byte_width` is the int32 fixed size byte width (each value in the array occupies the same number of bytes).

returns the datatype identifier

### **`date32`**

*Create a 32 bit date (days since UNIX epoch) datatype*

```q
.arrowkdb.date32[]
```

Returns the datatype identifier

### **`date64`**

*Create a 64 bit date (milliseconds since UNIX epoch) datatype*

```q
.arrowkdb.date64[]
```

Returns the datatype identifier

### **`timestamp`**

*Create a 64 bit timestamp (units since UNIX epoch with specified granularity) datatype*

```q
.arrowkdb.timestamp[time_unit]
```

Where `time_unit` is the time unit string: SECOND, MILLI, MICRO or NANO

returns the datatype identifier

### **`time32`**

*Create a 32 bit time (units since midnight with specified granularity) datatype*

```q
.arrowkdb.time32[time_unit]
```

Where `time_unit` is the time unit string: SECOND or MILLI

returns the datatype identifier

### **`time64`**

*Create a 64 bit time (units since midnight with specified granularity) datatype*

```q
.arrowkdb.time64[time_unit]
```

Where `time_unit` is the time unit string: MICRO or NANO

returns the datatype identifier

### **`month_interval`**

*Create a 32 bit interval (described as a number of months, similar to YEAR_MONTH in SQL) datatype*

```q
.arrowkdb.month_interval[]
```

Returns the datatype identifier

### **`day_time_interval`**

*Create a 64 bit interval (described as a number of days and milliseconds, similar to DAY_TIME in SQL) datatype*

```q
.arrowkdb.day_time_interval[]
```

Returns the datatype identifier

### **`duration`**

*Create a 64 bit duration (measured in units of specified granularity) datatype*

```q
.arrowkdb.duration[time_unit]
```

Where `time_unit` is the time unit string: SECOND, MILLI, MICRO or NANO

returns the datatype identifier

### **`decimal128`**

*Create a 128 bit integer (with precision and scale in two's complement) datatype*

```q
.arrowkdb.decimal128[precision;scale]
```

Where:

- `precision` is the int32 precision width
- `scale` is the int32 scaling factor

returns the datatype identifier

### **`list`**

*Create a list datatype, specified in terms of its child datatype*

```q
.arrowkdb.list[child_datatype_id]
```

Where `child_datatype_id` is the identifier of the list's child datatype

returns the datatype identifier

### **`large_list`**

*Create a large (64 bit offsets) list datatype, specified in terms of its child datatype*

```q
.arrowkdb.large_list[child_datatype_id]
```

Where `child_datatype_id` is the identifier of the list's child datatype

returns the datatype identifier

### **`fixed_size_list`**

*Create a fixed size list datatype, specified in terms of its child datatype*

```q
.arrowkdb.fixed_size_list[child_datatype_id;list_size]
```

Where:

- `child_datatype_id` is the identifier of the list's child datatype
- `list_size`  is the int32 fixed size of each of the child lists

returns the datatype identifier

### **`map`**

*Create a map datatype, specified in terms of its key and item child datatypes*

```q
.arrowkdb.map[key_datatype_id;item_datatype_id]
```

Where:

- `key_datatype_id` is the identifier of the map key child datatype
- `item_datatype_id` is the identifier of the map item child datatype

returns the datatype identifier

### **`struct`**

*Create a struct datatype, specified in terms of the field identifiers of its children*

```q
.arrowkdb.struct[field_ids]
```

Where `field_ids` is the list of field identifiers of the struct's children

returns the datatype identifier

### **`sparse_union`**

*Create a sparse union datatype, specified in terms of the field identifiers of its children*

```q
.arrowkdb.sparse_union[field_ids]
```

Where `field_ids` is the list of field identifiers of the union's children

returns the datatype identifier

An arrow union array is similar to a struct array except that it has an additional type_id array which identifies the live field in each union value set.
### **`dense_union`**

*Create a dense union datatype, specified in terms of the field identifiers of its children*

```q
.arrowkdb.dense_union[field_ids]
```

Where `field_ids` is the list of field identifiers of the union's children

returns the datatype identifier

An arrow union array is similar to a struct array except that it has an additional type_id array which identifies the live field in each union value set.

### Datatype Inspection



## Status

The arrowkdb interface is provided here under an Apache 2.0 license.

If you find issues with the interface or have feature requests, please consider raising an issue [here](https://github.com/KxSystems/arrowkdb/issues).

If you wish to contribute to this project, please follow the contributing guide [here](https://github.com/KxSystems/arrowkdb/blob/main/CONTRIBUTING.md).