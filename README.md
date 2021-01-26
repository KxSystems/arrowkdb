# arrowkdb



### Warning: This interface is currently a pre-release alpha and subject to change without notice



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

- Currently Arrow supports over 35 datatypes including concrete, parameterized and nested datatypes
- A field describes a column in the table and is composed of a datatype and a string field name
- A schema is built up from a list of fields
- The array data for each column in the table is then populated using a builder object specific to that field's datatype
- Similarly datatype specific reader objects are used to interpret and inspect the array data for each column in the table



## Arrow Datatypes and kdb+ mappings

Currently Arrow supports over 35 datatypes including concrete, parameterized and nested datatypes.

Similar to pyarrow, `arrowkdb` exposes the Arrow datatype constructors to q.  When one of these constructors is called it will return an integer datatype identifier which can then be passed to other functions, e.g. when creating a field.

### Concrete Datatypes

These cover the datatypes where there is a single fixed representation of that datatype.

| **Arrow Datatype** | **Description**                                         | **Kdb+ representation of Arrow array**               |
| ------------------ | ------------------------------------------------------- | ---------------------------------------------------- |
| na                 | NULL type having no physical storage                    | Mixed list of empty lists                            |
| boolean            | Boolean as 1 bit, LSB bit-packed ordering               | 1h                                                   |
| uint8              | Unsigned 8-bit little-endian integer                    | 4h                                                   |
| int8               | Signed 8-bit little-endian integer                      | 4h                                                   |
| uint16             | Unsigned 16-bit little-endian integer                   | 5h                                                   |
| int16              | Signed 16-bit little-endian integer                     | 5h                                                   |
| uint32             | Unsigned 32-bit little-endian integer                   | 6h                                                   |
| int32              | Signed 32-bit little-endian integer                     | 6h                                                   |
| uint64             | Unsigned 64-bit little-endian integer                   | 7h                                                   |
| int64              | Signed 64-bit little-endian integer                     | 7h                                                   |
| float16            | 2-byte floating point value (populated from uint16_t)   | 5h                                                   |
| float32            | 4-byte floating point value                             | 8h                                                   |
| float64            | 8-byte floating point value                             | 9h                                                   |
| utf8               | UTF8 variable-length string                             | Mixed list of 10h                                    |
| large_utf8         | Large UTF8 variable-length string                       | Mixed list of 10h                                    |
| binary             | Variable-length bytes (no guarantee of UTF8-ness)       | Mixed list of 4h                                     |
| large_binary       | Large variable-length bytes (no guarantee of UTF8-ness) | Mixed list of 4h                                     |
| date32             | int32_t days since the UNIX epoch                       | 14h (with automatic epoch offsetting)                |
| date64             | int64_t milliseconds since the UNIX epoch               | 12h (with automatic epoch offsetting and ms scaling) |
| month_interval     | Interval described as a number of months                | 13h                                                  |
| day_time_interval  | Interval described as number of days and milliseconds   | 16h (with automatic ns scaling)                      |



### Parameterized Datatypes

These represent multiple logical interpretations of the underlying physical data, where each parameterized interpretation is a distinct datatype in its own right.

| **Arrow Datatype**            | **Description**                                              | **Kdb+ representation of Arrow array**                     |
| ----------------------------- | ------------------------------------------------------------ | ---------------------------------------------------------- |
| fixed_size_binary(byte_width) | Fixed-size binary. Each value occupies the same number of bytes. | Mixed list of 4h                                           |
| timestamp(time_unit)          | Exact timestamp encoded with int64_t (as number of seconds, milliseconds, microseconds or nanoseconds since UNIX epoch) | 12h (with automatic epoch offsetting and TimeUnit scaling) |
| time32(time_unit)             | Time as signed 32-bit integer, representing either seconds or milliseconds since midnight | 19h (with automatic TimeUnit scaling)                      |
| time64(time_unit)             | Time as signed 64-bit integer, representing either microseconds or nanoseconds since midnight | 16h (with automatic TimeUnit scaling)                      |
| duration(time_unit)           | Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds | 16h (with automatic TimeUnit scaling)                      |
| decimal128(precision, scale)  | Precision- and scale-based signed 128-bit integer in two's complement | Mixed list of 4h (each of length 16)                       |



### Nested Datatypes

These are used to define higher level groupings of either the child datatypes or its constituent fields (a field specifies its datatype and the field's name).

| **Arrow Datatype**                               | **Description**                                              | **Kdb+ representation of Arrow array**                       |
| ------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| list(datatype_id)                                | List datatype specified in terms of its child datatype       | Mixed list for the parent list array containing a set of sub-lists (of type determined by the child datatype), one for each of the list value sets |
| large_list(datatype_id)                          | Large list datatype specified in terms of its child datatype | Mixed list for the parent list array containing a set of sub-lists (of type determined by the child datatype), one for each of the list value sets |
| fixed_size_list(datatype_id, list_size)          | Fixed size list datatype specified in terms of its child datatype and the fixed size of each of the child lists | Same as variable length lists, except each of the sub-lists must be of length equal to the list_size |
| map(key_datatype_id, item_datatype_id)           | Map datatype specified in terms of its key and item child datatypes | Mixed list for the parent map array, with a dictionary for each map value set |
| struct(field_ids)                                | Struct datatype specified in terms of a list of its constituent child field identifiers | Mixed list for the parent struct array, containing child lists for each field in the struct |
| dictionary(value_datatype_id, index_datatype_id) | A dictionary datatype specified in terms of its value and index datatypes, similar to pandas categorical | Two item item mixed list, first item contains the values list and the second item contains the indices list. |
| sparse_union(field_ids)                          | Union datatype specified in terms of a list of its constituent child field identifiers | Similar to a struct array except the mixed list has an additional type_id array (5h) at the start which identifies the live field in each union value set |
| dense_union(field_ids)                           | Union datatype specified in terms of a list of its constituent child field identifiers | Similar to a struct array except the mixed list has an additional type_id array (5h) at the start which identifies the live field in each union value set |



### Inferred Datatypes

It is also possible to have `arrowkbd` infer a suitable Arrow datatype from the type of a kdb+ list.  Similarly Arrow schemas can be inferred from a kdb+ table.  This approach is easier to use but only supports a subset of the Arrow datatypes and is considerably less flexible.  Inferring Arrow datatypes is suggested if you are less familiar with Arrow or do not wish to use the more complex or nested Arrow datatypes.

| Kdb+ list type    | Inferred Arrow Datatype         |
| ----------------- | ------------------------------- |
| 1h                | boolean                         |
| 2h                | fixed_size_binary(16)           |
| 4h                | int8                            |
| 5h                | int16                           |
| 6h                | int32                           |
| 7h                | int64                           |
| 8h                | float32                         |
| 9h                | float64                         |
| 10h               | uint8                           |
| 11h               | utf8                            |
| 12h               | timestamp(nano)                 |
| 13h               | month_interval                  |
| 14h               | date32                          |
| 15h               | NA - cast in q with `timestamp$ |
| 16h               | time64(nano)                    |
| 17h               | NA - cast in q with `time$      |
| 18h               | NA - cast in q with `time$      |
| 19h               | time32(milli)                   |
| Mixed list of 4h  | binary                          |
| Mixed list of 10h | utf8                            |

??? warning "The inference only works for a trivial kdb lists containing simple datatypes.  Only mixed lists of char arrays or byte arrays are supported, mapped to Arrow utf8 and binary datatypes respectively.  Other mixed list structures (e.g. those used by the nested arrow datatypes) cannot be interpreted - if required these should be created manually using the datatype constructors"



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

#### **`dt.na`**

*Create a NULL datatype*

```q
.arrowkdb.dt.na[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.na[]]
null
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.na[];(();();())]
3 nulls
```

#### **`dt.boolean`**

*Create a boolean datatype*

```q
.arrowkdb.dt.boolean[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.boolean[]]
bool
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.boolean[];(010b)]
[
  false,
  true,
  false
]
```

#### **`dt.uint8`**

*Create an uint8 datatype*

```q
.arrowkdb.dt.uint8[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint8[]]
uint8
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint8[];(0x102030)]
[
  16,
  32,
  48
]
```

#### **`dt.int8`**

*Create an int8 datatype*

```q
.arrowkdb.dt.int8[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int8[]]
int8
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int8[];(0x102030)]
[
  16,
  32,
  48
]
```

#### **`uint16`**

*Create an uint16 datatype*

```q
.arrowkdb.dt.uint16[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint16[]]
uint16
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint16[];(11 22 33h)]
[
  11,
  22,
  33
]
```

#### **`dt.int16`**

*Create an int16 datatype*

```q
.arrowkdb.dt.int16[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int16[]]
int16
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int16[];(11 22 33h)]
[
  11,
  22,
  33
]
```

#### **`dt.uint32`**

*Create an uint32 datatype*

```q
.arrowkdb.dt.uint32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint32[]]
uint32
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint32[];(11 22 33i)]
[
  11,
  22,
  33
]
```

#### **`dt.int32`**

*Create an int32 datatype*

```q
.arrowkdb.dt.int32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int32[]]
int32
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int32[];(11 22 33i)]
[
  11,
  22,
  33
]
```

#### **`dt.uint64`**

*Create an uint64 datatype*

```q
.arrowkdb.dt.uint64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint64[]]
uint64
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint64[];(11 22 33j)]
[
  11,
  22,
  33
]
```

#### **`dt.int64`**

*Create an int64 datatype*

```q
.arrowkdb.dt.int64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int64[]]
int64
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int64[];(11 22 33j)]
[
  11,
  22,
  33
]
```

#### **`dt.float16`**

*Create a float16 (represented as uint16_t) datatype*

```q
.arrowkdb.dt.float16[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.float16[]]
halffloat
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.float16[];(11 22 33h)]
[
  11,
  22,
  33
]
```

#### **`dt.float32`**

*Create a float32 datatype*

```q
.arrowkdb.dt.float32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.float32[]]
float
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.float32[];(1.1 2.2 3.3e)]
[
  1.1,
  2.2,
  3.3
]
```

#### **`dt.float64`**

*Create a float64 datatype*

```q
.arrowkdb.dt.float64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.float64[]]
double
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.float64[];(1.1 2.2 3.3f)]
[
  1.1,
  2.2,
  3.3
]
```

#### **`dt.utf8`**

*Create a UTF8 variable length string datatype*

```q
.arrowkdb.dt.utf8[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.utf8[]]
string
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.utf8[];(enlist "a";"bb";"ccc")]
[
  "a",
  "bb",
  "ccc"
]
```

#### **`dt.large_utf8`**

*Create a large (64 bit offsets) UTF8 variable length string datatype*

```q
.arrowkdb.dt.large_utf8[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.large_utf8[]]
large_string
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.large_utf8[];(enlist "a";"bb";"ccc")]
[
  "a",
  "bb",
  "ccc"
]
```

#### **`dt.binary`**

*Create a variable length bytes datatype*

```q
.arrowkdb.dt.binary[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.binary[]]
binary
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.binary[];(enlist 0x11;0x2222;0x333333)]
[
  11,
  2222,
  333333
]
```

#### **`dt.large_binary`**

*Create a large (64 bit offsets) variable length bytes datatype*

```q
.arrowkdb.dt.large_binary[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.large_binary[]]
large_binary
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.large_binary[];(enlist 0x11;0x2222;0x333333)]
[
  11,
  2222,
  333333
]
```

#### **`dt.fixed_size_binary`**

*Create a fixed width bytes datatype*

```q
.arrowkdb.dt.fixed_size_binary[byte_width]
```

Where `byte_width` is the int32 fixed size byte width (each value in the array occupies the same number of bytes).

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.fixed_size_binary[2i]]
fixed_size_binary[2]
q).arrowkdb.dt.getByteWidth[.arrowkdb.dt.fixed_size_binary[2i]]
2i
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.fixed_size_binary[2i];(0x1111;0x2222;0x3333)]
[
  1111,
  2222,
  3333
]
```

#### **`dt.date32`**

*Create a 32 bit date (days since UNIX epoch) datatype*

```q
.arrowkdb.dt.date32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.date32[]]
date32[day]
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.date32[];(2001.01.01 2002.02.02 2003.03.03)]
[
  2001-01-01,
  2002-02-02,
  2003-03-03
]
```

#### **`dt.date64`**

*Create a 64 bit date (milliseconds since UNIX epoch) datatype*

```q
.arrowkdb.dt.date64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.date64[]]
date64[ms]
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.date64[];(2001.01.01D00:00:00.000000000 2002.02.02D00:00:00.000000000 2003.03.03D00:00:00.000000000)]
[
  2001-01-01,
  2002-02-02,
  2003-03-03
]
```

#### **`dt.timestamp`**

*Create a 64 bit timestamp (units since UNIX epoch with specified granularity) datatype*

```q
.arrowkdb.dt.timestamp[time_unit]
```

Where `time_unit` is the time unit string: SECOND, MILLI, MICRO or NANO

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.timestamp[`NANO]]
timestamp[ns]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.timestamp[`NANO]]
`NANO
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.timestamp[`NANO];(2001.01.01D00:00:00.100000001 2002.02.02D00:00:00.200000002 2003.03.03D00:00:00.300000003)]
[
  2001-01-01 00:00:00.100000001,
  2002-02-02 00:00:00.200000002,
  2003-03-03 00:00:00.300000003
]
```

#### **`dt.time32`**

*Create a 32 bit time (units since midnight with specified granularity) datatype*

```q
.arrowkdb.dt.time32[time_unit]
```

Where `time_unit` is the time unit string: SECOND or MILLI

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.time32[`MILLI]]
time32[ms]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.time32[`MILLI]]
`MILLI
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.time32[`MILLI];(01:00:00.100 02:00:00.200 03:00:00.300)]
[
  01:00:00.100,
  02:00:00.200,
  03:00:00.300
]
```

#### **`dt.time64`**

*Create a 64 bit time (units since midnight with specified granularity) datatype*

```q
.arrowkdb.dt.time64[time_unit]
```

Where `time_unit` is the time unit string: MICRO or NANO

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.time64[`NANO]]
time64[ns]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.time64[`NANO]]
`NANO
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.time64[`NANO];(0D01:00:00.100000001 0D02:00:00.200000002 0D03:00:00.300000003)]
[
  01:00:00.100000001,
  02:00:00.200000002,
  03:00:00.300000003
]
```

#### **`dt.month_interval`**

*Create a 32 bit interval (described as a number of months, similar to YEAR_MONTH in SQL) datatype*

```q
.arrowkdb.dt.month_interval[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.month_interval[]]
month_interval
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.month_interval[];(2001.01m,2002.02m,2003.03m)]
[
  12,
  25,
  38
]
```

#### **`dt.day_time_interval`**

*Create a 64 bit interval (described as a number of days and milliseconds, similar to DAY_TIME in SQL) datatype*

```q
.arrowkdb.dt.day_time_interval[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.day_time_interval[]]
day_time_interval
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.day_time_interval[];(0D01:00:00.100000000 0D02:00:00.200000000 0D03:00:00.300000000)]
[
  0d3600100ms,
  0d7200200ms,
  0d10800300ms
]
```

#### **`dt.duration`**

*Create a 64 bit duration (measured in units of specified granularity) datatype*

```q
.arrowkdb.dt.duration[time_unit]
```

Where `time_unit` is the time unit string: SECOND, MILLI, MICRO or NANO

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.duration[`NANO]]
duration[ns]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.duration[`NANO]]
`NANO
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.duration[`NANO];(0D01:00:00.100000000 0D02:00:00.200000000 0D03:00:00.300000000)]
[
  3600100000000,
  7200200000000,
  10800300000000
]
```

#### **`dt.decimal128`**

*Create a 128 bit integer (with precision and scale in two's complement) datatype*

```q
.arrowkdb.dt.decimal128[precision;scale]
```

Where:

- `precision` is the int32 precision width
- `scale` is the int32 scaling factor

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.decimal128[38i;2i]]
decimal(38, 2)
q).arrowkdb.dt.getPrecisionScale[.arrowkdb.dt.decimal128[38i;2i]]
38
2
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.decimal128[38i;2i];(0x00000000000000000000000000000000; 0x01000000000000000000000000000000; 0x00000000000000000000000000000080)]
[
  0.00,
  0.01,
  -1701411834604692317316873037158841057.28
]
q) // With little endian two's complement the decimal128 values are 0, minimum positive, maximum negative
```

#### **`dt.list`**

*Create a list datatype, specified in terms of its child datatype*

```q
.arrowkdb.dt.list[child_datatype_id]
```

Where `child_datatype_id` is the identifier of the list's child datatype

returns the datatype identifier

```q
q)list_datatype:.arrowkdb.dt.list[.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[list_datatype]
list<item: int64>
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
q).arrowkdb.ar.prettyPrintArray[list_datatype;((enlist 1);(2 2);(3 3 3))]
[
  [
    1
  ],
  [
    2,
    2
  ],
  [
    3,
    3,
    3
  ]
]
```

#### **`dt.large_list`**

*Create a large (64 bit offsets) list datatype, specified in terms of its child datatype*

```q
.arrowkdb.dt.large_list[child_datatype_id]
```

Where `child_datatype_id` is the identifier of the list's child datatype

returns the datatype identifier

```q
q)list_datatype:.arrowkdb.dt.large_list[.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[list_datatype]
large_list<item: int64>
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
q).arrowkdb.ar.prettyPrintArray[list_datatype;((enlist 1);(2 2);(3 3 3))]
[
  [
    1
  ],
  [
    2,
    2
  ],
  [
    3,
    3,
    3
  ]
]
```

#### **`dt.fixed_size_list`**

*Create a fixed size list datatype, specified in terms of its child datatype*

```q
.arrowkdb.dt.fixed_size_list[child_datatype_id;list_size]
```

Where:

- `child_datatype_id` is the identifier of the list's child datatype
- `list_size`  is the int32 fixed size of each of the child lists

returns the datatype identifier

```q
q)list_datatype:.arrowkdb.dt.fixed_size_list[.arrowkdb.dt.int64[];2i]
q).arrowkdb.dt.printDatatype[list_datatype]
fixed_size_list<item: int64>[2]
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
q).arrowkdb.dt.getListSize[list_datatype]
2i
q).arrowkdb.ar.prettyPrintArray[list_datatype;((1 1);(2 2);(3 3))]
[
  [
    1,
    1
  ],
  [
    2,
    2
  ],
  [
    3,
    3
  ]
]
```

#### **`dt.map`**

*Create a map datatype, specified in terms of its key and item child datatypes*

```q
.arrowkdb.dt.map[key_datatype_id;item_datatype_id]
```

Where:

- `key_datatype_id` is the identifier of the map key child datatype
- `item_datatype_id` is the identifier of the map item child datatype

returns the datatype identifier

```q
q)map_datatype:.arrowkdb.dt.map[.arrowkdb.dt.int64[];.arrowkdb.dt.float64[]]
q).arrowkdb.dt.printDatatype[map_datatype]
map<int64, double>
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getMapDatatypes[map_datatype]
int64
double
::
::
q).arrowkdb.ar.prettyPrintArray[map_datatype;((enlist 1)!(enlist 1f);(2 2)!(2 2f);(3 3 3)!(3 3 3f))]
[
  keys:
  [
    1
  ]
  values:
  [
    1
  ],
  keys:
  [
    2,
    2
  ]
  values:
  [
    2,
    2
  ],
  keys:
  [
    3,
    3,
    3
  ]
  values:
  [
    3,
    3,
    3
  ]
]
```

#### `dt.dictionary`

*A dictionary datatype specified in terms of its value and index datatypes, similar to pandas categorical*

```q
.arrowkdb.dt.dictionary[value_datatype_id;index_datatype_id]
```

Where:

- `value_datatype_id` is the identifier of the dictionary value datatype, must be a scalar type
- `index_datatype_id` is the identifier of the dictionary index datatype, must be a signed int type

returns the datatype identifier

```q
q)dict_datatype:.arrowkdb.dt.dictionary[.arrowkdb.dt.utf8[];.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[dict_datatype]
dictionary<values=string, indices=int64, ordered=0>
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getDictionaryDatatypes[dict_datatype]
string
int64
::
::
q).arrowkdb.ar.prettyPrintArray[dict_datatype;(("aa";"bb";"cc");(2 0 1 0 0))]

-- dictionary:
  [
    "aa",
    "bb",
    "cc"
  ]
-- indices:
  [
    2,
    0,
    1,
    0,
    0
  ]
q) // The categorical interpretation of the dictionary (looking up the values set at each index) would be: "cc", "aa", "bb", "aa", "aa"
```

#### **`dt.struct`**

*Create a struct datatype, specified in terms of the field identifiers of its children*

```q
.arrowkdb.dt.struct[field_ids]
```

Where `field_ids` is the list of field identifiers of the struct's children

returns the datatype identifier

```q
q)field_one:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)field_two:.arrowkdb.fd.field[`utf8_field;.arrowkdb.dt.utf8[]]
q)struct_datatype:.arrowkdb.dt.struct[field_one,field_two]
q).arrowkdb.dt.printDatatype[struct_datatype]
struct<int_field: int64 not null, utf8_field: string not null>
q).arrowkdb.fd.fieldName each .arrowkdb.dt.getChildFields[struct_datatype]
`int_field`utf8_field
q).arrowkdb.dt.printDatatype each .arrowkdb.fd.fieldDatatype each .arrowkdb.dt.getChildFields[struct_datatype]
int64
string
::
::
q).arrowkdb.ar.prettyPrintArray[struct_datatype;((1 2 3);("aa";"bb";"cc"))]
-- is_valid: all not null
-- child 0 type: int64
  [
    1,
    2,
    3
  ]
-- child 1 type: string
  [
    "aa",
    "bb",
    "cc"
  ]
q) // By slicing across the lists the logical struct values are: (1,"aa"); (2,"bb"); (3,"cc")
```

#### **`dt.sparse_union`**

*Create a sparse union datatype, specified in terms of the field identifiers of its children*

```q
.arrowkdb.dt.sparse_union[field_ids]
```

Where `field_ids` is the list of field identifiers of the union's children

returns the datatype identifier

An arrow union array is similar to a struct array except that it has an additional type_id array which identifies the live field in each union value set.

```q
q)field_one:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)field_two:.arrowkdb.fd.field[`utf8_field;.arrowkdb.dt.utf8[]]
q)union_datatype:.arrowkdb.dt.sparse_union[field_one,field_two]
q).arrowkdb.dt.printDatatype[union_datatype]
sparse_union<int_field: int64 not null=0, utf8_field: string not null=1>
q).arrowkdb.fd.fieldName each .arrowkdb.dt.getChildFields[union_datatype]
`int_field`utf8_field
q).arrowkdb.dt.printDatatype each .arrowkdb.fd.fieldDatatype each .arrowkdb.dt.getChildFields[union_datatype]
int64
string
::
::
q).arrowkdb.ar.prettyPrintArray[union_datatype;((1 0 1h);(1 2 3);("aa";"bb";"cc"))]
-- is_valid: all not null
-- type_ids:   [
    1,
    0,
    1
  ]
-- child 0 type: int64
  [
    1,
    2,
    3
  ]
-- child 1 type: string
  [
    "aa",
    "bb",
    "cc"
  ]
q) // Looking up the type_id array the logical union values are: "aa", 2, "cc"
```

#### **`dt.dense_union`**

*Create a dense union datatype, specified in terms of the field identifiers of its children*

```q
.arrowkdb.dt.dense_union[field_ids]
```

Where `field_ids` is the list of field identifiers of the union's children

returns the datatype identifier

An arrow union array is similar to a struct array except that it has an additional type_id array which identifies the live field in each union value set.

```q
q)field_one:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)field_two:.arrowkdb.fd.field[`utf8_field;.arrowkdb.dt.utf8[]]
q)union_datatype:.arrowkdb.dt.dense_union[field_one,field_two]
q).arrowkdb.dt.printDatatype[union_datatype]
dense_union<int_field: int64 not null=0, utf8_field: string not null=1>
q).arrowkdb.fd.fieldName each .arrowkdb.dt.getChildFields[union_datatype]
`int_field`utf8_field
q).arrowkdb.dt.printDatatype each .arrowkdb.fd.fieldDatatype each .arrowkdb.dt.getChildFields[union_datatype]
int64
string
::
::
q).arrowkdb.ar.prettyPrintArray[union_datatype;((1 0 1h);(1 2 3);("aa";"bb";"cc"))]
-- is_valid: all not null
-- type_ids:   [
    1,
    0,
    1
  ]
-- value_offsets:   [
    0,
    0,
    0
  ]
-- child 0 type: int64
  [
    1,
    2,
    3
  ]
-- child 1 type: string
  [
    "aa",
    "bb",
    "cc"
  ]
q) // Looking up the type_id array the logical union values are: "aa", 2, "cc"
```

#### `dt.inferDatatype`

* Infer an Arrow datatype from a kdb+ list*

```q
.arrowkdb.dt.inferDatatype[list]
```

Where `list` is a kdb+ list

returns the datatype identifier

The kdb+ list type is mapped to an Arrow datatype as described [here](#inferreddatatypes).

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.inferDatatype[(1 2 3j)]]
int64
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.inferDatatype[("aa";"bb";"cc")]]
string
```

### Datatype Inspection

#### `dt.datatypeName`

*Return the base name of a datatype, ignoring any parameters or child datatypes/fields*

```q
.arrowkdb.dt.datatypeName[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns a symbol containing the base name of the datatype

```q
q).arrowkdb.dt.datatypeName[.arrowkdb.dt.int64[]]
`int64
q).arrowkdb.dt.datatypeName[.arrowkdb.dt.fixed_size_binary[4i]]
`fixed_size_binary
```

#### `dt.getTimeUnit`

*Return the TimeUnit of a time32/time64/timestamp/duration datatype*

```q
.arrowkdb.dt.getTimeUnit[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns a symbol containing the time unit string: SECOND/MILLI/MICRO/NANO

```q
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.timestamp[`NANO]]
`NANO
```

#### `dt.getByteWidth`

*Return the byte_width of a fixed_size_binary datatype*

```q
.arrowkdb.dt.getByteWidth[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the int32 byte width

```q
q).arrowkdb.dt.getByteWidth[.arrowkdb.dt.fixed_size_binary[4i]]
4i
```

#### `dt.getListSize`

*Returns the list_size of a fixed_size_list datatype*

```q
.arrowkdb.dt.getListSize[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the int32 list size

```q
q).arrowkdb.dt.getListSize[.arrowkdb.dt.fixed_size_list[.arrowkdb.dt.int64[];4i]]
4i
```

#### `dt.getPrecisionScale`

*Return the precision and scale of a decimal128 datatype*

```q
.arrowkdb.dt.getPrecisionScale[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the int32 precision and scale

```q
q).arrowkdb.dt.getPrecisionScale[.arrowkdb.dt.decimal128[38i;2i]]
38
2
```

#### `dt.getListDatatype`

*Return the child datatype identifier of a parent list/large_list/fixed_size_list datatype*

```q
.arrowkdb.dt.getListDatatype[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the list's child datatype identifier

```q
q)list_datatype:.arrowkdb.dt.list[.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
```

#### `dt.getMapDatatypes`

*Return the key and item child datatype identifiers of a parent map datatype*

```q
.arrowkdb.dt.getMapDatatypes[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the map's key and item child datatype identifiers

```q
q)map_datatype:.arrowkdb.dt.map[.arrowkdb.dt.int64[];.arrowkdb.dt.float64[]]
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getMapDatatypes[map_datatype]
int64
double
::
::
```

#### `dt.getDictionaryDatatypes`

*Return the value and index child datatype identifiers of a parent dictionary datatype*

```
.arrowkdb.dt.getDictionaryDatatypes[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the dictionary's value and index child datatype identifiers

```q
q)dict_datatype:.arrowkdb.dt.dictionary[.arrowkdb.dt.utf8[];.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getDictionaryDatatypes[dict_datatype]
string
int64
::
::
```

#### `dt.getChildFields`

*Return the list of child field identifiers of a struct/spare_union/dense_union parent datatype*

```q
.arrowkdb.dt.getChildFields[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the list of child field identifiers

```q
q)field_one:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)field_two:.arrowkdb.fd.field[`utf8_field;.arrowkdb.dt.utf8[]]
q)struct_datatype:.arrowkdb.dt.struct[field_one,field_two]
q).arrowkdb.fd.printField each .arrowkdb.dt.getChildFields[struct_datatype]
int_field: int64 not null
utf8_field: string not null
::
::
```

### Datatype Management

#### `dt.listDatatypes`

*Return the list of identifiers for all Arrow datatypes currently held in the DatatypeStore*

```q
.arrowkdb.dt.listDatatypes[]
```

Returns list of datatype identifiers

```q
q).arrowkdb.dt.int64[]
1i
q).arrowkdb.dt.float64[]
2i
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.listDatatypes[]
int64
double
::
::
```

#### `dt.printDatatype`

*Display user readable information on the specified datatype identifier, including parameters and nested child datatypes*

```q
.arrowkdb.dt.printDatatype[datatype_id]
```

Where `datatype_id` is the identifier of the datatype, 

1.  prints datatype information to stdout 
1.  returns generic null

??? warning "For debugging use only"

    The information is generated by the `arrow::DataType::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.fixed_size_list[.arrowkdb.dt.int64[];4i]]
fixed_size_list<item: int64>[4]
```

#### `dt.removeDatatype`

*Remove an Arrow datatype from the DatatypeStore.  Any memory held by the datatype object will be released.*

```q
.arrowkdb.dt.removeDatatype[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns generic null on success

```q
q).arrowkdb.dt.int64[]
1i
q).arrowkdb.dt.float64[]
2i
q).arrowkdb.dt.listDatatypes[]
1 2i
q).arrowkdb.dt.removeDatatype[1i]
q).arrowkdb.dt.listDatatypes[]
,2i
```

#### `dt.equalDatatypes`

*Check if two Arrow datatypes are logically equal, including parameters and nested child datatypes*

```q
.arrowkdb.dt.equalDatatypes[first_datatype_id;second_datatype_id]
```

Where:

- `first_datatype_id` is the identifier of the first datatype
- `second_datatype_id` is the identifier of the second datatype

returns boolean result

Internally the DatatypeStore uses the `equalDatatypes` functionality to prevent a new datatype identifier being created when an equal datatype is already present in the DatatypeStore, returning the existing datatype identifier instead.

```q
q).arrowkdb.dt.equalDatatypes[.arrowkdb.dt.int64[];.arrowkdb.dt.int64[]]
1b
q).arrowkdb.dt.equalDatatypes[.arrowkdb.dt.int64[];.arrowkdb.dt.float64[]]
0b
q).arrowkdb.dt.equalDatatypes[.arrowkdb.dt.fixed_size_binary[4i];.arrowkdb.dt.fixed_size_binary[4i]]
1b
q).arrowkdb.dt.equalDatatypes[.arrowkdb.dt.fixed_size_binary[2i];.arrowkdb.dt.fixed_size_binary[4i]]
0b
q).arrowkdb.dt.equalDatatypes[.arrowkdb.dt.list[.arrowkdb.dt.int64[]];.arrowkdb.dt.list[.arrowkdb.dt.int64[]]]
1b
q).arrowkdb.dt.equalDatatypes[.arrowkdb.dt.list[.arrowkdb.dt.int64[]];.arrowkdb.dt.list[.arrowkdb.dt.float64[]]]
0b
```

### Field Constructor

#### `fd.field`

*Create a field instance from its name and datatype*

```q
.arrowkdb.fd.field[field_name;datatype_id]
```

Where:

- `field_name` is a symbol containing the field's name
- `datatype_id` is the identifier of the field's datatype

returns the field identifier

```q
q).arrowkdb.fd.printField[.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]]
int_field: int64 not null
```

### Field Inspection

#### `fd.fieldName`

*Return the name of a field*

```
.arrowkdb.fd.fieldName[field_id]
```

Where `field_id` is the field identifier

returns a symbol containing the field's name

```q
q)field:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q).arrowkdb.fd.fieldName[field]
`int_field
```

#### `fd.fieldDatatype`

Return the datatype of a field

```
.arrowkdb.fd.fieldDatatype[field_id]
```

Where `field_id` is the field identifier

returns the datatype identifier

```q
q)field:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[.arrowkdb.fd.fieldDatatype[field]]
int64
```

### Field Management

#### `fd.listFields`

*Return the list of identifiers for all Arrow fields currently held in the FieldStore*

```q
.arrowkdb.fd.listFields[]
```

Returns list of field identifiers

```q
q).arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
1i
q).arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
2i
q).arrowkdb.fd.printField each .arrowkdb.fd.listFields[]
int_field: int64 not null
float_field: double not null
::
::
```

#### `fd.printField`

*Display user readable information on the specified field identifier, including name and datatype*

```q
.arrowkdb.fd.printField[field_id]
```

Where `field_id` is the identifier of the field, 

1.  prints field information to stdout 
1.  returns generic null

??? warning "For debugging use only"

    The information is generated by the `arrow::Field::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.fd.printField[.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]]
int_field: int64 not null
```

#### `fd.removeField`

*Remove an Arrow field from the FieldStore.  Any memory held by the field object will be released.*

```q
.arrowkdb.fd.removeField[field_id]
```

Where `field_id` is the identifier of the field

returns generic null on success

```q
q).arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
1i
q).arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
2i
q).arrowkdb.fd.listFields[]
1 2i
q).arrowkdb.fd.removeField[1i]
q).arrowkdb.fd.listFields[]
,2i
```

#### `fd.equalFields`

*Check if two Arrow fields are logically equal, including names and datatypes*

```q
.arrowkdb.fd.equalDatatypes[first_field_id;second_field_id]
```

Where:

- `first_field_id` is the identifier of the first field
- `second_field_id` is the identifier of the second field

returns boolean result

Internally the FieldStore uses the `equalFields` functionality to prevent a new field identifier being created when an equal field is already present in the FieldStore, returning the existing field identifier instead.

```q
q)int_dt:.arrowkdb.dt.int64[]
q)float_dt:.arrowkdb.dt.float64[]
q).arrowkdb.fd.equalFields[.arrowkdb.fd.field[`f1;int_dt];.arrowkdb.fd.field[`f1;int_dt]]
1b
q).arrowkdb.fd.equalFields[.arrowkdb.fd.field[`f1;int_dt];.arrowkdb.fd.field[`f2;int_dt]]
0b
q).arrowkdb.fd.equalFields[.arrowkdb.fd.field[`f1;int_dt];.arrowkdb.fd.field[`f1;float_dt]]
0b
```

### Schema Constructors

#### `sc.schema`

*Create a schema instance from a list of field identifiers*

```q
.arrowkdb.sc.schema[field_ids]
```

Where `fields_ids` is a list of field identifiers

returns the schema identifier

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q).arrowkdb.sc.printSchema[.arrowkdb.sc.schema[(f1,f2)]]
int_field: int64 not null
float_field: double not null
```

#### `sc.inferSchema`

*Infer and construct an Arrow schema based on a kdb+ table*

```q
.arrowkdb.sc.inferSchema[table]
```

Where `table` is a kdb+ table or dictionary

returns the schema identifier

??? warning "Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors"

    Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column's kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

```q
q)schema_from_table:.arrowkdb.sc.inferSchema[([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))]
q).arrowkdb.sc.printSchema[schema_from_table]
int_field: int64
float_field: double
str_field: string
```

### Schema Inspection

#### `sc.schemaFields`

*Return the list of field identifiers used by the schema instance*

```q
.arrowkdb.sc.schemaFields[schema_id]
```

Where `schema_id` is the schema identifier

returns list of field identifiers used by the schema

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)schema:.arrowkdb.sc.schema[(f1,f2)]
q).arrowkdb.fd.printField each .arrowkdb.sc.schemaFields[schema]
int_field: int64 not null
float_field: double not null
::
::
```

### Schema Management

#### `sc.listSchemas`

*Return the list of identifiers for all Arrow schemas currently held in the SchemaStore*

```q
.arrowkdb.sc.listSchemas[]
```

Returns list of schema identifiers

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q).arrowkdb.sc.schema[(f1,f2)]
1i
q).arrowkdb.sc.schema[(f2,f1)]
2i
q).arrowkdb.sc.listSchemas[]
1 2i
```

#### `sc.printSchema`

*Display user readable information on the specified schema identifier, including its fields and their order*

```q
.arrowkdb.sc.printSchema[schema_id]
```

Where `schema_id` is the identifier of the schema, 

1.  prints schema information to stdout 
1.  returns generic null

??? warning "For debugging use only"

    The information is generated by the `arrow::Schema::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q).arrowkdb.sc.printSchema[schema]
int_field: int64 not null
float_field: double not null
str_field: string not null
```

#### `sc.removeSchema`

*Remove an Arrow schema from the SchemaStore.  Any memory held by the field object will be released.*

```q
.arrowkdb.sc.removeSchema[schema_id]
```

Where `schema_id` is the identifier of the schema

returns generic null on success

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q).arrowkdb.sc.schema[(f1,f2)]
1i
q).arrowkdb.sc.schema[(f2,f1)]
2i
q).arrowkdb.sc.listSchemas[]
1 2i
q).arrowkdb.sc.removeSchema[1i]
q).arrowkdb.sc.listSchemas[]
,2i
```

#### `sc.equalSchemas`

*Check if two Arrow schemas are logically equal, including their fields and the fields' order*

```q
.arrowkdb.sc.equalSchemas[first_schema_id;second_schema_id]
```

Where:

- `first_schema_id` is the identifier of the first schema
- `second_schema_id` is the identifier of the second schema

returns boolean result

Internally the SchemaStore uses the `equalSchemas` functionality to prevent a new schema identifier being created when an equal schema is already present in the SchemaStore, returning the existing schema identifier instead.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q).arrowkdb.sc.schema[(f1,f2)]
1i
q).arrowkdb.sc.schema[(f2,f1)]
2i
q).arrowkdb.sc.equalSchemas[.arrowkdb.sc.schema[(f1,f2)];.arrowkdb.sc.schema[(f1,f2)]]
1b
q).arrowkdb.sc.equalSchemas[.arrowkdb.sc.schema[(f1,f2)];.arrowkdb.sc.schema[(f1,f1)]]
0b
q).arrowkdb.sc.equalSchemas[.arrowkdb.sc.schema[(f1,f2)];.arrowkdb.sc.schema[(f2,f1)]]
0b
```

### Array Data

#### `ar.prettyPrintArray`

*Convert a kdb+ list to an Arrow array and pretty prints the array*

```q
.arrowkdb.ar.prettyPrintArray[datatype_id;list]
```

Where:

- `datatype_id` is the datatype identifier of the array
- `list` is the kdb+ list data to be displayed

the function

1.  prints array contents to stdout 
1.  returns generic null

??? warning "For debugging use only"

    The information is generated by the `arrow::PrettyPrint()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q)int_datatype:.arrowkdb.dt.int64[]
q).arrowkdb.ar.prettyPrintArray[int_datatype;(1 2 3j)]
[
  1,
  2,
  3
]
```

#### `ar.prettyPrintArrayFromList`

*Convert a kdb+ list to an Arrow array and pretty print the array, inferring the Arrow datatype from the kdb+ list type*

```q
.arrowkdb.ar.prettyPrintArrayFromList[list]
```

Where `list` is the kdb+ list data to be displayed

the function

1.  prints array contents to stdout 
1.  returns generic null

The kdb+ list type is mapped to an Arrow datatype as described [here](#inferreddatatypes).

??? warning "For debugging use only"

    The information is generated by the `arrow::PrettyPrint()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.ar.prettyPrintArrayFromList[(1 2 3j)]
[
  1,
  2,
  3
]
```

### Table Data

#### `tb.prettyPrintTable`

*Convert a kdb+ mixed list of Arrow array data to an Arrow table and pretty print the table*

```
.arrowkdb.tb.prettyPrintTable[schema_id;array_data]
```

Where:

- `schema_id` is the schema identifier of the table
- `array_data` is a mixed list of array data

the function

1.  prints table contents to stdout 
1.  returns generic null

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field's datatype.

??? warning "For debugging use only"

    The information is generated by the `arrow::Table::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q).arrowkdb.tb.prettyPrintTable[schema;((1 2 3j);(4 5 6f);("aa";"bb";"cc"))]
int_field: int64 not null
float_field: double not null
str_field: string not null
----
int_field:
  [
    [
      1,
      2,
      3
    ]
  ]
float_field:
  [
    [
      4,
      5,
      6
    ]
  ]
str_field:
  [
    [
      "aa",
      "bb",
      "cc"
    ]
  ]
```

### `tb.prettyPrintTableFromTable`

*Convert a kdb+ table to an Arrow table and pretty print the table, inferring the Arrow schema from the kdb+ table structure*

```q
.arrowkdb.tb.prettyPrintTableFromTable[table]
```

Where `table` is a kdb+ table

the function

1.  prints table contents to stdout 
1.  returns generic null

Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column's kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

??? warning "Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors"

    Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column's kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

??? warning "For debugging use only"

    The information is generated by the `arrow::Table::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.tb.prettyPrintTableFromTable[([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))]
int_field: int64
float_field: double
str_field: string
----
int_field:
  [
    [
      1,
      2,
      3
    ]
  ]
float_field:
  [
    [
      4,
      5,
      6
    ]
  ]
str_field:
  [
    [
      "aa",
      "bb",
      "cc"
    ]
  ]
```

### Parquet Files

#### `pq.writeParquet`

*Convert a kdb+ mixed list of Arrow array data to an Arrow table and write to a Parquet file*

```q
.arrowkdb.pq.writeParquet[parquet_file;schema_id;array_data;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data
- `options` is a dictionary of symbol options to long/symbol values (pass :: to use defaults)

returns generic null on success

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field's datatype.

Supported options:

- `PARQUET_CHUNK_SIZE` - Controls the approximate size of encoded data pages within a column chunk (long, default: 1MB)
- `PARQUET_VERSION` - Select the Parquet format version, either `V1.0` or `V2.0`.  `V2.0` is more fully featured but may be incompatible with older Parquet implementations (symbol, default `V1.0`)

??? warning "The Parquet format is compressed and designed for for maximum space efficiency which causes a performance overhead compared to Arrow.  Parquet v1.0 is also less fully featured than Arrow which can result in schema limitations"

    Parquet format v1.0 only supports a subset of the the Arrow datatypes:
    * The only supported nested datatypes are top level lists, maps and structs (without further nesting).  Other nested datatypes will cause the Parquet/Arrow file writer to return an error.  
    * Some datatypes will be changed on writing, for example timestamp(ns) -> timestamp(us) and uint32 -> int64.  This causes differences in the read schema compared with that used on writing.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquet["file.parquet";schema;array_data;::]
q)read_data:.arrowkdb.pq.readParquetData["file.parquet";::]
q)array_data~read_data
1b
```

#### `pq.writeParquetFromTable`

*Convert a kdb+ table to an Arrow table and write to a Parquet file, inferring the Arrow schema from the kdb+ table structure*

```q
.arrowkdb.pq.writeParquetFromTable[parquet_file;table;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `table` is a kdb+ table
- `options` is a dictionary of symbol options to long/symbol values (pass :: to use defaults)

returns generic null on success

Supported options:

- `PARQUET_CHUNK_SIZE` - Controls the approximate size of encoded data pages within a column chunk (long, default: 1MB)
- `PARQUET_VERSION` - Select the Parquet format version, either `V1.0` or `V2.0`.  `V2.0` is more fully featured but may be incompatible with older Parquet implementations (symbol, default `V1.0`)

??? warning "Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors"

    Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column's kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q)read_table:.arrowkdb.pq.readParquetToTable["file.parquet";::]
q)read_table~table
1b
```

#### `pq.readParquetSchema`

*Read the Arrow schema from a Parquet file*

```q
.arrowkdb.pq.readParquetSchema[parquet_file]
```

Where `parquet_file` is a string containing the Parquet file name

returns the schema identifier

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquet["file.parquet";schema;array_data;::]
q).arrowkdb.sc.equalSchemas[schema;.arrowkdb.pq.readParquetSchema["file.parquet"]]
1b
```

#### `pq.readParquetData`

*Read an Arrow table from a Parquet file and convert to a kdb+ mixed list of Arrow array data*

```q
.arrowkdb.pq.readParquetData[parquet_file;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `options` is a dictionary of symbol options to long values (pass :: to use defaults)

returns the array data

Supported options:

- `PARQUET_MULTITHREADED_READ` - Flag indicating whether the Parquet reader should run in multithreaded mode.   This can improve performance by processing multiple columns in parallel (long, default: 0)

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquet["file.parquet";schema;array_data;::]
q)read_data:.arrowkdb.pq.readParquetData["file.parquet";::]
q)array_data~read_data
1b
```

#### `pq.readParquetToTable`

*Read an Arrow table from a Parquet file and convert to a kdb+ table*

```q
.arrowkdb.pq.readParquetToTable[parquet_file;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `options` is a dictionary of symbol options to long values (pass :: to use defaults)

returns the kdb+ table

Each schema field name is used as the column name and the Arrow array data is used as the column data.

Supported options:

- `PARQUET_MULTITHREADED_READ` - Flag indicating whether the Parquet reader should run in multithreaded mode.   This can improve performance by processing multiple columns in parallel (long, default: 0)

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q)read_table:.arrowkdb.pq.readParquetToTable["file.parquet";::]
q)read_table~table
1b
```

### Arrow IPC Files

#### `ipc.writeArrow`

*Convert a kdb+ mixed list of Arrow array data to an Arrow table and write to an Arrow file*

```q
.arrowkdb.ipc.writeArrow[arrow_file;schema_id;array_data]
```

Where:

- `arrow_file` is a string containing the Arrow file name
- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data

returns generic null on success

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field's datatype.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrow["file.arrow";schema;array_data]
q)read_data:.arrowkdb.ipc.readArrowData["file.arrow"]
q)read_data~array_data
1b
```

#### `ipc.writeArrowFromTable`

*Convert a kdb+ table to an Arrow table and write to an Arrow file, inferring the Arrow schema from the kdb+ table structure*

```q
.arrowkdb.ipc.writeArrowFromTable[arrow_file;table]
```

Where:

- `arrow_file` is a string containing the Arrow file name
- `table` is a kdb+ table

returns generic null on success

??? warning "Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors"

    Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column's kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrowFromTable["file.arrow";table]
q)read_table:.arrowkdb.ipc.readArrowToTable["file.arrow"]
q)read_table~table
1b
```

#### `ipc.readArrowSchema`

*Read the Arrow schema from an Arrow file*

```q
.arrowkdb.ipc.readArrowSchema[arrow_file]
```

Where `arrow_file` is a string containing the Arrow file name

returns the schema identifier

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrow["file.arrow";schema;array_data]
q).arrowkdb.sc.equalSchemas[schema;.arrowkdb.ipc.readArrowSchema["file.arrow"]]
1b
```

#### `ipc.readArrowData`

*Read an Arrow table from an Arrow file and convert to a kdb+ mixed list of Arrow array data*

```q
.arrowkdb.ipc.readArrowData[arrow_file]
```

Where `arrow_file` is a string containing the Arrow file name

returns the array data

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrow["file.arrow";schema;array_data]
q)read_data:.arrowkdb.ipc.readArrowData["file.arrow"]
q)read_data~array_data
1b
```

#### `ipc.readArrowToTable`

*Read an Arrow table from an Arrow file and convert to a kdb+ table*

```q
.arrowkdb.ipc.readArrowToTable[arrow_file]
```

Where `arrow_file` is a string containing the Arrow file name

returns the kdb+ table

Each schema field name is used as the column name and the Arrow array data is used as the column data.

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrowFromTable["file.arrow";table]
q)read_table:.arrowkdb.ipc.readArrowToTable["file.arrow"]
q)read_table~table
1b
```

### Arrow IPC Streams

#### `ipc.serializeArrow`

*Convert a kdb+ mixed list of Arrow array data to an Arrow table and serialize to an Arrow stream*

```q
.arrowkdb.ipc.serializeArrow[schema_id;array_data]
```

Where:

- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data

returns a byte list containing the serialized stream data

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field's datatype.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrow[schema;array_data]
q)read_data:.arrowkdb.ipc.parseArrowData[serialized]
q)read_data~array_data
1b
```

#### `ipc.serializeArrowFromTable`

*Convert a kdb+ table to an Arrow table and serialize to an Arrow stream, inferring the Arrow schema from the kdb+ table structure*

```q
.arrowkdb.ipc.serializeArrowFromTable[table]
```

Where `table` is a kdb+ table

returns a byte list containing the serialized stream data

??? warning "Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors"

    Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column's kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrowFromTable[table]
q)new_table:.arrowkdb.ipc.parseArrowToTable[serialized]
q)new_table~table
1b
```

#### `ipc.parseArrowSchema`

*Parse the Arrow schema from an Arrow stream*

```q
.arrowkdb.ipc.parseArrowSchema[serialized]
```

Where `serialized` is a byte list containing the serialized stream data

returns the schema identifier

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrow[schema;array_data]
q).arrowkdb.sc.equalSchemas[schema;.arrowkdb.ipc.parseArrowSchema[serialized]]
1b
```

#### `ipc.parseArrowData`

*Parse an Arrow table from an Arrow stream and convert to a kdb+ mixed list of Arrow array data*

```q
.arrowkdb.ipc.parseArrowData[serialized]
```

Where `serialized` is a byte list containing the serialized stream data

returns the array data

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrow[schema;array_data]
q)read_data:.arrowkdb.ipc.parseArrowData[serialized]
q)read_data~array_data
1b
```

#### `ipc.parseArrowToTable`

*Parse an Arrow table from an Arrow file and convert to a kdb+ table*

```q
.arrowkdb.ipc.parseArrowToTable[serialized]
```

Where `serialized` is a byte list containing the serialized stream data

returns the kdb+ table

Each schema field name is used as the column name and the Arrow array data is used as the column data.

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrowFromTable[table]
q)new_table:.arrowkdb.ipc.parseArrowToTable[serialized]
q)new_table~table
1b
```



## Status

The arrowkdb interface is provided here under an Apache 2.0 license.

If you find issues with the interface or have feature requests, please consider raising an issue [here](https://github.com/KxSystems/arrowkdb/issues).

If you wish to contribute to this project, please follow the contributing guide [here](https://github.com/KxSystems/arrowkdb/blob/main/CONTRIBUTING.md).