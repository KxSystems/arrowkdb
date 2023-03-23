# Function reference

These functions are exposed within the `.arrowkdb` namespace, allowing users to convert data between the Arrow/Parquet and kdb+.


## `.arrowkdb`   Arrow/Parquet interface


object | use
-------|-------
<br>**[Datatype constructors](#datatype-constructors)**
[`dt.na`](#dtna) | Create a NULL datatype
[`dt.boolean`](#dtboolean) | Create a boolean datatype
[`dt.int8`](#dtint8) | Create an int8 datatype
[`dt.int16`](#dtint16) | Create an int16 datatype
[`dt.int32`](#dtint32) | Create an int32 datatype
[`dt.int64`](#dtint64) | Create an int64 datatype
[`dt.uint8`](#dtuint8) | Create an uint8 datatype
[`dt.uint16`](#dtuint16) | Create an uint16 datatype
[`dt.uint32`](#dtuint32) | Create an uint32 datatype
[`dt.uint64`](#dtuint64) | Create an uint64 datatype
[`dt.float16`](#dtfloat16) | Create a float16 (represented as uint16_t) datatype
[`dt.float32`](#dtfloat32) | Create a float32 datatype
[`dt.float64`](#dtfloat64) | Create a float64 datatype
[`dt.time32`](#dttime32) | Create a 32-bit time (units since midnight with specified granularity) datatype
[`dt.time64`](#dttime64) | Create a 64-bit time (units since midnight with specified granularity) datatype
[`dt.timestamp`](#dttimestamp) | Create a 64-bit timestamp (units since UNIX epoch with specified granularity) datatype
[`dt.date32`](#dtdate32) | Create a 32-bit date (days since UNIX epoch) datatype
[`dt.date64`](#dtdate64) | Create a 64-bit date (milliseconds since UNIX epoch) datatype
[`dt.month_interval`](#dtmonth_interval) | Create a 32-bit interval (described as a number of months, similar to YEAR_MONTH in SQL) datatype
[`dt.day_time_interval`](#dtday_time_interval) | Create a 64-bit interval (described as a number of days and milliseconds, similar to DAY_TIME in SQL) datatype
[`dt.duration`](#dtduration) | Create a 64-bit duration (measured in units of specified granularity) datatype
[`dt.binary`](#dtbinary) | Create a variable length bytes datatype
[`dt.utf8`](#dtutf8) | Create a UTF8 variable length string datatype
[`dt.large_binary`](#dtlarge_binary) | Create a large (64-bit offsets) variable length bytes datatype
[`dt.large_utf8`](#dtlarge_utf8) | Create a large (64-bit offsets) UTF8 variable length string datatype
[`dt.fixed_size_binary`](#dtfixed_size_binary) | Create a fixed width bytes datatype
[`dt.decimal128`](#dtdecimal128) | Create a 128-bit integer (with precision and scale in twos complement) datatype
[`dt.list`](#dtlist) | Create a list datatype, specified in terms of its child datatype
[`dt.large_list`](#dtlarge_list) | Create a large (64-bit offsets) list datatype, specified in terms of its child datatype
[`dt.fixed_size_list`](#dt_fixed_size_list) | Create a fixed size list datatype, specified in terms of its child datatype
[`dt.map`](#dtmap) | Create a map datatype, specified in terms of its key and item child datatypes
[`dt.struct`](#dtstruct) | Create a struct datatype, specified in terms of the field identifiers of its children
[`dt.sparse_union`](#dtsparse_union) | Create a sparse union datatype, specified in terms of the field identifiers of its children
[`dt.dense_union`](#dtdense_union) | Create a dense union datatype, specified in terms of the field identifiers of its children
[`dt.dictionary`](#dtdictionary) | Create a dictionary datatype specified in terms of its value and index datatypes, similar to pandas categorical
[`dt.inferDatatype`](#dtinferDatatype) | Infer and construct a datatype from a kdb+ list
<br>**[Datatype inspection](#datatype-inspection)**
[`dt.datatypeName`](#dtdatatypename) | Return the base name of a datatype, ignoring any parameters or child datatypes/fields
[`dt.getTimeUnit`](#dtgettimeunit) | Return the TimeUnit of a time32/time64/timestamp/duration datatype
[`dt.getByteWidth`](#dtgetbytewidth) | Return the byte_width of a fixed_size_binary datatype
[`dt.getListSize`](#dtgetlistsize) | Returns the list_size of a fixed_size_list datatype
[`dt.getPrecisionScale`](#dtgetprecisionscale) | Return the precision and scale of a decimal128 datatype
[`dt.getListDatatype`](#dtgetlistdatatype) | Return the child datatype identifier of a list/large_list/fixed_size_list datatype
[`dt.getMapDatatypes`](#dtgetmapdatatypes) | Return the key and item child datatype identifiers of a map datatype
[`dt.getDictionaryDatatypes`](#dtgetdictionarydatatypes) | Return the value and index child datatype identifiers of a dictionary datatype
[`dt.getChildFields`](#dtgetchildfields) | Return the list of child field identifiers of a struct/spare_union/dense_union datatype
<br>**[Datatype management](#datatype-management)**
[`dt.printDatatype`](#dtprintdatatype) | Display user readable information for a datatype, including parameters and nested child datatypes
[`dt.listDatatypes`](#dtlistdatatypes) | Return the list of identifiers for all datatypes held in the DatatypeStore
[`dt.removeDatatype`](#dtremovedatatype) | Remove a datatype from the DatatypeStore
[`dt.equalDatatypes`](#dtequaldatatypes) | Check if two datatypes are logically equal, including parameters and nested child datatypes
<br>**[Field Constructor](#field-constructor)**
[`fd.field`](#fdfield) | Create a field instance from its name and datatype
<br>**[Field Inspection](#field-inspection)**
[`fd.fieldName`](#fdfieldname) | Return the name of a field
[`fd.fieldDatatype`](#fdfielddatatype) | Return the datatype of a field
<br>**[Field management](#field-management)**
[`fd.printField`](#fdprintfield) | Display user readable information for a field, including name and datatype
[`fd.listFields`](#fdlistfields) | Return the list of identifiers for all fields held in the FieldStore
[`fd.removeField`](#fdremovefield) | Remove a field from the FieldStore
[`fd.equalFields`](#fdequalfields) | Check if two fields are logically equal, including names and datatypes
<br>**[Schema constructors](#schema-constructors)**
[`sc.schema`](#scschema) | Create a schema instance from a list of field identifiers
[`sc.inferSchema`](#scinferschema) | Infer and construct a schema based on a kdb+ table
<br>**[Schema inspection](#schema-inspection)**
[`sc.schemaFields`](#scschemafields) | Return the list of field identifiers used by a schema
<br>**[Schema management](#schema-management)**
[`sc.printSchema`](#scprintschema) | Display user readable information for a schema, including its fields and their order
[`sc.listSchemas`](#sclistschemas) | Return the list of identifiers for all schemas held in the SchemaStore
[`sc.removeSchema`](#scremoveschema) | Remove a schema from the SchemaStore
[`sc.equalSchemas`](#scequalschemas) | Check if two schemas are logically equal, including their fields and the fields' order
<br>**[Array data](#array-data)**
[`ar.prettyPrintArray`](#arprettyprintarray) | Convert a kdb+ list to an Arrow array and pretty print the array
[`ar.prettyPrintArrayFromList`](#arprettyprintarrayfromlist) | Convert a kdb+ list to an Arrow array and pretty-print the array, inferring the datatype from the kdb+ list type
<br>**[Table data](#table-data)**
[`tb.prettyPrintTable`](#tbprettyprinttable) | Convert a kdb+ mixed list of array data to an Arrow table and pretty print the table
[`tb.prettyPrintTableFromTable`](#tbprettyprinttablefromtable) | Convert a kdb+ table to an Arrow table and pretty print the table, inferring the schema from the kdb+ table structure
<br>**[Parquet files](#parquet-files)**
[`pq.writeParquet`](#pqwriteparquet) | Convert a kdb+ mixed list of array data to an Arrow table and write to a Parquet file
[`pq.writeParquetFromTable`](#pqwriteparquetfromtable) | Convert a kdb+ table to an Arrow table and write to a Parquet file, inferring the schema from the kdb+ table structure
[`pq.readParquetSchema`](#pqreadparquetschema) | Read the schema from a Parquet file
[`pq.readParquetData`](#pqreadparquetdata) | Read an Arrow table from a Parquet file and convert to a kdb+ mixed list of array data
[`pq.readParquetColumn`](#pqreadparquetcolumn) | Read a single column from a Parquet file and convert to a kdb+ list
[`pq.readParquetToTable`](#pqreadparquettotable) | Read an Arrow table from a Parquet file and convert to a kdb+ table
[`pq.readParquetNumRowGroups`](#pqreadparquetnumrowgroups) | Read the number of row groups used by a Parquet file 
[`pq.readParquetRowGroups`](#pqreadparquetrowgroups) | Read a set of row groups from a Parquet file into an Arrow table then convert to a kdb+ mixed list of array data
[`pq.readParquetRowGroupsToTable`](#pqreadparquetrowgroupstotable) | Read a set of row groups from a Parquet file into an Arrow table then convert to a kdb+ table
<br>**[Arrow IPC files](#arrow-ipc-files)**
[`ipc.writeArrow`](#ipcwritearrow) | Convert a kdb+ mixed list of array data to an Arrow table and write to an Arrow file
[`ipc.writeArrowFromTable`](#ipcwritearrowfromtable) | Convert a kdb+ table to an Arrow table and write to an Arrow file, inferring the schema from the kdb+ table structure
[`ipc.readArrowSchema`](#ipcreadarrowschema) | Read the schema from an Arrow file
[`ipc.readArrowData`](#ipcreadarrowdata) | Read an Arrow table from an Arrow file and convert to a kdb+ mixed list of array data
[`ipc.readArrowToTable`](#ipcreadarrowtotable) | Read an Arrow table from an Arrow file and convert to a kdb+ table
<br>**[Arrow IPC streams](#arrow-ipc-streams)**
[`ipc.serializeArrow`](#ipcserializearrow) | Convert a kdb+ mixed list of array data to an Arrow table and serialize to an Arrow stream
[`ipc.serializeArrowFromTable`](#ipcserializearrowfromtable) | Convert a kdb+ table to an Arrow table and serialize to an Arrow stream, inferring the schema from the kdb+ table structure
[`ipc.parseArrowSchema`](#ipcparsearrowschema) | Parse the schema from an Arrow stream
[`ipc.parseArrowData`](#ipcparsearrowdata) | Parse an Arrow table from an Arrow stream and convert to a kdb+ mixed list of array data
[`ipc.parseArrowToTable`](#ipcparsearrowtotable) | Parse an Arrow table from an Arrow file and convert to a kdb+ table
<br>**[Utilities](#utilities)**
[`util.buildInfo`](#utilbuildinfo) | Return build information regarding the in use Arrow library



## Datatype constructors

### `dt.na`

*Create a NULL datatype*

```txt
.arrowkdb.dt.na[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.na[]]
null
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.na[];(();();());::]
3 nulls
```

### `dt.boolean`

*Create a boolean datatype*

```txt
.arrowkdb.dt.boolean[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.boolean[]]
bool
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.boolean[];(010b);::]
[
  false,
  true,
  false
]
```

### `dt.int8`

*Create an int8 datatype*

```txt
.arrowkdb.dt.int8[]
```

> **kdb+ type 10h can be written to an `int8` array**
> 
> The is supported on the writing path only.  Reading from an int8 array returns a 4h list

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int8[]]
int8
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int8[];(0x102030);::]
[
  16,
  32,
  48
]
```

### `dt.int16`

*Create an int16 datatype*

```txt
.arrowkdb.dt.int16[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int16[]]
int16
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int16[];(11 22 33h);::]
[
  11,
  22,
  33
]
```

### `dt.int32`

*Create an int32 datatype*

```txt
.arrowkdb.dt.int32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int32[]]
int32
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int32[];(11 22 33i);::]
[
  11,
  22,
  33
]
```

### `dt.int64`

*Create an int64 datatype*

```txt
.arrowkdb.dt.int64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.int64[]]
int64
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.int64[];(11 22 33j);::]
[
  11,
  22,
  33
]
```

### `dt.uint8`

*Create an uint8 datatype*

```txt
.arrowkdb.dt.uint8[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint8[]]
uint8
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint8[];(0x102030);::]
[
  16,
  32,
  48
]
```

### `dt.uint16`

*Create an uint16 datatype*

```txt
.arrowkdb.dt.uint16[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint16[]]
uint16
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint16[];(11 22 33h);::]
[
  11,
  22,
  33
]
```

### `dt.uint32`

*Create an uint32 datatype*

```txt
.arrowkdb.dt.uint32[]
```

Returns the datatype identifier

> :warning: **`uint32` datatype is supported by Parquet v2.0 only, being changed > to `int64` otherwise**
> 
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint32[]]
uint32
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint32[];(11 22 33i);::]
[
  11,
  22,
  33
]
```

### `dt.uint64`

*Create an uint64 datatype*

```txt
.arrowkdb.dt.uint64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.uint64[]]
uint64
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.uint64[];(11 22 33j);::]
[
  11,
  22,
  33
]
```

### `dt.float16`

*Create a float16 (represented as uint16_t) datatype*

```txt
.arrowkdb.dt.float16[]
```

Returns the datatype identifier

> :warning: **`float16` datatype is not supported by Parquet**
> 
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.float16[]]
halffloat
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.float16[];(11 22 33h);::]
[
  11,
  22,
  33
]
```

### `dt.float32`

*Create a float32 datatype*

```txt
.arrowkdb.dt.float32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.float32[]]
float
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.float32[];(1.1 2.2 3.3e);::]
[
  1.1,
  2.2,
  3.3
]
```

### `dt.float64`

*Create a float64 datatype*

```txt
.arrowkdb.dt.float64[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.float64[]]
double
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.float64[];(1.1 2.2 3.3f);::]
[
  1.1,
  2.2,
  3.3
]
```

### `dt.time32`

*Create a 32-bit time (units since midnight with specified granularity) datatype*

```txt
.arrowkdb.dt.time32[time_unit]
```

Where `time_unit` is the time unit string: SECOND or MILLI

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.time32[`MILLI]]
time32[ms]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.time32[`MILLI]]
`MILLI
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.time32[`MILLI];(01:00:00.100 02:00:00.200 03:00:00.300);::]
[
  01:00:00.100,
  02:00:00.200,
  03:00:00.300
]
```

### `dt.time64`

*Create a 64-bit time (units since midnight with specified granularity) datatype*

```txt
.arrowkdb.dt.time64[time_unit]
```

Where `time_unit` is the time unit string: MICRO or NANO

returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.time64[`NANO]]
time64[ns]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.time64[`NANO]]
`NANO
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.time64[`NANO];(0D01:00:00.100000001 0D02:00:00.200000002 0D03:00:00.300000003);::]
[
  01:00:00.100000001,
  02:00:00.200000002,
  03:00:00.300000003
]
```

### `dt.timestamp`

*Create a 64-bit timestamp (units since UNIX epoch with specified granularity) datatype*

```txt
.arrowkdb.dt.timestamp[time_unit]
```

Where `time_unit` is the time unit string: SECOND, MILLI, MICRO or NANO

returns the datatype identifier

> :warning: **`timestamp(nano)` datatype is supported by Parquet v2.0 only, being mapped to `timestamp(milli)` otherwise**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.timestamp[`NANO]]
timestamp[ns]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.timestamp[`NANO]]
`NANO
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.timestamp[`NANO];(2001.01.01D00:00:00.100000001 2002.02.02D00:00:00.200000002 2003.03.03D00:00:00.300000003);::]
[
  2001-01-01 00:00:00.100000001,
  2002-02-02 00:00:00.200000002,
  2003-03-03 00:00:00.300000003
]
```

### `dt.date32`

*Create a 32-bit date (days since UNIX epoch) datatype*

```txt
.arrowkdb.dt.date32[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.date32[]]
date32[day]
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.date32[];(2001.01.01 2002.02.02 2003.03.03);::]
[
  2001-01-01,
  2002-02-02,
  2003-03-03
]
```

### `dt.date64`

*Create a 64-bit date (milliseconds since UNIX epoch) datatype*

```txt
.arrowkdb.dt.date64[]
```

Returns the datatype identifier

> :warning: **`date64` datatype is changed to `date32(days)` by Parquet**
> 
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.date64[]]
date64[ms]
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.date64[];(2001.01.01D00:00:00.000000000 2002.02.02D00:00:00.000000000 2003.03.03D00:00:00.000000000);::]
[
  2001-01-01,
  2002-02-02,
  2003-03-03
]
```

### `dt.month_interval`

*Create a 32-bit interval (described as a number of months, similar to YEAR_MONTH in SQL) datatype*

```txt
.arrowkdb.dt.month_interval[]
```

Returns the datatype identifier

> :warning: **`month_interval` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.month_interval[]]
month_interval
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.month_interval[];(2001.01m,2002.02m,2003.03m);::]
[
  12,
  25,
  38
]
```

### `dt.day_time_interval`

*Create a 64-bit interval (described as a number of days and milliseconds, similar to DAY_TIME in SQL) datatype*

```txt
.arrowkdb.dt.day_time_interval[]
```

Returns the datatype identifier

> :warning: **`day_time_interval` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.day_time_interval[]]
day_time_interval
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.day_time_interval[];(0D01:00:00.100000000 0D02:00:00.200000000 0D03:00:00.300000000);::]
[
  0d3600100ms,
  0d7200200ms,
  0d10800300ms
]
```

### `dt.duration`

*Create a 64-bit duration (measured in units of specified granularity) datatype*

```txt
.arrowkdb.dt.duration[time_unit]
```

Where `time_unit` is the time unit string: SECOND, MILLI, MICRO or NANO

returns the datatype identifier

> :warning: **`duration` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.duration[`NANO]]
duration[ns]
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.duration[`NANO]]
`NANO
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.duration[`NANO];(0D01:00:00.100000000 0D02:00:00.200000000 0D03:00:00.300000000);::]
[
  3600100000000,
  7200200000000,
  10800300000000
]
```

### `dt.binary`

*Create a variable length bytes datatype*

```txt
.arrowkdb.dt.binary[]
```

Returns the datatype identifier

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.binary[]]
binary
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.binary[];(enlist 0x11;0x2222;0x333333);::]
[
  11,
  2222,
  333333
]
```

### `dt.utf8`

*Create a UTF8 variable length string datatype*

```txt
.arrowkdb.dt.utf8[]
```

Returns the datatype identifier

> :warning: **kdb+ type 11h can be written to an `utf8` array**
>
> The is supported on the writing path only.  Reading from an utf8 array returns a mixed list of 10h

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.utf8[]]
string
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.utf8[];(enlist "a";"bb";"ccc");::]
[
  "a",
  "bb",
  "ccc"
]
```

### `dt.large_binary`

*Create a large (64-bit offsets) variable length bytes datatype*

```txt
.arrowkdb.dt.large_binary[]
```

Returns the datatype identifier

> :warning: **`large_binary` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.large_binary[]]
large_binary
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.large_binary[];(enlist 0x11;0x2222;0x333333);::]
[
  11,
  2222,
  333333
]
```

### `dt.large_utf8`

*Create a large (64-bit offsets) UTF8 variable length string datatype*

```txt
.arrowkdb.dt.large_utf8[]
```

Returns the datatype identifier

> :warning: **`large_utf8` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.large_utf8[]]
large_string
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.large_utf8[];(enlist "a";"bb";"ccc");::]
[
  "a",
  "bb",
  "ccc"
]
```

### `dt.fixed_size_binary`

*Create a fixed width bytes datatype*

```txt
.arrowkdb.dt.fixed_size_binary[byte_width]
```

Where `byte_width` is the int32 fixed size byte width (each value in the array occupies the same number of bytes).

returns the datatype identifier

> :warning: **kdb+ type 2h can be written to a `fixed_size_binary(16)` array**
>
> The is supported on the writing path only.  Reading from a fixed_size_binary array returns a mixed list of 4h

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.fixed_size_binary[2i]]
fixed_size_binary[2]
q).arrowkdb.dt.getByteWidth[.arrowkdb.dt.fixed_size_binary[2i]]
2i
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.fixed_size_binary[2i];(0x1111;0x2222;0x3333);::]
[
  1111,
  2222,
  3333
]
```

### `dt.decimal128`

*Create a 128-bit integer (with precision and scale in twos complement) datatype*

```txt
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
q).arrowkdb.ar.prettyPrintArray[.arrowkdb.dt.decimal128[38i;2i];(0x00000000000000000000000000000000; 0x01000000000000000000000000000000; 0x00000000000000000000000000000080);::]
[
  0.00,
  0.01,
  -1701411834604692317316873037158841057.28
]
q) // With little endian twos complement the decimal128 values are 0, minimum positive, maximum negative
```

### `dt.list`

*Create a list datatype, specified in terms of its child datatype*

```txt
.arrowkdb.dt.list[child_datatype_id]
```

Where `child_datatype_id` is the identifier of the list’s child datatype

returns the datatype identifier

```q
q)list_datatype:.arrowkdb.dt.list[.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[list_datatype]
list<item: int64>
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
q).arrowkdb.ar.prettyPrintArray[list_datatype;((enlist 1);(2 2);(3 3 3));::]
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

### `dt.large_list`

*Create a large (64-bit offsets) list datatype, specified in terms of its child datatype*

```txt
.arrowkdb.dt.large_list[child_datatype_id]
```

Where `child_datatype_id` is the identifier of the list’s child datatype

returns the datatype identifier

```q
q)list_datatype:.arrowkdb.dt.large_list[.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[list_datatype]
large_list<item: int64>
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
q).arrowkdb.ar.prettyPrintArray[list_datatype;((enlist 1);(2 2);(3 3 3));::]
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

### `dt.fixed_size_list`

*Create a fixed size list datatype, specified in terms of its child datatype*

```txt
.arrowkdb.dt.fixed_size_list[child_datatype_id;list_size]
```

Where:

- `child_datatype_id` is the identifier of the list’s child datatype
- `list_size`  is the int32 fixed size of each of the child lists

returns the datatype identifier

> :warning: **`fixed_size_list` datatype is changed to `list` by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q)list_datatype:.arrowkdb.dt.fixed_size_list[.arrowkdb.dt.int64[];2i]
q).arrowkdb.dt.printDatatype[list_datatype]
fixed_size_list<item: int64>[2]
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
q).arrowkdb.dt.getListSize[list_datatype]
2i
q).arrowkdb.ar.prettyPrintArray[list_datatype;((1 1);(2 2);(3 3));::]
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

### `dt.map`

*Create a map datatype, specified in terms of its key and item child datatypes*

```txt
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
q).arrowkdb.ar.prettyPrintArray[map_datatype;((enlist 1)!(enlist 1f);(2 2)!(2 2f);(3 3 3)!(3 3 3f));::]
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

### `dt.struct`

*Create a struct datatype, specified in terms of the field identifiers of its children*

```txt
.arrowkdb.dt.struct[field_ids]
```

Where `field_ids` is the list of field identifiers of the struct’s children

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
q).arrowkdb.ar.prettyPrintArray[struct_datatype;((1 2 3);("aa";"bb";"cc"));::]
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

### `dt.sparse_union`

*Create a sparse union datatype, specified in terms of the field identifiers of its children*

```txt
.arrowkdb.dt.sparse_union[field_ids]
```

Where `field_ids` is the list of field identifiers of the union’s children

returns the datatype identifier

An arrow union array is similar to a struct array except that it has an additional type_id array which identifies the live field in each union value set.

> :warning: **`sparse_union` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

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
q).arrowkdb.ar.prettyPrintArray[union_datatype;((1 0 1h);(1 2 3);("aa";"bb";"cc"));::]
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

### `dt.dense_union`

*Create a dense union datatype, specified in terms of the field identifiers of its children*

```txt
.arrowkdb.dt.dense_union[field_ids]
```

Where `field_ids` is the list of field identifiers of the union’s children

returns the datatype identifier

An arrow union array is similar to a struct array except that it has an additional type_id array which identifies the live field in each union value set.

> :warning: **`dense_union` datatype is not supported by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

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
q).arrowkdb.ar.prettyPrintArray[union_datatype;((1 0 1h);(1 2 3);("aa";"bb";"cc"));::]
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

### `dt.dictionary`

*Create a dictionary datatype specified in terms of its value and index datatypes, similar to pandas categorical*

```txt
.arrowkdb.dt.dictionary[value_datatype_id;index_datatype_id]
```

Where:

- `value_datatype_id` is the identifier of the dictionary value datatype, must be a scalar type
- `index_datatype_id` is the identifier of the dictionary index datatype, must be a signed int type

returns the datatype identifier

> :warning: **Only the categorical interpretation of a `dictionary` datatype array is saved by Parquet**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

```q
q)dict_datatype:.arrowkdb.dt.dictionary[.arrowkdb.dt.utf8[];.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[dict_datatype]
dictionary<values=string, indices=int64, ordered=0>
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getDictionaryDatatypes[dict_datatype]
string
int64
::
::
q).arrowkdb.ar.prettyPrintArray[dict_datatype;(("aa";"bb";"cc");(2 0 1 0 0));::]

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

### `dt.inferDatatype`

*Infer and construct a datatype from a kdb+ list*

```txt
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

## Datatype inspection

### `dt.datatypeName`

*Return the base name of a datatype, ignoring any parameters or child datatypes/fields*

```txt
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

### `dt.getTimeUnit`

*Return the TimeUnit of a time32/time64/timestamp/duration datatype*

```txt
.arrowkdb.dt.getTimeUnit[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns a symbol containing the time unit string: SECOND/MILLI/MICRO/NANO

```q
q).arrowkdb.dt.getTimeUnit[.arrowkdb.dt.timestamp[`NANO]]
`NANO
```

### `dt.getByteWidth`

*Return the byte_width of a fixed_size_binary datatype*

```txt
.arrowkdb.dt.getByteWidth[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the int32 byte width

```q
q).arrowkdb.dt.getByteWidth[.arrowkdb.dt.fixed_size_binary[4i]]
4i
```

### `dt.getListSize`

*Returns the list_size of a fixed_size_list datatype*

```txt
.arrowkdb.dt.getListSize[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the int32 list size

```q
q).arrowkdb.dt.getListSize[.arrowkdb.dt.fixed_size_list[.arrowkdb.dt.int64[];4i]]
4i
```

### `dt.getPrecisionScale`

*Return the precision and scale of a decimal128 datatype*

```txt
.arrowkdb.dt.getPrecisionScale[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the int32 precision and scale

```q
q).arrowkdb.dt.getPrecisionScale[.arrowkdb.dt.decimal128[38i;2i]]
38
2
```

### `dt.getListDatatype`

*Return the child datatype identifier of a list/large_list/fixed_size_list datatype*

```txt
.arrowkdb.dt.getListDatatype[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the list’s child datatype identifier

```q
q)list_datatype:.arrowkdb.dt.list[.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.getListDatatype[list_datatype]]
int64
```

### `dt.getMapDatatypes`

*Return the key and item child datatype identifiers of a map datatype*

```txt
.arrowkdb.dt.getMapDatatypes[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the map’s key and item child datatype identifiers

```q
q)map_datatype:.arrowkdb.dt.map[.arrowkdb.dt.int64[];.arrowkdb.dt.float64[]]
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getMapDatatypes[map_datatype]
int64
double
::
::
```

### `dt.getDictionaryDatatypes`

*Return the value and index child datatype identifiers of a dictionary datatype*

```txt
.arrowkdb.dt.getDictionaryDatatypes[datatype_id]
```

Where `datatype_id` is the identifier of the datatype

returns the dictionary’s value and index child datatype identifiers

```q
q)dict_datatype:.arrowkdb.dt.dictionary[.arrowkdb.dt.utf8[];.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype each .arrowkdb.dt.getDictionaryDatatypes[dict_datatype]
string
int64
::
::
```

### `dt.getChildFields`

*Return the list of child field identifiers of a struct/spare_union/dense_union datatype*

```txt
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

## Datatype management

### `dt.printDatatype`

*Display user-readable information for a datatype, including parameters and nested child datatypes*

```txt
.arrowkdb.dt.printDatatype[datatype_id]
```

Where `datatype_id` is the identifier of the datatype, 

1.  prints datatype information to stdout 
1.  returns generic null

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::DataType::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.dt.printDatatype[.arrowkdb.dt.fixed_size_list[.arrowkdb.dt.int64[];4i]]
fixed_size_list<item: int64>[4]
```

### `dt.listDatatypes`

*Return the list of identifiers for all datatypes held in the DatatypeStore*

```txt
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

### `dt.removeDatatype`

*Remove a datatype from the DatatypeStore*

```txt
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

### `dt.equalDatatypes`

*Check if two datatypes are logically equal, including parameters and nested child datatypes*

```txt
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


## Field constructor

### `fd.field`

*Create a field instance from its name and datatype*

```txt
.arrowkdb.fd.field[field_name;datatype_id]
```

Where:

- `field_name` is a symbol containing the field’s name
- `datatype_id` is the identifier of the field’s datatype

returns the field identifier

```q
q).arrowkdb.fd.printField[.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]]
int_field: int64 not null
```

## Field inspection

### `fd.fieldName`

_Name of a field_

```txt
.arrowkdb.fd.fieldName[field_id]
```

Where `field_id` is the field identifier

returns a symbol containing the field’s name

```q
q)field:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q).arrowkdb.fd.fieldName[field]
`int_field
```

### `fd.fieldDatatype`

_Datatype of a field_

```txt
.arrowkdb.fd.fieldDatatype[field_id]
```

Where `field_id` is the field identifier

returns the datatype identifier

```q
q)field:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q).arrowkdb.dt.printDatatype[.arrowkdb.fd.fieldDatatype[field]]
int64
```


## Field management

### `fd.printField`

*Display user readable information for a field, including name and datatype*

```txt
.arrowkdb.fd.printField[field_id]
```

Where `field_id` is the identifier of the field, 

1.  prints field information to stdout 
1.  returns generic null

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::Field::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.fd.printField[.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]]
int_field: int64 not null
```

### `fd.listFields`

_List of identifiers for all fields held in the FieldStore_

```txt
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

### `fd.removeField`

*Remove a field from the FieldStore*

```txt
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

### `fd.equalFields`

*Check if two fields are logically equal, including names and datatypes*

```txt
.arrowkdb.fd.equalDatatypes[first_field_id;second_field_id]
```

Where:

-   `first_field_id` is the identifier of the first field
-   `second_field_id` is the identifier of the second field

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

## Schema constructors

### `sc.schema`

*Create a schema instance from a list of field identifiers*

```txt
.arrowkdb.sc.schema[field_ids]
```

Where `fields_ids` is a list of field identifiers

returns the schema identifier

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q).arrowkdb.sc.printSchema[.arrowkdb.sc.schema[(f1,f2)]]
int_field: int64
float_field: double
```

### `sc.inferSchema`

*Infer and construct a schema based on a kdb+ table*

```txt
.arrowkdb.sc.inferSchema[table]
```

Where `table` is a kdb+ table or dictionary

returns the schema identifier

> :warning: **Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors**
>
> Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferred-datatypes).

```q
q)schema_from_table:.arrowkdb.sc.inferSchema[([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))]
q).arrowkdb.sc.printSchema[schema_from_table]
int_field: int64
float_field: double
str_field: string
```

## Schema inspection

### `sc.schemaFields`

*Return the list of field identifiers used by a schema*

```txt
.arrowkdb.sc.schemaFields[schema_id]
```

Where `schema_id` is the schema identifier

returns list of field identifiers used by the schema

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)schema:.arrowkdb.sc.schema[(f1,f2)]
q).arrowkdb.fd.printField each .arrowkdb.sc.schemaFields[schema];
int_field: int64
float_field: double
```

## Schema management

### `sc.printSchema`

*Display user readable information for a schema, including its fields and their order*

```txt
.arrowkdb.sc.printSchema[schema_id]
```

Where `schema_id` is the identifier of the schema, 

1.  prints schema information to stdout 
1.  returns generic null

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::Schema::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q).arrowkdb.sc.printSchema[schema]
int_field: int64
float_field: double
str_field: string
```

### `sc.listSchemas`

*Return the list of identifiers for all schemas held in the SchemaStore*

```txt
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

### `sc.removeSchema`

*Remove a schema from the SchemaStore*

```txt
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

### `sc.equalSchemas`

*Check if two schemas are logically equal, including their fields and the fields' order*

```txt
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

## Array data

### `ar.prettyPrintArray`

*Convert a kdb+ list to an Arrow array and pretty print the array*

```txt
.arrowkdb.ar.prettyPrintArray[datatype_id;list;options]
```

Where:

- `datatype_id` is the datatype identifier of the array
- `list` is the kdb+ list data to be displayed
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

the function

1.  prints array contents to stdout 
1.  returns generic null

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::PrettyPrint()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q)int_datatype:.arrowkdb.dt.int64[]
q).arrowkdb.ar.prettyPrintArray[int_datatype;(1 2 3j);::]
[
  1,
  2,
  3
]
```

### `ar.prettyPrintArrayFromList`

*Convert a kdb+ list to an Arrow array and pretty print the array, inferring the datatype from the kdb+ list type*

```txt
.arrowkdb.ar.prettyPrintArrayFromList[list;options]
```

Where:

- `list` is the kdb+ list data to be displayed
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

the function

1.  prints array contents to stdout 
1.  returns generic null

Supported options:

- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

The kdb+ list type is mapped to an Arrow datatype as described [here](#inferreddatatypes).

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::PrettyPrint()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.ar.prettyPrintArrayFromList[(1 2 3j);::]
[
  1,
  2,
  3
]
```

## Table data

### `tb.prettyPrintTable`

*Convert a kdb+ mixed list of array data to an Arrow table and pretty print the table*

```
.arrowkdb.tb.prettyPrintTable[schema_id;array_data;options]
```

Where:

- `schema_id` is the schema identifier of the table
- `array_data` is a mixed list of array data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list; values list can be `7h`, `11h` or mixed list of -7|-11|4|99|101h

the function

1.  prints table contents to stdout 
1.  returns generic null

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field’s datatype.

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::Table::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q).arrowkdb.tb.prettyPrintTable[schema;((1 2 3j);(4 5 6f);("aa";"bb";"cc"));::]
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

*Convert a kdb+ table to an Arrow table and pretty print the table, inferring the schema from the kdb+ table structure*

```txt
.arrowkdb.tb.prettyPrintTableFromTable[table;options]
```

Where:

- `table` is a kdb+ table
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list; values list can be `7h`, `11h` or mixed list of -7|-11|4|99|101h

the function

1.  prints table contents to stdout 
1.  returns generic null

Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferreddatatypes).

Supported options:

- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

> :warning: **Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors**
>
> Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferred-datatypes).

> :warning: **For debugging use only**
>
> The information is generated by the `arrow::Table::ToString()` functionality and displayed on stdout to preserve formatting and indentation.

```q
q).arrowkdb.tb.prettyPrintTableFromTable[([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"));::]
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

## Apache ORC files

### `orc.writeOrc`

*Convert a kdb+ mixed list of array data to an Arrow table and write to an Apache ORC file*
```txt
.arrowkdb.orc.writeOrc[orc_file;schema_id;array_data;options]
```

Where:

- `orc_file` is a string containing the ORC file name
- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns generic null on success

> :warning: The Apache ORC file format is not supported on Windows platform and will return with the message **ORC files are not supported on Windows**.

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field’s datatype.

Supported options:

- `ORC_CHUNK_SIZE` - Controls the approximate size of ORC data stripes within a column.  Long, default 1MB.

> :warning: The Apache ORC file format is [less](https://arrow.apache.org/docs/cpp/orc.html) fully featured compared to Parquet and consequently the ORC dataloader currently **does not support unsigned datatypes**.

```q
q)i8_fd:.arrowkdb.fd.field[`int8;.arrowkdb.dt.int8[]];
q)i16_fd:.arrowkdb.fd.field[`int16;.arrowkdb.dt.int16[]];
q)i32_fd:.arrowkdb.fd.field[`int32;.arrowkdb.dt.int32[]];
q)orc_schema:.arrowkdb.sc.schema[(i8_fd,i16_fd,i32_fd)];
q)orc_data:(5?0x64;5?100h;5?100i);
q).arrowkdb.orc.writeOrc["dataloader.orc";orc_schema;orc_data;options]
```

## `orc.writeOrcFromTable`

*Convert a kdb+ table to an Arrow table and write to an Apache ORC file, inferring the schema from the kdb+ table structure*

```txt
.arrowkdb.orc.writeOrcFromTable[orc_file;table;options]
```

Where:

- `orc_file` is a string containing the ORC file name
- `table` is a kdb+ table
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns generic null on success

> :warning: The Apache ORC file format is not supported on Windows platform and will return with the message **ORC files are not supported on Windows**.

Supported options:

- `ORC_CHUNK_SIZE` - Controls the approximate size of ORC data stripes within a column.  Long, default 1MB.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values. See [here](null-mapping.md) for more details.

> :warning: The Apache ORC file format is [less](https://arrow.apache.org/docs/cpp/orc.html) fully featured compared to Parquet and consequently the ORC dataloader currently **does not support unsigned datatypes**.

> :warning: **Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors**
>
> Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferred-datatypes).

```q
q)table:([] i8_fd:5?0x64; i16_fd:5?100h; i32_fd:5?100i)
q).arrowkdb.orc.writeOrcFromTable["dataloader.orc";table;::]
```

### `orc.readOrcSchema`

*Read the schema from an Apache ORC file*

```txt
.arrowkdb.orc.readOrcSchema[orc_file]
```

Where `orc_file` is a string containing the ORC file name

returns the schema identifier

> :warning: The Apache ORC file format is not supported on Windows platform and will return with the message **ORC files are not supported on Windows**.

```q
q)i8_fd:.arrowkdb.fd.field[`int8;.arrowkdb.dt.int8[]];
q)i16_fd:.arrowkdb.fd.field[`int16;.arrowkdb.dt.int16[]];
q)i32_fd:.arrowkdb.fd.field[`int32;.arrowkdb.dt.int32[]];
q)orc_schema:.arrowkdb.sc.schema[(i8_fd,i16_fd,i32_fd)];
q)orc_data:(5?0x64;5?100h;5?100i);
q).arrowkdb.orc.writeOrc["dataloader.orc";orc_schema;orc_data;options]
q).arrowkdb.sc.equalSchemas[schema;.arrowkdb.orc.readOrcSchema["dataloader.orc"]]
1b
```

### `orc.readOrcData`

*Read an Arrow table from an Apache ORC file and convert to a kdb+ mixed list of array data*

```txt
.arrowkdb.orc.readOrcData[orc_file;options]
```

Where:

- `orc_file` is a string containing the ORC file name
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the array data

> :warning: The Apache ORC file format is not supported on Windows platform and will return with the message **ORC files are not supported on Windows**.

Supported options:

- `ORC_CHUNK_SIZE` - Controls the approximate size of ORC data stripes within a column.  Long, default 1MB.
- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures. See [here](null-bitmap.md) for more details. Long, default 0.

```q
q)i8_fd:.arrowkdb.fd.field[`int8;.arrowkdb.dt.int8[]];
q)i16_fd:.arrowkdb.fd.field[`int16;.arrowkdb.dt.int16[]];
q)i32_fd:.arrowkdb.fd.field[`int32;.arrowkdb.dt.int32[]];
q)orc_schema:.arrowkdb.sc.schema[(i8_fd,i16_fd,i32_fd)];
q)orc_data:(5?0x64;5?100h;5?100i);
q).arrowkdb.orc.writeOrc["dataloader.orc";orc_schema;orc_data;options]
q)read_data:.arrowkdb.orc.readOrcData["dataloader.orc";::]
q)orc_data~read_data
1b
```

### `orc.readOrcToTable`

*Read an Arrow table from an Apache ORC file and convert to a kdb+ table*

```txt
.arrowkdb.orc.readOrcToTable[orc_file;options]
```

Where:

- `orc_file` is a string containing the ORC file name
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the kdb+ table

> :warning: The Apache ORC file format is not supported on Windows platform and will return with the message **ORC files are not supported on Windows**.

Each schema field name is used as the column name and the Arrow array data is used as the column data.

Supported options:

- `ORC_CHUNK_SIZE` - Controls the approximate size of ORC data stripes within a column.  Long, default 1MB.

```q
q)table:([] i8_fd:5?0x64; i16_fd:5?100h; i32_fd:5?100i)
q).arrowkdb.orc.writeOrcFromTable["dataloader.orc";table;::]
q)read_table:.arrowkdb.orc.readOrcToTable["dataloader.orc";::]
q)read_table~table
1b
```

## Parquet files

### `pq.writeParquet`

*Convert a kdb+ mixed list of array data to an Arrow table and write to a Parquet file*

```txt
.arrowkdb.pq.writeParquet[parquet_file;schema_id;array_data;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns generic null on success

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field’s datatype.

Supported options:

- `PARQUET_CHUNK_SIZE` - Controls the approximate size of encoded data pages within a column chunk.  Long, default 1MB.
- `PARQUET_VERSION` - Select the Parquet format version: `V1.0`, `V2.0`, `V2.4`, `V2.6` or `V2.LATEST`.  Later versions are more fully featured but may be incompatible with older Parquet implementations.  Default `V1.0`
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

> :warning: **The Parquet format is compressed and designed for for maximum space efficiency which may cause a performance overhead compared to Arrow.  Parquet is also less fully featured than Arrow which can result in schema limitations**
>
> The Parquet file format is less fully featured compared to Arrow and consequently the Arrow/Parquet file writer currently does not support some datatypes or represents them using a different datatype as described [here](#parquet-datatype-limitations)

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

### `pq.writeParquetFromTable`

*Convert a kdb+ table to an Arrow table and write to a Parquet file, inferring the schema from the kdb+ table structure*

```txt
.arrowkdb.pq.writeParquetFromTable[parquet_file;table;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `table` is a kdb+ table
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns generic null on success

Supported options:

- `PARQUET_CHUNK_SIZE` - Controls the approximate size of encoded data pages within a column chunk.  Long, default 1MB.
- `PARQUET_VERSION` - Select the Parquet format version: `V1.0`, `V2.0`, `V2.4`, `V2.6` or `V2.LATEST`.  Later versions are more fully featured but may be incompatible with older Parquet implementations.  Default `V1.0`
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

> :warning: **Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors**
>
> Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferred-datatypes).

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q)read_table:.arrowkdb.pq.readParquetToTable["file.parquet";::]
q)read_table~table
1b
```

### `pq.readParquetSchema`

*Read the schema from a Parquet file*

```txt
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

### `pq.readParquetData`

*Read an Arrow table from a Parquet file and convert to a kdb+ mixed list of array data*

```txt
.arrowkdb.pq.readParquetData[parquet_file;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the array data

Supported options:

- `PARQUET_MULTITHREADED_READ` - Flag indicating whether the Parquet reader should run in multithreaded mode.   This can improve performance by processing multiple columns in parallel.  Long, default 0.
- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

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

### `pq.readParquetColumn`

*Read a single column from a Parquet file and convert to a kdb+ list*

```txt
.arrowkdb.pq.readParquetColumn[parquet_file;column_index;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `column_index` is the index of the column to read, relative to the schema field order
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the array’s data

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquet["file.parquet";schema;array_data;::]
q)col1:.arrowkdb.pq.readParquetColumn["file.parquet";1i;::]
q)col1~array_data[1]
1b
```

### `pq.readParquetToTable`

*Read an Arrow table from a Parquet file and convert to a kdb+ table*

```txt
.arrowkdb.pq.readParquetToTable[parquet_file;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the kdb+ table

Each schema field name is used as the column name and the Arrow array data is used as the column data.

Supported options:

- `PARQUET_MULTITHREADED_READ` - Flag indicating whether the Parquet reader should run in multithreaded mode.   This can improve performance by processing multiple columns in parallel.  Long, default 0.
- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q)read_table:.arrowkdb.pq.readParquetToTable["file.parquet";::]
q)read_table~table
1b
```

### `pq.readParquetNumRowGroups`

*Read the number of row groups used by a Parquet file*

```txt
.arrowkdb.pq.readParquetNumRowGroups[parquet_file]
```

Where `parquet_file` is a string containing the Parquet file name

returns the number of row groups

```q
q)table:([]a:10000000#0;b:10000000#1)
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q).arrowkdb.pq.readParquetNumRowGroups["file.parquet"]
10i
```

### `pq.readParquetRowGroups`

*Read a set of row groups from a Parquet file into an Arrow table then convert to a kdb+ mixed list of array data*

```txt
.arrowkdb.pq.readParquetRowGroups[parquet_file;row_groups;columns;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `row_groups` is an integer list (6h) of row groups indices to read, or generic null (`::`) to read all row groups
- `columns` is an integer list (6h) of column indices to read, or generic null (`::`) to read all columns
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the array data

Supported options:

- `PARQUET_MULTITHREADED_READ` - Flag indicating whether the Parquet reader should run in multithreaded mode.   This can improve performance by processing multiple columns in parallel.  Long, default 0.
- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)table:([]a:10000000#0;b:10000000#1)
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q).arrowkdb.pq.readParquetRowGroups["file.parquet";1 2i;enlist 0i;::]
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0..
q)count .arrowkdb.pq.readParquetRowGroups["file.parquet";1 2i;enlist 0i;::]
1
q)count first .arrowkdb.pq.readParquetRowGroups["file.parquet";1 2i;enlist 0i;::]
2097152
```

### `pq.readParquetRowGroupsToTable`

*Read a set of row groups from a Parquet file into an Arrow table then convert to a kdb+ table*

```txt
.arrowkdb.pq.readParquetRowGroupsToTable[parquet_file;row_groups;columns;options]
```

Where:

- `parquet_file` is a string containing the Parquet file name
- `row_groups` is an integer list (6h) of row groups indices to read, or generic null (`::`) to read all row groups
- `columns` is an integer list (6h) of column indices to read, or generic null (`::`) to read all columns
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the kdb+ table

Supported options:

- `PARQUET_MULTITHREADED_READ` - Flag indicating whether the Parquet reader should run in multithreaded mode.   This can improve performance by processing multiple columns in parallel.  Long, default 0.
- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)table:([]a:10000000#0;b:10000000#1)
q).arrowkdb.pq.writeParquetFromTable["file.parquet";table;::]
q)meta .arrowkdb.pq.readParquetRowGroupsToTable["file.parquet";1 2i;enlist 0i;::]
c| t f a
-| -----
a| j
q)count .arrowkdb.pq.readParquetRowGroupsToTable["file.parquet";1 2i;enlist 0i;::]
2097152
```

### Arrow IPC files

### `ipc.writeArrow`

*Convert a kdb+ mixed list of array data to an Arrow table and write to an Arrow file*

```txt
.arrowkdb.ipc.writeArrow[arrow_file;schema_id;array_data;options]
```

Where:

- `arrow_file` is a string containing the Arrow file name
- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns generic null on success

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field’s datatype.

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `ARROW_CHUNK_ROWS` - The number of rows to include in each arrow array.  If the total rows in the kdb data are greater then the kdb lists are chunked into the arrow IPC writer.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrow["file.arrow";schema;array_data;::]
q)read_data:.arrowkdb.ipc.readArrowData["file.arrow";::]
q)read_data~array_data
1b
```

> :warning: **When writing a large table Arrow may raise 'Capacity error: Cannot write arrays larger than 2^31 - 1 in length**. 
>
> Preferable [way](https://arrow.apache.org/docs/python/ipc.html) of serializing of such a table is dividing it into chunks by specifying `ARROW_CHUNK_ROWS` option.


### `ipc.writeArrowFromTable`

*Convert a kdb+ table to an Arrow table and write to an Arrow file, inferring the schema from the kdb+ table structure*

```txt
.arrowkdb.ipc.writeArrowFromTable[arrow_file;table;options]
```

Where:

- `arrow_file` is a string containing the Arrow file name
- `table` is a kdb+ table
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns generic null on success

Supported options:

- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `ARROW_CHUNK_ROWS` - The number of rows to include in each arrow array.  If the total rows in the kdb data are greater then the kdb lists are chunked into the arrow IPC writer.

> :warning: **Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors**
>
> Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferred-datatypes).

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrowFromTable["file.arrow";table;::]
q)read_table:.arrowkdb.ipc.readArrowToTable["file.arrow";::]
q)read_table~table
1b
```

> :warning: **When writing a large table Arrow may raise 'Capacity error: Cannot write arrays larger than 2^31 - 1 in length**. 
>
> Preferable [way](https://arrow.apache.org/docs/python/ipc.html) of serializing of such a table is dividing it into chunks by specifying `ARROW_CHUNK_ROWS` option.

```q
table:([]col:2147483652#0x00)
options:(``ARROW_CHUNK_ROWS)!((::);214748365)
.arrowkdb.ipc.writeArrowFromTable["table.arrow";table;options]
```
### `ipc.readArrowSchema`

*Read the schema from an Arrow file*

```txt
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
q).arrowkdb.ipc.writeArrow["file.arrow";schema;array_data;::]
q).arrowkdb.sc.equalSchemas[schema;.arrowkdb.ipc.readArrowSchema["file.arrow"]]
1b
```

### `ipc.readArrowData`

*Read an Arrow table from an Arrow file and convert to a kdb+ mixed list of array data*

```txt
.arrowkdb.ipc.readArrowData[arrow_file;options]
```

Where:

-  `arrow_file` is a string containing the Arrow file name
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the array data

Supported options:

- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrow["file.arrow";schema;array_data;::]
q)read_data:.arrowkdb.ipc.readArrowData["file.arrow";::]
q)read_data~array_data
1b
```

### `ipc.readArrowToTable`

*Read an Arrow table from an Arrow file and convert to a kdb+ table*

```txt
.arrowkdb.ipc.readArrowToTable[arrow_file;options]
```

Where:

-  `arrow_file` is a string containing the Arrow file name
-  `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the kdb+ table

Each schema field name is used as the column name and the Arrow array data is used as the column data.

Supported options:

- `USE_MMAP` - Flag indicating whether the Parquet file should be memory mapped in.  This can improve performance on systems which support mmap.  Long, default: 0.
- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q).arrowkdb.ipc.writeArrowFromTable["file.arrow";table;::]
q)read_table:.arrowkdb.ipc.readArrowToTable["file.arrow";::]
q)read_table~table
1b
```

## Arrow IPC streams

### `ipc.serializeArrow`

*Convert a kdb+ mixed list of array data to an Arrow table and serialize to an Arrow stream*

```txt
.arrowkdb.ipc.serializeArrow[schema_id;array_data;options]
```

Where:

- `schema_id` is the schema identifier to use for the table
- `array_data` is a mixed list of array data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns a byte list containing the serialized stream data

The mixed list of Arrow array data should be ordered in schema field number and each list item representing one of the arrays must be structured according to the field’s datatype.

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `ARROW_CHUNK_ROWS` - The number of rows to include in each arrow array.  If the total rows in the kdb data are greater then the kdb lists are chunked into the arrow IPC writer.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrow[schema;array_data;::]
q)read_data:.arrowkdb.ipc.parseArrowData[serialized;::]
q)read_data~array_data
1b
```

> :warning: **When writing a large table Arrow may raise 'Capacity error: Cannot write arrays larger than 2^31 - 1 in length**. 
>
> Preferable [way](https://arrow.apache.org/docs/python/ipc.html) of serializing of such a table is dividing it into chunks by specifying `ARROW_CHUNK_ROWS` option.

### `ipc.serializeArrowFromTable`

*Convert a kdb+ table to an Arrow table and serialize to an Arrow stream, inferring the schema from the kdb+ table structure*

```txt
.arrowkdb.ipc.serializeArrowFromTable[table;options]
```

Where:

- `table` is a kdb+ table
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns a byte list containing the serialized stream data

Supported options:

- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `ARROW_CHUNK_ROWS` - The number of rows to include in each arrow array.  If the total rows in the kdb data are greater then the kdb lists are chunked into the arrow IPC writer.

> :warning: **Inferred schemas only support a subset of the Arrow datatypes and is considerably less flexible than creating them with the datatype/field/schema constructors**
>
> Each column in the table is mapped to a field in the schema.  The column name is used as the field name and the column’s kdb+ type is mapped to an Arrow datatype as as described [here](#inferred-datatypes).

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrowFromTable[table;::]
q)new_table:.arrowkdb.ipc.parseArrowToTable[serialized;::]
q)new_table~table
1b
```

> :warning: **When writing a large table Arrow may raise 'Capacity error: Cannot write arrays larger than 2^31 - 1 in length**. 
>
> Preferable [way](https://arrow.apache.org/docs/python/ipc.html) of serializing of such a table is dividing it into chunks by specifying `ARROW_CHUNK_ROWS` option.

```q
table:([]col:2147483652#0x00)
options:(``ARROW_CHUNK_ROWS)!((::);214748365)
serialized:.arrowkdb.ipc.serializeArrowFromTable["table.arrow";table;options]
```

### `ipc.parseArrowSchema`

*Parse the schema from an Arrow stream*

```txt
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
q)serialized:.arrowkdb.ipc.serializeArrow[schema;array_data;::]
q).arrowkdb.sc.equalSchemas[schema;.arrowkdb.ipc.parseArrowSchema[serialized]]
1b
```

### `ipc.parseArrowData`

*Parse an Arrow table from an Arrow stream and convert to a kdb+ mixed list of array data*

```txt
.arrowkdb.ipc.parseArrowData[serialized;options]
```

Where:

- `serialized` is a byte list containing the serialized stream data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the array data

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)f1:.arrowkdb.fd.field[`int_field;.arrowkdb.dt.int64[]]
q)f2:.arrowkdb.fd.field[`float_field;.arrowkdb.dt.float64[]]
q)f3:.arrowkdb.fd.field[`str_field;.arrowkdb.dt.utf8[]]
q)schema:.arrowkdb.sc.schema[(f1,f2,f3)]
q)array_data:((1 2 3j);(4 5 6f);("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrow[schema;array_data;::]
q)read_data:.arrowkdb.ipc.parseArrowData[serialized;::]
q)read_data~array_data
1b
```

### `ipc.parseArrowToTable`

*Parse an Arrow table from an Arrow file and convert to a kdb+ table*

```txt
.arrowkdb.ipc.parseArrowToTable[serialized;options]
```

Where:

- `serialized` is a byte list containing the serialized stream data
- `options` is a kdb+ dictionary of options or generic null (`::`) to use defaults.  Dictionary key must be a `11h` list. Values list can be `7h`, `11h` or mixed list of `-7|-11|4|99|101h`.

returns the kdb+ table

Each schema field name is used as the column name and the Arrow array data is used as the column data.

Supported options:

- `DECIMAL128_AS_DOUBLE` - Flag indicating whether to override the default type mapping for the Arrow decimal128 datatype and instead represent it as a double (9h).  Long, default 0.
- `NULL_MAPPING` - Sub-directory of null mapping datatypes and values.  See [here](null-mapping.md) for more details.
- `WITH_NULL_BITMAP` - Flag indicating whether to return the data values and the null bitmap as separate structures.  See [here](null-bitmap.md) for more details.  Long, default 0.

```q
q)table:([] int_field:(1 2 3); float_field:(4 5 6f); str_field:("aa";"bb";"cc"))
q)serialized:.arrowkdb.ipc.serializeArrowFromTable[table;::]
q)new_table:.arrowkdb.ipc.parseArrowToTable[serialized;::]
q)new_table~table
1b
```

## Utilities

### `util.buildInfo`

*Return build information regarding the in use Arrow library*

```txt
.arrowkdb.util.buildInfo[]
```

Returns a dictionary detailing various Arrow build info including: Arrow version, shared object version, git description and compiler used.

```q
q).arrowkdb.util.buildInfo[]
version         | 3000000i
version_string  | `3.0.0-SNAPSHOT
full_so_version | `300.0.0
compiler_id     | `MSVC
compiler_version| `19.26.28806.0
compiler_flags  | `/DWIN32 /D_WINDOWS  /GR /EHsc /D_SILENCE_TR1_NAMESPACE_DEP..
git_id          | `c8c2110cd7d01d2f4420079c450997ef5fa89029
git_description | `apache-arrow-2.0.0-194-gc8c2110cd
package_kind    | `
```

