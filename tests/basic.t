// test.t

-1 "\n+----------|| Load arrowkdb library ||----------+\n";

\l q/arrowkdb.q

// Move to arrowkdb namespace
\d .arrowkdb


-1 "\n+----------|| Set up examples of all datatypes ||----------+\n";

na_dt:dt.na[]
boolean_dt:dt.boolean[]
uint8_dt:dt.uint8[]
int8_dt:dt.int8[]
uint16_dt:dt.uint16[]
int16_dt:dt.int16[]
uint32_dt:dt.uint32[]
int32_dt:dt.int32[]
uint64_dt:dt.uint64[]
int64_dt:dt.int64[]
float16_dt:dt.float16[]
float32_dt:dt.float32[]
float64_dt:dt.float64[]
utf8_dt:dt.utf8[]
large_utf8_dt:dt.large_utf8[]
binary_dt:dt.binary[]
large_binary_dt:dt.large_binary[]
date32_dt:dt.date32[]
date64_dt:dt.date64[]
month_interval_dt:dt.month_interval[]
day_time_interval_dt:dt.day_time_interval[]
fixed_size_binary_dt:dt.fixed_size_binary[2i]
timestamp_dt:dt.timestamp[`NANO]
time32_dt:dt.time32[`MILLI]
time64_dt:dt.time64[`NANO]
duration_dt:dt.duration[`MILLI]
decimal128_dt:dt.decimal128[38i;2i]
list_dt:dt.list[int64_dt]
large_list_dt:dt.large_list[float64_dt]
fixed_size_list_dt:dt.fixed_size_list[int64_dt; 4i]
map_dt:dt.map[int64_dt;float64_dt]
dictionary_dt:dt.dictionary[utf8_dt;int64_dt]

all_dt:(na_dt,boolean_dt,uint8_dt,int8_dt,uint16_dt,int16_dt,uint32_dt,int32_dt,uint64_dt,int64_dt,float16_dt,float32_dt,float64_dt,utf8_dt,large_utf8_dt,binary_dt,large_binary_dt,date32_dt,date64_dt, month_interval_dt,day_time_interval_dt,fixed_size_binary_dt,timestamp_dt,time32_dt,time64_dt,duration_dt,decimal128_dt,list_dt,large_list_dt,fixed_size_list_dt,map_dt,dictionary_dt)


-1 "\n+----------|| Set up a field for each datatypes ||----------+\n";

na_fd:fd.field[`na;na_dt]
boolean_fd:fd.field[`boolean;boolean_dt]
uint8_fd:fd.field[`uint8;uint8_dt]
int8_fd:fd.field[`int8;int8_dt]
uint16_fd:fd.field[`uint16;uint16_dt]
int16_fd:fd.field[`int16;int16_dt]
uint32_fd:fd.field[`uint32;uint32_dt]
int32_fd:fd.field[`int32;int32_dt]
uint64_fd:fd.field[`uint64;uint64_dt]
int64_fd:fd.field[`int64;int64_dt]
float16_fd:fd.field[`float16;float16_dt]
float32_fd:fd.field[`float32;float32_dt]
float64_fd:fd.field[`float64;float64_dt]
utf8_fd:fd.field[`utf8;utf8_dt]
large_utf8_fd:fd.field[`large_utf8;large_utf8_dt]
binary_fd:fd.field[`binary;binary_dt]
large_binary_fd:fd.field[`large_binary;large_binary_dt]
date32_fd:fd.field[`date32;date32_dt]
date64_fd:fd.field[`date64;date64_dt]
month_interval_fd:fd.field[`month_interval;month_interval_dt]
day_time_interval_fd:fd.field[`day_time_interval;day_time_interval_dt]
fixed_size_binary_fd:fd.field[`fixed_size_binary;fixed_size_binary_dt]
timestamp_fd:fd.field[`timestamp;timestamp_dt]
time32_fd:fd.field[`time32;time32_dt]
time64_fd:fd.field[`time64;time64_dt]
duration_fd:fd.field[`duration;duration_dt]
decimal128_fd:fd.field[`decimal128;decimal128_dt]
list_fd:fd.field[`list;list_dt]
large_list_fd:fd.field[`large_list;list_dt]
fixed_size_list_fd:fd.field[`fixed_size_list;fixed_size_list_dt]
map_fd:fd.field[`map;map_dt]
dictionary_fd:fd.field[`dictionary;dictionary_dt]

all_fd:(na_fd,boolean_fd,uint8_fd,int8_fd,uint16_fd,int16_fd,uint32_fd,int32_fd,uint64_fd,int64_fd,float16_fd,float32_fd,float64_fd,utf8_fd,large_utf8_fd,binary_fd,large_binary_fd,date32_fd,date64_fd, month_interval_fd,day_time_interval_fd,fixed_size_binary_fd,timestamp_fd,time32_fd,time64_fd,duration_fd,decimal128_fd,list_fd,large_list_fd,fixed_size_list_fd,map_fd,dictionary_fd)


-1 "\n+----------|| Set up a struct/union datatypes ||----------+\n";

struct_dt:dt.struct[(int64_fd,float64_fd)]
sparse_union_dt:dt.sparse_union[(int64_fd,float64_fd)]
dense_union_dt:dt.dense_union[(int64_fd,float64_fd)]

all_dt:all_dt,(struct_dt,sparse_union_dt,dense_union_dt)


-1 "\n+----------|| Set up a field for struct/union datatypes ||----------+\n";

struct_fd:fd.field[`struct;struct_dt]
sparse_union_fd:fd.field[`sparse_union;sparse_union_dt]
dense_union_fd:fd.field[`dense_union;dense_union_dt]

all_fd:all_fd,(struct_fd,sparse_union_fd,dense_union_fd)


-1 "\n+----------|| Test datatype inspection ||----------+\n";

dt.datatypeName[int64_dt]~`int64
dt.getTimeUnit[timestamp_dt]~`NANO
dt.getByteWidth[fixed_size_binary_dt]~2i
dt.getListSize[fixed_size_list_dt]~4i
(dt.getPrecisionScale[decimal128_dt])[0]~38i
(dt.getPrecisionScale[decimal128_dt])[1]~2i
dt.getListDatatype[list_dt]~int64_dt
(dt.getMapDatatypes[map_dt])[0]~int64_dt
(dt.getMapDatatypes[map_dt])[1]~float64_dt
(dt.getDictionaryDatatypes[dictionary_dt])[0]~utf8_dt
(dt.getDictionaryDatatypes[dictionary_dt])[1]~int64_dt
dt.getChildFields[struct_dt]~(int64_fd,float64_fd)
dt.getChildFields[sparse_union_dt]~(int64_fd,float64_fd)


-1 "\n+----------|| Test field inspection ||----------+\n";

fd.fieldName[utf8_fd]~`utf8
fd.fieldDatatype[utf8_fd]~utf8_dt


-1 "\n+----------|| Create array data for each field ||----------+\n";

na_data:(();();())
boolean_data:101b
uint8_data:int8_data:0x001122
uint16_data:int16_data:0 1 2h
uint32_data:int32_data:0 1 2i
uint64_data:int64_data:0 1 2j
float16_data:0 1 2h // Arrow uses uint16_t for float16
float32_data:0 1 2e
float64_data:0 1 2f
utf8_data:large_utf8_data:(enlist "a";"bb";"ccc")
binary_data:large_binary_data:(enlist 0x11;0x2222;0x333333)
date32_data:(2001.01.01 2002.02.02 2003.03.03)
date64_data:(2001.01.01D00:00:00.000000000 2002.02.02D00:00:00.000000000 2003.03.03D00:00:00.000000000)
month_interval_data:(2001.01m,2002.02m,2003.03m)
day_time_interval_data:(0D01:00:00.100000000 0D02:00:00.200000000 0D03:00:00.300000000)
fixed_size_binary_data:(0x1111;0x2222;0x3333)
timestamp_data:(2001.01.01D00:00:00.100000001 2002.02.02D00:00:00.200000002 2003.03.03D00:00:00.300000003)
time32_data:(01:00:00.100 02:00:00.200 03:00:00.300)
time64_data:(0D01:00:00.100000001 0D02:00:00.200000002 0D03:00:00.300000003)
duration_data:(0D01:00:00.100000000 0D02:00:00.200000000 0D03:00:00.300000000)
decimal128_data:(0x00000000000000000000000000000000; 0x01000000000000000000000000000000; 0x00000000000000000000000000000080)
list_data:large_list_data:(enlist 1;(2 2);(3 3 3))
fixed_size_list_data:((1 1 1 1);(2 2 2 2);(3 3 3 3))
map_data:((enlist 1)!(enlist 1f);(2 2)!(2 2f);(3 3 3)!(3 3 3f))
dictionary_data:(("aa";"bb";"cc");(2 0 1))
struct_data:(1 2 3;4 5 6f)
sparse_union_data:dense_union_data:(0 1 0h;1 2 3;4 5 6f)


-1 "\n+----------|| Test integer types schema ||----------+\n";

fields:(uint8_fd,int8_fd,uint16_fd,int16_fd,uint32_fd,int32_fd,uint64_fd,int64_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(uint8_data;int8_data;uint16_data;int16_data;uint32_data;int32_data;uint64_data;int64_data)
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]}

-1 "<--- Read/write parquet --->";

// Use Parquet v2.0
// This is required otherwise the uint32 is converted to int64
parquet_write_options:(enlist `PARQUET_VERSION)!(enlist `V2.0)

filename:"ints.parquet"
pq.writeParquet[filename;schema;array_data;parquet_write_options]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"ints.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test floats/boolean/na/decimal types schema ||----------+\n";

fields:(float32_fd,float64_fd,boolean_fd,na_fd,decimal128_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(float32_data;float64_data;boolean_data;na_data;decimal128_data)

-1 "<--- Read/write parquet --->";

filename:"floats_bool_na_dec.parquet"
pq.writeParquet[filename;schema;array_data;::]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"floats_bool_na_dec.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test utf8/binary types schema ||----------+\n";

fields:(utf8_fd,binary_fd,fixed_size_binary_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(utf8_data;binary_data;fixed_size_binary_data)

-1 "<--- Read/write parquet --->";

filename:"utf8_binary.parquet"
pq.writeParquet[filename;schema;array_data;::]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"utf8_binary.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test temporal types schema ||----------+\n";

fields:(date32_fd,timestamp_fd,time32_fd,time64_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(date32_data;timestamp_data;time32_data;time64_data)

-1 "<--- Read/write parquet --->";

// Use Parquet v2.0
// This is required otherwise the timestamp[nano] is converted to timestamp[milli]
parquet_write_options:(enlist `PARQUET_VERSION)!(enlist `V2.0)

filename:"temporal.parquet"
pq.writeParquet[filename;schema;array_data;parquet_write_options]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"temporal.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test lists types schema ||----------+\n";

fields:(list_fd,large_list_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(list_data;large_list_data)

-1 "<--- Read/write parquet --->";

filename:"lists.parquet"
pq.writeParquet[filename;schema;array_data;::]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"lists.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test map/struct types schema ||----------+\n";

fields:(map_fd,struct_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(map_data;struct_data)

-1 "<--- Read/write parquet --->";

filename:"map_struct.parquet"
pq.writeParquet[filename;schema;array_data;parquet_write_options]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"map_struct.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test simple arrow only types schema ||----------+\n";

// Parquet doesn't support:
// * float16
// * large_utf8
// * large_binary
// * month_interval
// * day_time_interval
// * duration

// Parquet changes datatype:
// * date64 to date32[days]

fields:(float16_fd,large_utf8_fd,large_binary_fd,month_interval_fd,day_time_interval_fd,duration_fd,date64_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(float16_data;large_utf8_data;large_binary_data;month_interval_data;day_time_interval_data;duration_data;date64_data)

-1 "<--- Read/write arrow file --->";

filename:"simple_arrow_only.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test nested arrow only types schema ||----------+\n";

// Parquet doesn't support:
// * sparse_union
// * dense_union

// Parquet changes datatype:
// * fixed_size_list to list
// * dictionary to its categorical interpretation

fields:(fixed_size_list_fd,sparse_union_fd,dense_union_fd,dictionary_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(fixed_size_list_data;sparse_union_data;dense_union_data;dictionary_data)

-1 "<--- Read/write arrow file --->";

filename:"nested_arrow_only.arrow"
ipc.writeArrow[filename;schema;array_data;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrow[schema;array_data;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Test compression ||----------+\n";

fields:(uint8_fd,int8_fd,uint16_fd,int16_fd,uint32_fd,int32_fd,uint64_fd,int64_fd)
schema:sc.schema[fields]
sc.schemaFields[schema]~fields
array_data:(uint8_data;int8_data;uint16_data;int16_data;uint32_data;int32_data;uint64_data;int64_data)

-1 "<--- Read/write GZIP parquet --->";

// Use Parquet v2.0 & GZIP compression
parquet_write_options:(`PARQUET_VERSION`COMPRESSION)!(`V2.0`GZIP)

filename:"gzip.parquet"
pq.writeParquet[filename;schema;array_data;parquet_write_options]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write SNAPPY parquet --->";

// Use Parquet v2.0 & SNAPPY compression
parquet_write_options:(`PARQUET_VERSION`COMPRESSION)!(`V2.0`SNAPPY)

filename:"snappy.parquet"
pq.writeParquet[filename;schema;array_data;parquet_write_options]
pq.readParquetSchema[filename]~schema
pq.readParquetData[filename;::]~array_data
rm filename;

-1 "<--- Read/write UNCOMPRESSED arrow file --->";

// Use UNCOMPRESSED compression
arrow_write_options:(enlist `COMPRESSION)!(enlist `UNCOMPRESSED)

filename:"uncompressed.arrow"
ipc.writeArrow[filename;schema;array_data;arrow_write_options]
ipc.readArrowSchema[filename]~schema
ipc.readArrowData[filename;::]~array_data
rm filename;

-1 "<--- Read/write ZSTD arrow stream --->";

// Use ZSTD compression
arrow_write_options:(enlist `COMPRESSION)!(enlist `ZSTD)

serialized:ipc.serializeArrow[schema;array_data;arrow_write_options]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowData[serialized;::]~array_data

sc.removeSchema[schema]


-1 "\n+----------|| Clean up the constructed fields and datatypes ||----------+\n";

sc.listSchemas[]~`int$()
fd.listFields[]~all_fd
fd.removeField each all_fd;
fd.listFields[]~`int$()
dt.listDatatypes[]~all_dt
dt.removeDatatype each all_dt;
dt.listDatatypes[]~`int$()


-1 "\n+----------|| Test inferred schemas ||----------+\n";

table:([]
    boolean:boolean_data;
    int8:int8_data;
    int16:int16_data;
    int32:int32_data;
    int64:int64_data;
    float32:float32_data;
    float64:float64_data;
    timestamp:timestamp_data;
    date32:date32_data;
    time32:time32_data;
    time64:time64_data;
    utf8:utf8_data;
    binary:binary_data)

schema:sc.inferSchema[table]

-1 "<--- Read/write parquet --->";

filename:"inferred.parquet"
pq.writeParquetFromTable[filename;table;parquet_write_options]
pq.readParquetSchema[filename]~schema
pq.readParquetToTable[filename;::]~table
pq.readParquetColumn[filename;6i;::]~float64_data
rm filename;

-1 "<--- Read/write arrow file --->";

filename:"inferred.arrow"
ipc.writeArrowFromTable[filename;table;::]
ipc.readArrowSchema[filename]~schema
ipc.readArrowToTable[filename;::]~table
rm filename;

-1 "<--- Read/write arrow stream --->";

serialized:ipc.serializeArrowFromTable[table;::]
ipc.parseArrowSchema[serialized]~schema
ipc.parseArrowToTable[serialized;::]~table

sc.removeSchema[schema]


-1 "\n+----------|| Test utils ||----------+\n";

show util.buildInfo[]
(type util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
