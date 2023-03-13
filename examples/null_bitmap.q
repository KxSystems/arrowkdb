// null_bitmap.q
// Example of exposing null bitmap as a separate structure to kdb 

-1"\n+----------|| null_bitmap.q ||----------+\n";

// import the arrowkdb library
\l q/arrowkdb.q

// Filesystem functions for Linux/MacOS/Windows
ls:{[filename] $[.z.o like "w*";system "dir /b ",filename;system "ls ",filename]};
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

/////////////////////////
// CONSTRUCTED SCHEMAS //
/////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Support null mapping
bitmap_opts:(`bool`int32`float64`utf8`date32)!(0b;1i;2.34;"start";2006.07.21);
nested_struct_opts:(`uint16`float32`binary`time64)!(9h;8.76e;"x"$"acknowledge";00:00:00.123456789);
nested_dict_opts:(enlist `int64)!(enlist 5);

nested_options:(``NULL_MAPPING)!((::);bitmap_opts,nested_struct_opts,nested_dict_opts);

// Create the datatype identifiers
ts_dt:.arrowkdb.dt.timestamp[`nano];

bool_dt:.arrowkdb.dt.boolean[];
i32_dt:.arrowkdb.dt.int32[];
f64_dt:.arrowkdb.dt.float64[];
str_dt:.arrowkdb.dt.utf8[];
d32_dt:.arrowkdb.dt.date32[];

ui16_dt:.arrowkdb.dt.uint16[];

f32_dt:.arrowkdb.dt.float32[];
bin_dt:.arrowkdb.dt.binary[];
t64_dt:.arrowkdb.dt.time64[`nano];

i64_dt:.arrowkdb.dt.int64[];

// Create the field identifiers
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];
str_fd:.arrowkdb.fd.field[`string;str_dt];
d32_fd:.arrowkdb.fd.field[`date32;d32_dt];

ui16_fd:.arrowkdb.fd.field[`uint16;ui16_dt];

f32_fd:.arrowkdb.fd.field[`float32;f32_dt];
bin_fd:.arrowkdb.fd.field[`binary;bin_dt];
t64_fd:.arrowkdb.fd.field[`time64;t64_dt];

i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

// Create a field containing the list datatype
list_dt:.arrowkdb.dt.list[ui16_dt];
list_fd:.arrowkdb.fd.field[`list_field;list_dt];

// Create a field containing the struct datatype
struct_dt:.arrowkdb.dt.struct[(f32_fd,bin_fd,t64_fd)];
struct_fd:.arrowkdb.fd.field[`struct_field;struct_dt];

// Create fields containing dictionary datatypes
dict_dt:.arrowkdb.dt.dictionary[str_dt;i64_dt]
dict_fd:.arrowkdb.fd.field[`dictionary;dict_dt]
map_dt:.arrowkdb.dt.map[i64_dt;f64_dt]
map_fd:.arrowkdb.fd.field[`map;map_dt];

// Create fields containing union datatypes
sparse_dt:.arrowkdb.dt.sparse_union[(i64_fd,f64_fd)]
sparse_fd:.arrowkdb.fd.field[`sparse_union;sparse_dt]
dense_dt:.arrowkdb.dt.dense_union[(i64_fd,f64_fd)]
dense_fd:.arrowkdb.fd.field[`dense_union;dense_dt]

// Create the schemas for primitive fields
bitmap_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,i32_fd,f64_fd,str_fd,d32_fd)];

// Create the schema containing the list and struct fields
struct_schema:.arrowkdb.sc.schema[(list_fd,struct_fd)];

// Create the schema containing the dictionary and map fields
dict_schema:.arrowkdb.sc.schema[(dict_fd, map_fd)];

// Create the schema containing the sparce and dense union fields
union_schema:.arrowkdb.sc.schema[(sparse_fd, dense_fd)]

// Print the schema
-1"\nBitmap schema:";
.arrowkdb.sc.printSchema[bitmap_schema];

-1"\nStruct schema:";
.arrowkdb.sc.printSchema[struct_schema];

-1"\nDict schema:";
.arrowkdb.sc.printSchema[dict_schema];

-1"\nUnion schema:";
.arrowkdb.sc.printSchema[union_schema];

// Number of items in each array
N:10

// Create data for each column in the table
ts_data:asc N?0p;

bool_data:N?(0b;1b);
bool_data[0]:0b;
i32_data:N?100i;
i32_data[1]:1i;
f64_data:N?100f;
f64_data[2]:2.34f;
str_data:N?("start";"stop";"alert";"acknowledge";"");
str_data[3]:"start"
d32_data:N?(2006.07.21;2005.07.18;2004.07.16;2003.07.15;2002.07.11);
d32_data[4]:2006.07.21;

// Create the data for each of the struct child fields
f32_data:5?100e;
f32_data[0]:8.76e;
bin_data:5?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
bin_data[1]:"x"$"acknowledge"
t64_data:5?(12:00:00.000000000;13:00:00.000000000;14:00:00.000000000;15:00:00.000000000;16:00:00.000000000);
t64_data[2]:00:00:00.123456789;

// Create the data for the union child fields
i64_data:N?100;
i64_data[0]:1;

// Combine the data for primitive columns
bitmap_data:(ts_data;bool_data;i32_data;f64_data;str_data;d32_data);

// Combine the array data for the list and struct columns
list_array:(enlist 9h;(8h;7h);(6h;5h;4h);(1h;2h;3h;4h);(5h;6h;7h;8h;9h));
struct_array:(f32_data;bin_data;t64_data);
struct_data:(list_array;struct_array);

// Combine the array data for the list and struct columns
dict_data:(("aa";"bb";"cc");(2 0 1))
map_data:((enlist 1)!(enlist 1f);(2 2)!(2 2.34f);(3 3 3)!(3 3 3f))
dict_data:(dict_data;map_data);

// Combine the array data for the list and struct columns
sparse_data:dense_data:(0 1 0h;5 2 3;4 2.34 6f)
union_data:(sparse_data;dense_data)

// Pretty print the Arrow table populated from the bitmap data
-1"\nBitmap table:";
.arrowkdb.tb.prettyPrintTable[bitmap_schema;bitmap_data;nested_options];

// Show the array data as an arrow table
-1"\nStruct table:";
.arrowkdb.tb.prettyPrintTable[struct_schema;struct_data;nested_options]

// Show the array data as an arrow table
-1"\nDict table:";
.arrowkdb.tb.prettyPrintTable[dict_schema;dict_data;nested_options]

// Show the array data as an arrow table
-1"\nUnion table:";
.arrowkdb.tb.prettyPrintTable[union_schema;union_data;nested_options]

//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
nested_options[`PARQUET_VERSION]:`V2.0;

parquet_null_bitmap:"null_bitmap.parquet";
parquet_nested_struct:"nested_struct.parquet";
parquet_nested_dict:"nested_dict.parquet";
parquet_nested_union:"nested_union.parquet";

.arrowkdb.pq.writeParquet[parquet_null_bitmap;bitmap_schema;bitmap_data;nested_options];
.arrowkdb.pq.writeParquet[parquet_nested_struct;struct_schema;struct_data;nested_options];
.arrowkdb.pq.writeParquet[parquet_nested_dict;dict_schema;dict_data;nested_options];

show ls parquet_null_bitmap
show ls parquet_nested_struct
show ls parquet_nested_dict

// Read the schema back and compare
nested_options[`WITH_NULL_BITMAP]:1;

parquet_bitmap_schema:.arrowkdb.pq.readParquetSchema[parquet_null_bitmap];
parquet_struct_schema:.arrowkdb.pq.readParquetSchema[parquet_nested_struct];

show .arrowkdb.sc.equalSchemas[bitmap_schema;parquet_bitmap_schema]
show .arrowkdb.sc.equalSchemas[struct_schema;parquet_struct_schema]

show bitmap_schema~parquet_bitmap_schema
show struct_schema~parquet_struct_schema

// Read the array data back and compare
parquet_bitmap_data:.arrowkdb.pq.readParquetData[parquet_null_bitmap;nested_options];
parquet_struct_data:.arrowkdb.pq.readParquetData[parquet_nested_struct;nested_options];
parquet_dict_data:.arrowkdb.pq.readParquetData[parquet_nested_dict;nested_options];

show bitmap_data~first parquet_bitmap_data
show struct_data~first parquet_struct_data
show first[dict_data[0]]~asc first parquet_dict_data[0]
show last[dict_data]~last parquet_dict_data[0]

// Compare null bitmaps of parquet data
nulls_data:1b,(N-1)?1b;
bitmap_nulls:{x rotate nulls_data} each neg til {x-1} count bitmap_data;
nested_list_nulls:(enlist 1b;00b;000b;0000b;00001b);
nested_struct_nulls:(10000b;01000b;00100b);
nested_dict_nulls:(000b;000b);
nested_map_nulls:((enlist 0b)!(enlist 0b);00b!01b;000b!000b);
nested_union_nulls:((0 1 0h);100b;010b);

parquet_bitmap_nulls:last parquet_bitmap_data;
parquet_list_nulls:first parquet_struct_data[1]
parquet_struct_nulls:last parquet_struct_data[1]
parquet_dict_nulls:parquet_dict_data[1]

show bitmap_nulls~bitmap_nulls & sublist[{1-x} count parquet_bitmap_nulls;parquet_bitmap_nulls]
show nested_list_nulls~parquet_list_nulls
show nested_struct_nulls~parquet_struct_nulls
show nested_dict_nulls[0]~parquet_dict_nulls[0]
show nested_map_nulls~last[parquet_dict_nulls]

rm parquet_null_bitmap;
rm parquet_nested_struct;
rm parquet_nested_dict;

//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Write the schema and array data to an arrow file
arrow_null_bitmap:"null_bitmap.arrow";
arrow_struct_bitmap:"nested_struct.arrow";
arrow_dict_bitmap:"nested_dict.arrow";
arrow_union_bitmap:"nested_union.arrow";

.arrowkdb.ipc.writeArrow[arrow_null_bitmap;bitmap_schema;bitmap_data;nested_options];
.arrowkdb.ipc.writeArrow[arrow_struct_bitmap;struct_schema;struct_data;nested_options];
.arrowkdb.ipc.writeArrow[arrow_dict_bitmap;dict_schema;dict_data;nested_options];
.arrowkdb.ipc.writeArrow[arrow_union_bitmap;union_schema;union_data;nested_options];

show ls arrow_null_bitmap
show ls arrow_struct_bitmap
show ls arrow_dict_bitmap
show ls arrow_union_bitmap

// Read the schema back and compare
arrow_bitmap_schema:.arrowkdb.ipc.readArrowSchema[arrow_null_bitmap];
arrow_struct_schema:.arrowkdb.ipc.readArrowSchema[arrow_struct_bitmap];
arrow_dict_schema:.arrowkdb.ipc.readArrowSchema[arrow_dict_bitmap];
arrow_union_schema:.arrowkdb.ipc.readArrowSchema[arrow_union_bitmap];

show .arrowkdb.sc.equalSchemas[bitmap_schema;arrow_bitmap_schema]
show .arrowkdb.sc.equalSchemas[struct_schema;arrow_struct_schema]
show .arrowkdb.sc.equalSchemas[dict_schema;arrow_dict_schema]
show .arrowkdb.sc.equalSchemas[union_schema;arrow_union_schema]

show bitmap_schema~arrow_bitmap_schema
show struct_schema~arrow_struct_schema
show dict_schema~arrow_dict_schema
show union_schema~arrow_union_schema

// Read the array data back and compare
arrow_bitmap_data:.arrowkdb.ipc.readArrowData[arrow_null_bitmap;nested_options];
arrow_struct_data:.arrowkdb.ipc.readArrowData[arrow_struct_bitmap;nested_options];
arrow_dict_data:.arrowkdb.ipc.readArrowData[arrow_dict_bitmap;nested_options];
arrow_union_data:.arrowkdb.ipc.readArrowData[arrow_union_bitmap;nested_options];

show bitmap_data~first arrow_bitmap_data
show struct_data~first arrow_struct_data
show dict_data~first arrow_dict_data
show union_data~first arrow_union_data

// Compare null bitmaps of arrow data
arrow_bitmap_nulls:last arrow_bitmap_data;
arrow_list_nulls:first arrow_struct_data[1]
arrow_struct_nulls:last arrow_struct_data[1]
arrow_dict_nulls:arrow_dict_data[1]
arrow_union_nulls:arrow_union_data[1]

show bitmap_nulls~bitmap_nulls & sublist[{1-x} count arrow_bitmap_nulls;arrow_bitmap_nulls]
show nested_list_nulls~arrow_list_nulls
show nested_struct_nulls~arrow_struct_nulls
show nested_dict_nulls~first[arrow_dict_nulls]
show nested_map_nulls~last[arrow_dict_nulls]
show nested_union_nulls~arrow_union_nulls[0]
show nested_union_nulls~arrow_union_nulls[1]

rm arrow_null_bitmap;
rm arrow_struct_bitmap;
rm arrow_dict_bitmap;
rm arrow_union_bitmap;

//-----------------------------//
// Example-3. Arrow IPC stream //
//-----------------------------//

// Serialize the schema and array data to an arrow stream
serialized_null_bitmap:.arrowkdb.ipc.serializeArrow[bitmap_schema;bitmap_data;nested_options];
serialized_nested_struct:.arrowkdb.ipc.serializeArrow[struct_schema;struct_data;nested_options];
serialized_nested_dict:.arrowkdb.ipc.serializeArrow[dict_schema;dict_data;nested_options];
serialized_nested_union:.arrowkdb.ipc.serializeArrow[union_schema;union_data;nested_options];

show serialized_null_bitmap
show serialized_nested_struct
show serialized_nested_dict
show serialized_nested_union

// Parse the schema back abd compare
stream_bitmap_schema:.arrowkdb.ipc.parseArrowSchema[serialized_null_bitmap];
stream_struct_schema:.arrowkdb.ipc.parseArrowSchema[serialized_nested_struct];
stream_dict_schema:.arrowkdb.ipc.parseArrowSchema[serialized_nested_dict];
stream_union_schema:.arrowkdb.ipc.parseArrowSchema[serialized_nested_union];

show .arrowkdb.sc.equalSchemas[bitmap_schema;stream_bitmap_schema]
show .arrowkdb.sc.equalSchemas[struct_schema;stream_struct_schema]
show .arrowkdb.sc.equalSchemas[dict_schema;stream_dict_schema]
show .arrowkdb.sc.equalSchemas[union_schema;stream_union_schema]

show bitmap_schema~stream_bitmap_schema
show struct_schema~stream_struct_schema
show dict_schema~stream_dict_schema
show union_schema~stream_union_schema

// Parse the array data back and compare
stream_bitmap_data:.arrowkdb.ipc.parseArrowData[serialized_null_bitmap;nested_options];
stream_struct_data:.arrowkdb.ipc.parseArrowData[serialized_nested_struct;nested_options];
stream_dict_data:.arrowkdb.ipc.parseArrowData[serialized_nested_dict;nested_options];
stream_union_data:.arrowkdb.ipc.parseArrowData[serialized_nested_union;nested_options];

show bitmap_data~first stream_bitmap_data
show struct_data~first stream_struct_data
show dict_data~first stream_dict_data
show union_data~first stream_union_data

// Compare null bitmaps of stream data
stream_bitmap_nulls:last stream_bitmap_data;
stream_list_nulls:first stream_struct_data[1]
stream_struct_nulls:last stream_struct_data[1]
stream_dict_nulls:stream_dict_data[1]
stream_union_nulls:stream_union_data[1]

show bitmap_nulls~bitmap_nulls & sublist[{1-x} count stream_bitmap_nulls;stream_bitmap_nulls]
show nested_list_nulls~stream_list_nulls
show nested_struct_nulls~stream_struct_nulls
show nested_dict_nulls~first[stream_dict_nulls]
show nested_map_nulls~last[stream_dict_nulls]
show nested_union_nulls~stream_union_nulls[0]
show nested_union_nulls~stream_union_nulls[1]

-1 "\n+----------------------------------------+\n";

// Process off
//exit 0;
