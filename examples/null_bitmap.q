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
bitmap_opts:(`bool`int32`float64`string`date32)!(0b;1i;2.34;"start";2006.07.21);
nested_opts:(`uint16`float32`binary`time64)!(9h;8.76e;"x"$"acknowledge";00:00:00.123456789);

nested_options:(``NULL_MAPPING)!((::);bitmap_opts,nested_opts);

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

// Create a list datatype, using the uint16 datatype as its child
list_dt:.arrowkdb.dt.list[ui16_dt];

// Create a field containing the list datatype
list_fd:.arrowkdb.fd.field[`list_field;list_dt];

// Create a struct datatype using the float32, binary and time64 fields as its children
struct_dt:.arrowkdb.dt.struct[(f32_fd,bin_dt,t64_dt)];

// Create a field containing the struct datatype
struct_fd:.arrowkdb.fd.field[`struct_field;struct_dt];

// Create the schemas for the list of fields
bitmap_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,i32_fd,f64_fd,str_fd,d32_fd)];

// Create the schema containing the list and struct fields
nested_schema:.arrowkdb.sc.schema[(list_fd,struct_fd)];

// Print the schema
-1"\nBitmap schema:";
.arrowkdb.sc.printSchema[bitmap_schema];

-1"\nNested schema:";
.arrowkdb.sc.printSchema[nested_schema];

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

// Combine the data for all columns
bitmap_data:(ts_data;bool_data;i32_data;f64_data;str_data;d32_data);

// Create the data for the list array
list_data:(enlist 9h;(8h;7h);(6h;5h;4h);(1h;2h;3h;4h);(5h;6h;7h;8h;9h));

// Create the data for the struct array from its child arrays
struct_data:(f32_data;bin_data;t64_data);

// Combine the array data for the list and struct columns
nested_data:(list_data;struct_data);

// Pretty print the Arrow table populated from the bitmap data
-1"\nBitmap table:";
.arrowkdb.tb.prettyPrintTable[bitmap_schema;bitmap_data;nested_options];

// Show the array data as an arrow table
-1"\nNested table:";
.arrowkdb.tb.prettyPrintTable[nested_schema;nested_data;nested_options]

//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
nested_options[`PARQUET_VERSION]:`V2.0;

parquet_null_bitmap:"null_bitmap.parquet";
parquet_nested_bitmap:"nested_bitmap.parquet";

.arrowkdb.pq.writeParquet[parquet_null_bitmap;bitmap_schema;bitmap_data;nested_options];
.arrowkdb.pq.writeParquet[parquet_nested_bitmap;nested_schema;nested_data;nested_options];

show ls parquet_null_bitmap
show ls parquet_nested_bitmap

// Read the array data back and compare
nested_options[`WITH_NULL_BITMAP]:1;

// Read the schema back and compare
parquet_bitmap_schema:.arrowkdb.pq.readParquetSchema[parquet_null_bitmap];
parquet_nested_schema:.arrowkdb.pq.readParquetSchema[parquet_nested_bitmap];

show .arrowkdb.sc.equalSchemas[bitmap_schema;parquet_bitmap_schema]
show .arrowkdb.sc.equalSchemas[nested_schema;parquet_nested_schema]

show bitmap_schema~parquet_bitmap_schema
show nested_schema~parquet_nested_schema

parquet_bitmap_data:.arrowkdb.pq.readParquetData[parquet_null_bitmap;nested_options];
parquet_nested_data:.arrowkdb.pq.readParquetData[parquet_nested_bitmap;nested_options];

show bitmap_data~first parquet_bitmap_data
show nested_data~first parquet_nested_data

nulls_data:1b,(N-1)?1b;
bitmap_nulls:{x rotate nulls_data} each neg til {x-1} count bitmap_data;
nested_list_nulls:(enlist 1b;00b;000b;0000b;00001b)
nested_struct_nulls:(10000b;01000b;00100b)

parquet_bitmap_nulls:last parquet_bitmap_data;
parquet_list_nulls:first parquet_nested_data[1]
parquet_struct_nulls:last parquet_nested_data[1]

show bitmap_nulls~bitmap_nulls & sublist[{1-x} count parquet_bitmap_nulls;parquet_bitmap_nulls]
nested_list_nulls~parquet_list_nulls
nested_struct_nulls~parquet_struct_nulls[0]

rm parquet_null_bitmap;
rm parquet_nested_bitmap;

//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Write the schema and array data to an arrow file
arrow_null_bitmap:"null_bitmap.arrow";
arrow_nested_bitmap:"nested_bitmap.arrow";

.arrowkdb.ipc.writeArrow[arrow_null_bitmap;bitmap_schema;bitmap_data;nested_options];
.arrowkdb.ipc.writeArrow[arrow_nested_bitmap;nested_schema;nested_data;nested_options];

show ls arrow_null_bitmap
show ls arrow_nested_bitmap

// Read the schema back and compare
arrow_bitmap_schema:.arrowkdb.ipc.readArrowSchema[arrow_null_bitmap];
arrow_nested_schema:.arrowkdb.ipc.readArrowSchema[arrow_nested_bitmap];

show .arrowkdb.sc.equalSchemas[bitmap_schema;arrow_bitmap_schema]
show .arrowkdb.sc.equalSchemas[nested_schema;arrow_nested_schema]

show bitmap_schema~arrow_bitmap_schema
show nested_schema~arrow_nested_schema

// Read the array data back and compare
arrow_bitmap_data:.arrowkdb.ipc.readArrowData[arrow_null_bitmap;nested_options];
arrow_nested_data:.arrowkdb.ipc.readArrowData[arrow_nested_bitmap;nested_options];

show bitmap_data~first arrow_bitmap_data
show nested_data~first arrow_nested_data

arrow_bitmap_nulls:last arrow_bitmap_data;
arrow_list_nulls:first arrow_nested_data[1]
arrow_struct_nulls:last arrow_nested_data[1]

show bitmap_nulls~bitmap_nulls & sublist[{1-x} count arrow_bitmap_nulls;arrow_bitmap_nulls]
nested_list_nulls~arrow_list_nulls
nested_struct_nulls~arrow_struct_nulls[0]

rm arrow_null_bitmap;
rm arrow_nested_bitmap;

//-----------------------------//
// Example-3. Arrow IPC stream //
//-----------------------------//

// Serialize the schema and array data to an arrow stream
serialized_null_bitmap:.arrowkdb.ipc.serializeArrow[bitmap_schema;bitmap_data;nested_options];
serialized_nested_bitmap:.arrowkdb.ipc.serializeArrow[nested_schema;nested_data;nested_options];

show serialized_null_bitmap
show serialized_nested_bitmap

// Parse the schema back abd compare
stream_bitmap_schema:.arrowkdb.ipc.parseArrowSchema[serialized_null_bitmap];
stream_nested_schema:.arrowkdb.ipc.parseArrowSchema[serialized_nested_bitmap];

show .arrowkdb.sc.equalSchemas[bitmap_schema;stream_bitmap_schema]
show .arrowkdb.sc.equalSchemas[nested_schema;stream_nested_schema]

show bitmap_schema~stream_bitmap_schema
show nested_schema~stream_nested_schema

// Parse the array data back and compare
stream_bitmap_data:.arrowkdb.ipc.parseArrowData[serialized_null_bitmap;nested_options];
stream_nested_data:.arrowkdb.ipc.parseArrowData[serialized_nested_bitmap;nested_options];

show bitmap_data~first stream_bitmap_data
show nested_data~first stream_nested_data

stream_bitmap_nulls:last stream_bitmap_data;
stream_list_nulls:first stream_nested_data[1]
stream_struct_nulls:last stream_nested_data[1]

show bitmap_nulls~bitmap_nulls & sublist[{1-x} count stream_bitmap_nulls;stream_bitmap_nulls]
nested_list_nulls~stream_list_nulls
nested_struct_nulls~stream_struct_nulls[0]

-1 "\n+----------------------------------------+\n";

// Process off
//exit 0;
