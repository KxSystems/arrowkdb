// null_bitmap.q
// Example of exposing null bitmap as a separate structure to kdb 

-1"\n+----------|| null_bitmap.q ||----------+\n";

// import the arrowkdb library
\l arrowkdb.q

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

options:(``NULL_MAPPING)!((::);bitmap_opts);

// Create the datatype identifiers
ts_dt:.arrowkdb.dt.timestamp[`nano];

bool_dt:.arrowkdb.dt.boolean[];
i32_dt:.arrowkdb.dt.int32[];
f64_dt:.arrowkdb.dt.float64[];
str_dt:.arrowkdb.dt.utf8[];
d32_dt:.arrowkdb.dt.date32[];

// Create the field identifiers
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];
str_fd:.arrowkdb.fd.field[`string;str_dt];
d32_fd:.arrowkdb.fd.field[`date32;d32_dt];

// Create the schemas for the list of fields
bitmap_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,i32_fd,f64_fd,str_fd,d32_fd)];

// Print the schema
.arrowkdb.sc.printSchema[bitmap_schema];

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

// Combine the data for all columns
bitmap_data:(ts_data;bool_data;i32_data;f64_data;str_data;d32_data);

// Pretty print the Arrow table populated from the bitmap data
.arrowkdb.tb.prettyPrintTable[bitmap_schema;bitmap_data;options];

//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
options[`PARQUET_VERSION]:`V2.0;

parquet_bitmap:"null_bitmap.parquet";
.arrowkdb.pq.writeParquet[parquet_bitmap;bitmap_schema;bitmap_data;options];
show ls parquet_bitmap

// Read the array data back and compare
options[`WITH_NULL_BITMAP]:1;
parquet_bitmap_data:.arrowkdb.pq.readParquetData[parquet_bitmap;options];
show bitmap_data~first parquet_bitmap_data

nulls_data:1b,(N-1)?1b;
bitmap_nulls:{x rotate nulls_data} each neg til {x-1} count bitmap_data;
parquet_bitmap_nulls:last parquet_bitmap_data;
show bitmap_nulls~bitmap_nulls & sublist[{1-x} count parquet_bitmap_nulls;parquet_bitmap_nulls]
rm parquet_bitmap;

//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Write the schema and array data to an arrow file
arrow_bitmap:"null_bitmap.arrow";
.arrowkdb.ipc.writeArrow[arrow_bitmap;bitmap_schema;bitmap_data;options];
show ls arrow_bitmap

// Read the schema back and compare
arrow_bitmap_schema:.arrowkdb.ipc.readArrowSchema[arrow_bitmap];
show .arrowkdb.sc.equalSchemas[bitmap_schema;arrow_bitmap_schema]
show bitmap_schema~arrow_bitmap_schema

// Read the array data back and compare
arrow_bitmap_data:.arrowkdb.ipc.readArrowData[arrow_bitmap;options];
show bitmap_data~first arrow_bitmap_data
arrow_bitmap_nulls:last arrow_bitmap_data;
show bitmap_nulls~bitmap_nulls & sublist[{1-x} count arrow_bitmap_nulls;arrow_bitmap_nulls]
rm arrow_bitmap;