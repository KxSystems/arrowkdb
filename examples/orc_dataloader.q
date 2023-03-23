// orc_dataloader.q
// Examples of read/write ORC file

-1"\n+----------|| orc_dataloader.q ||----------+\n";

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

// Create the datatype identifiers
ts_dt:.arrowkdb.dt.timestamp[`nano];

i8_dt:.arrowkdb.dt.int8[];
i16_dt:.arrowkdb.dt.int16[];
i32_dt:.arrowkdb.dt.int32[];
i64_dt:.arrowkdb.dt.int64[];

// Create the field identifiers
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

// Create the schemas for the list of fields
dataloader_schema:.arrowkdb.sc.schema[(ts_fd,i8_fd,i16_fd,i32_fd,i64_fd)];

// Print the schemas
.arrowkdb.sc.printSchema[dataloader_schema];

// Number of items in each array
N:10

// Create data for each column in the table
ts_data:asc N?0p;

i8_data:N?0x64;
i16_data:N?100h;
i32_data:N?100i;
i64_data:N?100;

// Combine the data for all columns
dataloader_data:(ts_data;i8_data;i16_data;i32_data;i64_data);

// Pretty print the Arrow table populated from the array data
.arrowkdb.tb.prettyPrintTable[dataloader_schema;dataloader_data;::];

//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
orc_options:(``PARQUET_VERSION)!((::);`V2.0);

parquet_dataloader:"orc_dataloader.parquet";
.arrowkdb.pq.writeParquet[parquet_dataloader;dataloader_schema;dataloader_data;orc_options];
show ls parquet_dataloader

// Read the schema back and compare
parquet_dataloader_schema:.arrowkdb.pq.readParquetSchema[parquet_dataloader];
show .arrowkdb.sc.equalSchemas[dataloader_schema;parquet_dataloader_schema]
show dataloader_schema~parquet_dataloader_schema

// Read the array data back and compare
parquet_dataloader_data:.arrowkdb.pq.readParquetData[parquet_dataloader;orc_options];
show dataloader_data~parquet_dataloader_data
rm parquet_dataloader;

//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Write the schema and array data to an arrow file
arrow_dataloader:"orc_dataloader.arrow";
.arrowkdb.ipc.writeArrow[arrow_dataloader;dataloader_schema;dataloader_data;orc_options];
show ls arrow_dataloader

// Read the schema back and compare
arrow_dataloader_schema:.arrowkdb.ipc.readArrowSchema[arrow_dataloader];
show .arrowkdb.sc.equalSchemas[dataloader_schema;arrow_dataloader_schema]
show dataloader_schema~arrow_dataloader_schema

// Read the array data back and compare
arrow_dataloader_data:.arrowkdb.ipc.readArrowData[arrow_dataloader;orc_options];
show dataloader_data~arrow_dataloader_data
rm arrow_dataloader;

//-----------------------------//
// Example-3. Arrow IPC stream //
//-----------------------------//

// Serialize the schema and array data to an arrow stream
serialized_dataloader:.arrowkdb.ipc.serializeArrow[dataloader_schema;dataloader_data;orc_options];
show serialized_dataloader

// Parse the schema back abd compare
stream_dataloader_schema:.arrowkdb.ipc.parseArrowSchema[serialized_dataloader];
show .arrowkdb.sc.equalSchemas[dataloader_schema;stream_dataloader_schema]
show dataloader_schema~stream_dataloader_schema

// Parse the array data back and compare
stream_dataloader_data:.arrowkdb.ipc.parseArrowData[serialized_dataloader;orc_options];
show dataloader_data~stream_dataloader_data

//---------------------------//
// Example-4. Apache ORC file//
//---------------------------//

// Write the schema and array data to a ORC file
orc_options[`ORC_CHUNK_SIZE]:1024

orc_dataloader:"orc_dataloader.orc"
.arrowkdb.orc.writeOrc[orc_dataloader;dataloader_schema;dataloader_data;orc_options]
show orc_dataloader;

// Read the schema back and compare
orc_dataloader_schema:.arrowkdb.orc.readOrcSchema[orc_dataloader];
show .arrowkdb.sc.equalSchemas[dataloader_schema;orc_dataloader_schema]
show dataloader_schema~orc_dataloader_schema

// Read the array data back and compare
orc_dataloader_data:.arrowkdb.orc.readOrcData[orc_dataloader;orc_options];
show dataloader_data~orc_dataloader_data
rm orc_dataloader;


-1 "\n+----------------------------------------+\n";

// Process off
exit 0;
