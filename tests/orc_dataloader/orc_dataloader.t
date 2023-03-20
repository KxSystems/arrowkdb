// orc_dataloader.t

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

i8_dt:.arrowkdb.dt.int8[];
i16_dt:.arrowkdb.dt.int16[];
i32_dt:.arrowkdb.dt.int32[];
i64_dt:.arrowkdb.dt.int64[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
dataloader_schema:.arrowkdb.sc.schema[(ts_fd,i8_fd,i16_fd,i32_fd,i64_fd)];

-1"\n+----------|| Number of items in each array ||----------+\n";
N:10

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

i8_data:N?0x64;
i16_data:N?100h;
i32_data:N?100i;
i64_data:N?100;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
dataloader_data:(ts_data;i8_data;i16_data;i32_data;i64_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
orc_options:(``PARQUET_VERSION)!((::);`V2.0);

parquet_dataloader:"orc_dataloader.parquet";
.arrowkdb.pq.writeParquet[parquet_dataloader;dataloader_schema;dataloader_data;orc_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_dataloader_schema:.arrowkdb.pq.readParquetSchema[parquet_dataloader];
.arrowkdb.sc.equalSchemas[dataloader_schema;parquet_dataloader_schema]
dataloader_schema~parquet_dataloader_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_dataloader_data:.arrowkdb.pq.readParquetData[parquet_dataloader;orc_options];
dataloader_data~parquet_dataloader_data
rm parquet_dataloader;

-1"\n+----------|| Write the schema and array data to a ORC file ||----------+\n";
orc_options[`ORC_CHUNK_SIZE]:1024

orc_dataloader:"orc_dataloader.orc"
.arrowkdb.orc.writeOrc[orc_dataloader;dataloader_schema;dataloader_data;orc_options]

-1"\n+----------|| Read the schema back and compare ||----------+\n";
orc_dataloader_schema:.arrowkdb.orc.readOrcSchema[orc_dataloader];
.arrowkdb.sc.equalSchemas[dataloader_schema;orc_dataloader_schema]
dataloader_schema~orc_dataloader_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
orc_dataloader_data:.arrowkdb.orc.readOrcData[orc_dataloader;orc_options];
dataloader_data~orc_dataloader_data
rm orc_dataloader;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_dataloader:"orc_dataloader.arrow";
.arrowkdb.ipc.writeArrow[arrow_dataloader;dataloader_schema;dataloader_data;orc_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_dataloader_schema:.arrowkdb.ipc.readArrowSchema[arrow_dataloader];
.arrowkdb.sc.equalSchemas[dataloader_schema;arrow_dataloader_schema]
dataloader_schema~arrow_dataloader_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_dataloader_data:.arrowkdb.ipc.readArrowData[arrow_dataloader;orc_options];
dataloader_data~arrow_dataloader_data
rm arrow_dataloader;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_dataloader:.arrowkdb.ipc.serializeArrow[dataloader_schema;dataloader_data;orc_options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_dataloader_schema:.arrowkdb.ipc.parseArrowSchema[serialized_dataloader];
.arrowkdb.sc.equalSchemas[dataloader_schema;stream_dataloader_schema]
dataloader_schema~stream_dataloader_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_dataloader_data:.arrowkdb.ipc.parseArrowData[serialized_dataloader;orc_options];
dataloader_data~stream_dataloader_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
