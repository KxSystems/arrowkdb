// null_mapping_long.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
long_opts:(`uint32`int32`uint64`int64)!(5i;6i;7;8);

options:(``NULL_MAPPING)!((::);long_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

ui32_dt:.arrowkdb.dt.uint32[];
i32_dt:.arrowkdb.dt.int32[];
ui64_dt:.arrowkdb.dt.uint64[];
i64_dt:.arrowkdb.dt.int64[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

ui32_fd:.arrowkdb.fd.field[`uint32;ui32_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
ui64_fd:.arrowkdb.fd.field[`uint64;ui64_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
long_schema:.arrowkdb.sc.schema[(ts_fd,ui32_fd,i32_fd,ui64_fd,i64_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

ui32_data:N?100i;
ui32_data[0]:5i;
i32_data:N?100i;
i32_data[1]:6i;
ui64_data:N?100;
ui64_data[2]:7;
i64_data:N?100;
i64_data[3]:8;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
long_data:(ts_data;ui32_data;i32_data;ui64_data;i64_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
options[`PARQUET_VERSION]:`V2.0

parquet_long:"null_mapping_long.parquet";
.arrowkdb.pq.writeParquet[parquet_long;long_schema;long_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_long_schema:.arrowkdb.pq.readParquetSchema[parquet_long];
.arrowkdb.sc.equalSchemas[long_schema;parquet_long_schema]
long_schema~parquet_long_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_long_data:.arrowkdb.pq.readParquetData[parquet_long;options];
long_data~parquet_long_data
rm parquet_long;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_long:"null_mapping_long.arrow";
.arrowkdb.ipc.writeArrow[arrow_long;long_schema;long_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_long_schema:.arrowkdb.ipc.readArrowSchema[arrow_long];
.arrowkdb.sc.equalSchemas[long_schema;arrow_long_schema]
long_schema~arrow_long_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_long_data:.arrowkdb.ipc.readArrowData[arrow_long;options];
long_data~arrow_long_data
rm arrow_long;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_long:.arrowkdb.ipc.serializeArrow[long_schema;long_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_long_schema:.arrowkdb.ipc.parseArrowSchema[serialized_long];
.arrowkdb.sc.equalSchemas[long_schema;stream_long_schema]
long_schema~stream_long_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_long_data:.arrowkdb.ipc.parseArrowData[serialized_long;options];
long_data~stream_long_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
