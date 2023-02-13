// null_mapping_float.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
float_opts:(`float32`float64`decimal)!(1.23e;4.56;7.89);

options:(``NULL_MAPPING)!((::);float_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

f32_dt:.arrowkdb.dt.float32[];
f64_dt:.arrowkdb.dt.float64[];
dec_dt:.arrowkdb.dt.decimal128[38i;2i];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

f32_fd:.arrowkdb.fd.field[`float32;f32_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];
dec_fd:.arrowkdb.fd.field[`decimal;dec_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
float_schema:.arrowkdb.sc.schema[(ts_fd,f32_fd,f64_fd,dec_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

f32_data:N?100e;
f32_data[0]:1.23e;
f64_data:N?100f;
f64_data[1]:4.56f;
dec_data:{"F"$.Q.f[2]x} each N?(10f)
dec_data[2]:7.89f

-1"\n+----------|| Combine the data for all columns ||----------+\n";
float_data:(ts_data;f32_data;f64_data;dec_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
options[`DECIMAL128_AS_DOUBLE]:1
options[`PARQUET_VERSION]:`V2.0

parquet_float:"null_mapping_float.parquet";
.arrowkdb.pq.writeParquet[parquet_float;float_schema;float_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_float_schema:.arrowkdb.pq.readParquetSchema[parquet_float];
.arrowkdb.sc.equalSchemas[float_schema;parquet_float_schema]
float_schema~parquet_float_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_float_data:.arrowkdb.pq.readParquetData[parquet_float;options];
float_data~parquet_float_data
rm parquet_float;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_float:"null_mapping_float.arrow";
.arrowkdb.ipc.writeArrow[arrow_float;float_schema;float_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_float_schema:.arrowkdb.ipc.readArrowSchema[arrow_float];
.arrowkdb.sc.equalSchemas[float_schema;arrow_float_schema]
float_schema~arrow_float_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_float_data:.arrowkdb.ipc.readArrowData[arrow_float;options];
float_data~arrow_float_data
rm arrow_float;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_float:.arrowkdb.ipc.serializeArrow[float_schema;float_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_float_schema:.arrowkdb.ipc.parseArrowSchema[serialized_float];
.arrowkdb.sc.equalSchemas[float_schema;stream_float_schema]
float_schema~stream_float_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_float_data:.arrowkdb.ipc.parseArrowData[serialized_float;options];
float_data~stream_float_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
