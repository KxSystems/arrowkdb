// null_mapping_str.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
str_opts:(`string`binary`fixed_binary)!("start";"x"$"alert";0Ng);

options:(``NULL_MAPPING)!((::);str_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

str_dt:.arrowkdb.dt.utf8[];
bin_dt:.arrowkdb.dt.binary[];
fbin_dt:.arrowkdb.dt.fixed_size_binary[16i];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

str_fd:.arrowkdb.fd.field[`string;str_dt];
bin_fd:.arrowkdb.fd.field[`binary;bin_dt];
fbin_fd:.arrowkdb.fd.field[`fixed_binary;fbin_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
str_schema:.arrowkdb.sc.schema[(ts_fd,str_fd,bin_fd,fbin_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

str_data:N?("start";"stop";"alert";"acknowledge";"");
str_data[0]:"start"
bin_data:N?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
bin_data[2]:"x"$"alert"
fbin_data:N?0Ng;
fbin_data[4]:0Ng;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
str_data:(ts_data;str_data;bin_data;fbin_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
options[`PARQUET_VERSION]:`V2.0

parquet_str:"null_mapping_str.parquet";
.arrowkdb.pq.writeParquet[parquet_str;str_schema;str_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_str_schema:.arrowkdb.pq.readParquetSchema[parquet_str];
.arrowkdb.sc.equalSchemas[str_schema;parquet_str_schema]
str_schema~parquet_str_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_str_data:.arrowkdb.pq.readParquetData[parquet_str;options];
parquet_str_data[3]:{0x0 sv x} each parquet_str_data[3] // Convert to GUIDs
str_data~parquet_str_data
rm parquet_str;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_str:"null_mapping_str.arrow";
.arrowkdb.ipc.writeArrow[arrow_str;str_schema;str_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_str_schema:.arrowkdb.ipc.readArrowSchema[arrow_str];
.arrowkdb.sc.equalSchemas[str_schema;arrow_str_schema]
str_schema~arrow_str_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_str_data:.arrowkdb.ipc.readArrowData[arrow_str;options];
arrow_str_data[3]:{0x0 sv x} each arrow_str_data[3] // Convert to GUIDs
str_data~arrow_str_data
rm arrow_str;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_str:.arrowkdb.ipc.serializeArrow[str_schema;str_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_str_schema:.arrowkdb.ipc.parseArrowSchema[serialized_str];
.arrowkdb.sc.equalSchemas[str_schema;stream_str_schema]
str_schema~stream_str_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_str_data:.arrowkdb.ipc.parseArrowData[serialized_str;options];
stream_str_data[3]:{0x0 sv x} each stream_str_data[3] // Convert to GUIDs
str_data~stream_str_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
