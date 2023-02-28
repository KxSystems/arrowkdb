// crucial_null_bitmap.t

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
crucial_opts:(`bool`int32`float64`string`date32)!(0b;1i;2.34;"start";2006.07.21);

crucial_options:(``NULL_MAPPING)!((::);crucial_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

bool_dt:.arrowkdb.dt.boolean[];
i32_dt:.arrowkdb.dt.int32[];
f64_dt:.arrowkdb.dt.float64[];
str_dt:.arrowkdb.dt.utf8[];
d32_dt:.arrowkdb.dt.date32[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];
str_fd:.arrowkdb.fd.field[`string;str_dt];
d32_fd:.arrowkdb.fd.field[`date32;d32_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
crucial_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,i32_fd,f64_fd,str_fd,d32_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
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

-1"\n+----------|| Combine the data for all columns ||----------+\n";
crucial_data:(ts_data;bool_data;i32_data;f64_data;str_data;d32_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
crucial_options[`PARQUET_VERSION]:`V2.0;

parquet_crucial_bitmap:"null_bitmap.parquet";
.arrowkdb.pq.writeParquet[parquet_crucial_bitmap;crucial_schema;crucial_data;crucial_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_crucial_schema:.arrowkdb.pq.readParquetSchema[parquet_crucial_bitmap];
.arrowkdb.sc.equalSchemas[crucial_schema;parquet_crucial_schema]
crucial_schema~parquet_crucial_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
crucial_options[`WITH_NULL_BITMAP]:1;
parquet_crucial_data:.arrowkdb.pq.readParquetData[parquet_crucial_bitmap;crucial_options];
crucial_data~first parquet_crucial_data

nulls_data:1b,(N-1)?1b;
crucial_nulls:{x rotate nulls_data} each neg til {x-1} count crucial_data;
parquet_crucial_nulls:last parquet_crucial_data;
crucial_nulls~crucial_nulls & sublist[{1-x} count parquet_crucial_nulls;parquet_crucial_nulls]
rm parquet_crucial_bitmap;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_crucial_bitmap:"null_bitmap.arrow";
.arrowkdb.ipc.writeArrow[arrow_crucial_bitmap;crucial_schema;crucial_data;crucial_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_crucial_schema:.arrowkdb.ipc.readArrowSchema[arrow_crucial_bitmap];
.arrowkdb.sc.equalSchemas[crucial_schema;arrow_crucial_schema]
crucial_schema~arrow_crucial_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_crucial_data:.arrowkdb.ipc.readArrowData[arrow_crucial_bitmap;crucial_options];
crucial_data~first arrow_crucial_data
arrow_crucial_nulls:last arrow_crucial_data;
crucial_nulls~crucial_nulls & sublist[{1-x} count arrow_crucial_nulls;arrow_crucial_nulls]
rm arrow_crucial_bitmap;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_bitmap:.arrowkdb.ipc.serializeArrow[crucial_schema;crucial_data;crucial_options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_crucial_schema:.arrowkdb.ipc.parseArrowSchema[serialized_bitmap];
.arrowkdb.sc.equalSchemas[crucial_schema;stream_crucial_schema]
crucial_schema~stream_crucial_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_crucial_data:.arrowkdb.ipc.parseArrowData[serialized_bitmap;crucial_options];
crucial_data~first stream_crucial_data

stream_crucial_nulls:last stream_crucial_data;
crucial_nulls~crucial_nulls & sublist[{1-x} count stream_crucial_nulls;stream_crucial_nulls]


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
