// null_bitmap.t

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
bitmap_opts:(`bool`int32`float64`string`date32)!(0b;1i;2.34;"start";2006.07.21);

options:(``NULL_MAPPING)!((::);bitmap_opts);

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
bitmap_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,i32_fd,f64_fd,str_fd,d32_fd)];

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
bitmap_data:(ts_data;bool_data;i32_data;f64_data;str_data;d32_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
options[`PARQUET_VERSION]:`V2.0;

parquet_bitmap:"null_bitmap.parquet";
.arrowkdb.pq.writeParquet[parquet_bitmap;bitmap_schema;bitmap_data;options];

-1"\n+----------|| Read the array data back and compare ||----------+\n";
options[`WITH_NULL_BITMAP]:1;
parquet_bitmap_data:.arrowkdb.pq.readParquetData[parquet_bitmap;options];
bitmap_data~first parquet_bitmap_data

nulls_data:1b,(N-1)?1b;
bitmap_nulls:{x rotate nulls_data} each neg til {x-1} count bitmap_data;
parquet_bitmap_nulls:last parquet_bitmap_data;
bitmap_nulls~bitmap_nulls & sublist[{1-x} count parquet_bitmap_nulls;parquet_bitmap_nulls]
rm parquet_bitmap;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_bitmap:"null_bitmap.arrow";
.arrowkdb.ipc.writeArrow[arrow_bitmap;bitmap_schema;bitmap_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_bitmap_schema:.arrowkdb.ipc.readArrowSchema[arrow_bitmap];
.arrowkdb.sc.equalSchemas[bitmap_schema;arrow_bitmap_schema]
bitmap_schema~arrow_bitmap_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_bitmap_data:.arrowkdb.ipc.readArrowData[arrow_bitmap;options];
bitmap_data~first arrow_bitmap_data
arrow_bitmap_nulls:last arrow_bitmap_data;
bitmap_nulls~bitmap_nulls & sublist[{1-x} count arrow_bitmap_nulls;arrow_bitmap_nulls]
rm arrow_bitmap;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_bitmap:.arrowkdb.ipc.serializeArrow[bitmap_schema;bitmap_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_bitmap_schema:.arrowkdb.ipc.parseArrowSchema[serialized_bitmap];
.arrowkdb.sc.equalSchemas[bitmap_schema;stream_bitmap_schema]
bitmap_schema~stream_bitmap_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_bitmap_data:.arrowkdb.ipc.parseArrowData[serialized_bitmap;options];
bitmap_data~first stream_bitmap_data

stream_bitmap_nulls:last stream_bitmap_data;
bitmap_nulls~bitmap_nulls & sublist[{1-x} count stream_bitmap_nulls;stream_bitmap_nulls]


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
