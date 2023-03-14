// nested_null_bitmap.t

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
nested_opts:(`uint16`float32`binary`time64)!(9h;8.76e;"x"$"acknowledge";00:00:00.123456789);

nested_options:(``NULL_MAPPING)!((::);nested_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

ui16_dt:.arrowkdb.dt.uint16[];

f32_dt:.arrowkdb.dt.float32[];
bin_dt:.arrowkdb.dt.binary[];
t64_dt:.arrowkdb.dt.time64[`nano];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

ui16_fd:.arrowkdb.fd.field[`uint16;ui16_dt];

f32_fd:.arrowkdb.fd.field[`float32;f32_dt];
bin_fd:.arrowkdb.fd.field[`binary;bin_dt];
t64_fd:.arrowkdb.fd.field[`time64;t64_dt];

-1"\n+----------|| Create a list datatype, using the uint16 datatype as its child ||----------+\n";
list_dt:.arrowkdb.dt.list[ui16_dt];

-1"\n+----------|| Create a field containing the list datatype ||----------+\n";
list_fd:.arrowkdb.fd.field[`list_field;list_dt];

-1"\n+----------|| Create a struct datatype using the float32, binary and time64 fields as its children ||----------+\n";
struct_dt:.arrowkdb.dt.struct[(f32_fd,bin_dt,t64_dt)];

-1"\n+----------|| Create a field containing the struct datatype ||----------+\n";
struct_fd:.arrowkdb.fd.field[`struct_field;struct_dt];

-1"\n+----------|| Create the schema containing the list and struct fields ||----------+\n";
nested_schema:.arrowkdb.sc.schema[(list_fd,struct_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

-1"\n+----------|| Create the data for each of the struct child fields ||----------+\n";
f32_data:5?100e;
f32_data[0]:8.76e;
bin_data:5?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
bin_data[1]:"x"$"acknowledge"
t64_data:5?(12:00:00.000000000;13:00:00.000000000;14:00:00.000000000;15:00:00.000000000;16:00:00.000000000);
t64_data[2]:00:00:00.123456789;

-1"\n+----------|| Create the data for the list array ||----------+\n";
list_data:(enlist 9h;(8h;7h);(6h;5h;4h);(1h;2h;3h;4h);(5h;6h;7h;8h;9h));

-1"\n+----------|| Create the data for the struct array from its child arrays ||----------+\n";
struct_data:(f32_data;bin_data;t64_data);

-1"\n+----------|| Combine the array data for the list and struct columns ||----------+\n";
nested_data:(list_data;struct_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
nested_options[`PARQUET_VERSION]:`V2.0;

parquet_nested_bitmap:"nested_bitmap.parquet";
.arrowkdb.pq.writeParquet[parquet_nested_bitmap;nested_schema;nested_data;nested_options];

-1"\n+----------|| Read the array back and compare ||----------+\n";
nested_options[`WITH_NULL_BITMAP]:1;

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_nested_schema:.arrowkdb.pq.readParquetSchema[parquet_nested_bitmap];
.arrowkdb.sc.equalSchemas[nested_schema;parquet_nested_schema]
nested_schema~parquet_nested_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_nested_data:.arrowkdb.pq.readParquetData[parquet_nested_bitmap;nested_options];
nested_data~first parquet_nested_data

-1"\n+----------|| Compare nested null bitmaps ||----------+\n";
nested_list_nulls:(enlist 1b;00b;000b;0000b;00001b)
nested_struct_nulls:(10000b;01000b;00100b)

parquet_list_nulls:first parquet_nested_data[1]
parquet_struct_nulls:last parquet_nested_data[1]
nested_list_nulls~parquet_list_nulls
nested_struct_nulls~parquet_struct_nulls

rm parquet_nested_bitmap;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_nested_bitmap:"nested_bitmap.arrow";
.arrowkdb.ipc.writeArrow[arrow_nested_bitmap;nested_schema;nested_data;nested_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_nested_schema:.arrowkdb.ipc.readArrowSchema[arrow_nested_bitmap];
.arrowkdb.sc.equalSchemas[nested_schema;arrow_nested_schema]
nested_schema~arrow_nested_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_nested_data:.arrowkdb.ipc.readArrowData[arrow_nested_bitmap;nested_options];
nested_data~first arrow_nested_data

-1"\n+----------|| Compare nested null bitmaps ||----------+\n";
arrow_list_nulls:first arrow_nested_data[1]
arrow_struct_nulls:last arrow_nested_data[1]
nested_list_nulls~arrow_list_nulls
nested_struct_nulls~arrow_struct_nulls

rm arrow_nested_bitmap;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_nested_bitmap:.arrowkdb.ipc.serializeArrow[nested_schema;nested_data;nested_options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_nested_schema:.arrowkdb.ipc.parseArrowSchema[serialized_nested_bitmap];
.arrowkdb.sc.equalSchemas[nested_schema;stream_nested_schema]
nested_schema~stream_nested_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_nested_data:.arrowkdb.ipc.parseArrowData[serialized_nested_bitmap;nested_options];
nested_data~first stream_nested_data

-1"\n+----------|| Compare nested null bitmaps ||----------+\n";
stream_list_nulls:first stream_nested_data[1]
stream_struct_nulls:last stream_nested_data[1]
nested_list_nulls~stream_list_nulls
nested_struct_nulls~stream_struct_nulls


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
