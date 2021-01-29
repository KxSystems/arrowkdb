\l ../q/arrowkdb.q

parquet_write_options:(enlist `PARQUET_VERSION)!(enlist `V2.0)
parquet_read_options:(enlist `PARQUET_MULTITHREADED_READ)!(enlist 1j)

int64_field:.arrowkdb.fd.field[`f1;.arrowkdb.dt.int64[]]
//table_schema:.arrowkdb.sc.schema[enlist int64_field]
int64_data_100m:100000000?1000j

timestamp_field:.arrowkdb.fd.field[`f2;.arrowkdb.dt.date64[]]
/table_schema:.arrowkdb.sc.schema[enlist timestamp_field]
timestamp_data_100m:`timestamp$(1000*100000000?1000j)

float_field:.arrowkdb.fd.field[`f3;.arrowkdb.dt.float64[]]
//table_schema:.arrowkdb.sc.schema[enlist float_field]
float_data_100m:`float$100000000?1000j

table_schema:.arrowkdb.sc.schema[(int64_field,timestamp_field,float_field)]
table_data:(int64_data_100m;timestamp_data_100m;float_data_100m)

total_iterations:1

-1"\nwriteParquet:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;.arrowkdb.pq.writeParquet["file.parquet"; table_schema;table_data;parquet_write_options]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

total_iterations:1

-1"\nreadParquetData:";
iteration:total_iterations+1
start:.z.p
read_table:.arrowkdb.pq.readParquetData["file.parquet";parquet_read_options]
/while[iteration-:1;read_table:.arrowkdb.pq.readParquetData["file.parquet";parquet_read_options]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_table~table_data

//getMemoryPoolStats[]

//exit 1

total_iterations:1

-1"\nwriteArrow:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;.arrowkdb.ipc.writeArrow["file.arrow"; table_schema; table_data]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

-1"\nreadArrowData:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;read_table:.arrowkdb.ipc.readArrowData["file.arrow"]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_table~table_data

-1"\nserializeArrow:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;buffer:.arrowkdb.ipc.serializeArrow[table_schema; table_data]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

-1"\nparseArrowData:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;read_table:.arrowkdb.ipc.parseArrowData[buffer]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_table~table_data
