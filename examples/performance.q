\l ../q/arrowkdb.q

setParquetMultithreadedRead[1b]

int64_field:field[`f1;int64[]]
table_schema:schema[enlist int64_field]
table_data_1m:mixed[100000000?10j]

timestamp_field:field[`f2;timestamp[`MICRO]]
/table_schema:schema[enlist timestamp_field]
/table_data_1m:mixed[`timestamp$(1000*100000000?10j)]

float_field:field[`f3;float64[]]
table_schema:schema[enlist float_field]
table_data_1m:mixed[`float$100000000?10j]

total_iterations:1

-1"\nwriteParquet:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;writeParquet["file.parquet"; table_schema; table_data_1m]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

total_iterations:1

-1"\nreadParquetData:";
iteration:total_iterations+1
start:.z.p
read_ints:readParquetData["file.parquet"]
/while[iteration-:1;read_ints:readParquetData["file.parquet"]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_ints~table_data_1m

getMemoryPoolStats[]

//exit 1

total_iterations:1

-1"\nwriteReadArray:";
iteration:total_iterations+1
s:readParquetSchema["file.parquet"]

start:.z.p
while[iteration-:1;read_ints:writeReadTable[s;read_ints]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_ints~table_data_1m

//exit 1

total_iterations:1

-1"\nwriteArrow:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;writeArrow["file.arrow"; table_schema; table_data_1m]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

-1"\nreadArrowData:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;read_ints:readArrowData["file.arrow"]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_ints~table_data_1m

-1"\nserializeArrow:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;buffer:serializeArrow[table_schema; table_data_1m]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

-1"\nparseArrowData:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;read_ints:parseArrowData[buffer]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_ints~table_data_1m
