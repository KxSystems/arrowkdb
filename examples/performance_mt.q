\l ../q/arrowkdb.q

setParquetMultithreadedRead[1b]

f1:field[`f1;int64[]]
f2:field[`f2;int64[]]
f3:field[`f3;int64[]]
f4:field[`f4;int64[]]
f5:field[`f5;int64[]]
f6:field[`f6;int64[]]
f7:field[`f7;int64[]]
f8:field[`f8;int64[]]
table_schema:schema[(f1,f2,f3,f4,f5,f6,f7,f8)]
table_data_10m_8:(10000000?100j;10000000?100j;10000000?100j;10000000?100j;10000000?100j;10000000?100j;10000000?100j;10000000?100j)

total_iterations:1

-1"\nwriteParquet:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;writeParquet["10m_8.parquet"; table_schema; table_data_10m_8]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

-1"\nreadParquetData:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;read_ints:readParquetData["10m_8.parquet"]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_ints~table_data_10m_8

-1"\nwriteArrow:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;writeArrow["10m_8.arrow"; table_schema; table_data_10m_8]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000

-1"\nreadArrowData:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;read_ints:readArrowData["10m_8.arrow"]]
end:.z.p
delta:end-start

-1"Time per iteration (ms): ";`long$delta % total_iterations * 1000000
read_ints~table_data_10m_8

-1"\nserializeArrow:";
iteration:total_iterations+1
start:.z.p
while[iteration-:1;buffer:serializeArrow[table_schema; table_data_10m_8]]
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
read_ints~table_data_10m_8