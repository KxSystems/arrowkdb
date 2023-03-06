// union_null_bitmap.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
nested_union_opts:(`float64`int64)!(2.34;5);
union_options:(``NULL_MAPPING)!((::);nested_union_opts);
N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

f64_dt:.arrowkdb.dt.float64[];
i64_dt:.arrowkdb.dt.int64[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

f64_fd:.arrowkdb.fd.field[`float64;f64_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

-1"\n+----------|| Create fields containing union datatypes ||----------+\n";
sparse_dt:.arrowkdb.dt.sparse_union[(i64_fd,f64_fd)]
sparse_fd:.arrowkdb.fd.field[`sparse_union;sparse_dt]
dense_dt:.arrowkdb.dt.dense_union[(i64_fd,f64_fd)]
dense_fd:.arrowkdb.fd.field[`dense_union;dense_dt]

-1"\n+----------|| Create the schema containing the sparce and dense union fields ||----------+\n";
union_schema:.arrowkdb.sc.schema[(sparse_fd, dense_fd)]

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

f64_data:N?100f;
f64_data[0]:2.34f;
i64_data:N?100h;
i64_data[1]:5h;

-1"\n+----------|| Create the data the union child fields ||----------+\n";
i64_data:N?100;
i64_data[0]:1;

-1"\n+----------|| Combine the array data for the list and struct columns ||----------+\n";
sparse_data:dense_data:(0 1 0h;5 2 3;4 2.34 6f)
union_data:(sparse_data;dense_data)

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
union_options[`WITH_NULL_BITMAP]:1;
arrow_union_bitmap:"nested_union.arrow";
.arrowkdb.ipc.writeArrow[arrow_union_bitmap;union_schema;union_data;union_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_union_schema:.arrowkdb.ipc.readArrowSchema[arrow_union_bitmap];
.arrowkdb.sc.equalSchemas[union_schema;arrow_union_schema]
union_schema~arrow_union_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_union_data:.arrowkdb.ipc.readArrowData[arrow_union_bitmap;union_options];
union_data~first arrow_union_data

-1"\n+----------|| Compare null bitmaps of arrow data ||----------+\n";
nested_union_nulls:((0 1 0h);100b;010b);

arrow_union_nulls:arrow_union_data[1]
nested_union_nulls~arrow_union_nulls[0][0]
nested_union_nulls~arrow_union_nulls[1][0]

rm arrow_union_bitmap;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_nested_union:.arrowkdb.ipc.serializeArrow[union_schema;union_data;union_options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_union_schema:.arrowkdb.ipc.parseArrowSchema[serialized_nested_union];
.arrowkdb.sc.equalSchemas[union_schema;stream_union_schema]
union_schema~stream_union_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_union_data:.arrowkdb.ipc.parseArrowData[serialized_nested_union;union_options];
union_data~first stream_union_data

-1"\n+----------|| Compare null bitmaps of stream data ||----------+\n";
stream_union_nulls:stream_union_data[1]
nested_union_nulls~stream_union_nulls[0][0]
nested_union_nulls~stream_union_nulls[1][0]


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
