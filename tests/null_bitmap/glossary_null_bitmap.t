// glossary_null_bitmap.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
glossary_opts:(`int64`float64)!(5;2.34);

glossary_options:(``NULL_MAPPING)!((::);glossary_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

i64_dt:.arrowkdb.dt.int64[];
f64_dt:.arrowkdb.dt.float64[];

-1"\n+----------|| Create a map datatype using the i16_dt as the key and dec_dt as its values ||----------+\n";
map_dt:.arrowkdb.dt.map[i64_dt;f64_dt]

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

i64_fd:.arrowkdb.fd.field[`int64;i64_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];

-1"\n+----------|| Create a field containing the map datatype ||----------+\n";
map_fd:.arrowkdb.fd.field[`map;map_dt];

-1"\n+----------|| Create the schema containing the large list, dictionary and sparce union fields ||----------+\n";
glossary_schema:.arrowkdb.sc.schema[(enlist map_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

i64_data:N?100i;
i64_data[0]:1i;
f64_data:N?100f;
f64_data[1]:2.34f;

map_data:((enlist 1)!(enlist 1f);(2 2)!(2 2.34f);(3 3 3)!(3 3 3f))

-1"\n+----------|| Combine the array data for the glossary columns ||----------+\n";
glossary_data:(enlist map_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
glossary_options[`PARQUET_VERSION]:`V2.0;

parquet_glossary_bitmap:"glossary_bitmap.parquet";
.arrowkdb.pq.writeParquet[parquet_glossary_bitmap;glossary_schema;glossary_data;glossary_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
glossary_options[`WITH_NULL_BITMAP]:1;

parquet_glossary_schema:.arrowkdb.pq.readParquetSchema[parquet_glossary_bitmap];
.arrowkdb.sc.equalSchemas[glossary_schema;parquet_glossary_schema]
glossary_schema~parquet_glossary_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_glossary_data:.arrowkdb.pq.readParquetData[parquet_glossary_bitmap;glossary_options];
glossary_data~first parquet_glossary_data

-1"\n+----------|| Compare null bitmaps of parquet data ||----------+\n";
null_data:((enlist 0b)!(enlist 0b);00b!01b;000b!000b)
glossary_nulls:enlist null_data
glossary_nulls~last parquet_glossary_data

rm parquet_glossary_bitmap;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_glossary_bitmap:"nested_map.arrow";
.arrowkdb.ipc.writeArrow[arrow_glossary_bitmap;glossary_schema;glossary_data;glossary_options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_glossary_schema:.arrowkdb.ipc.readArrowSchema[arrow_glossary_bitmap];
.arrowkdb.sc.equalSchemas[glossary_schema;arrow_glossary_schema]
glossary_schema~arrow_glossary_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_glossary_data:.arrowkdb.ipc.readArrowData[arrow_glossary_bitmap;glossary_options];
glossary_data~first arrow_glossary_data

-1"\n+----------|| Compare null bitmaps of arrow data ||----------+\n";
glossary_nulls~last arrow_glossary_data

rm arrow_glossary_bitmap;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_glossary:.arrowkdb.ipc.serializeArrow[glossary_schema;glossary_data;glossary_options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_glossary_schema:.arrowkdb.ipc.parseArrowSchema[serialized_glossary];
.arrowkdb.sc.equalSchemas[glossary_schema;stream_glossary_schema]
glossary_schema~stream_glossary_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_glossary_data:.arrowkdb.ipc.parseArrowData[serialized_glossary;glossary_options];
glossary_data~first stream_glossary_data

-1"\n+----------|| Compare null bitmaps of stream data ||----------+\n";
glossary_nulls~last stream_glossary_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
