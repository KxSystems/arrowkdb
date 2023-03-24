// orc_numeric_nulls.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping in Apache ORC ||----------+\n";
num_opts:(`int8`int16`int32`int64`float64)!(0x02;3h;4i;5;6.54);

numeric_options:(``NULL_MAPPING)!((::);num_opts);

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

i8_dt:.arrowkdb.dt.int8[];
i16_dt:.arrowkdb.dt.int16[];
i32_dt:.arrowkdb.dt.int32[];
i64_dt:.arrowkdb.dt.int64[];
f64_dt:.arrowkdb.dt.float64[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];

-1"\n+----------|| Create the schema for the list of fields ||----------+\n";
numeric_schema:.arrowkdb.sc.schema[(ts_fd, i16_fd, i32_fd, i64_fd, f64_fd)];

-1"\n+----------|| Number of items in each array ||----------+\n";
N:5

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

i16_data:N?100h;
i16_data[0]:3h;
i32_data:N?100i;
i32_data[1]:4i;
i64_data:N?100;
i64_data[2]:5;
f64_data:N?100f;
f64_data[3]:6.54f;

-1"\n+----------|| Combine the data for numeric columns ||----------+\n";
numeric_data:(ts_data;i16_data;i32_data;i64_data;f64_data);

-1"\n+----------|| Write the schema and array data to a ORC file ||----------+\n";
numeric_options[`ORC_CHUNK_SIZE]:1024

orc_numeric:"numeric_bitmap.orc";
.arrowkdb.orc.writeOrc[orc_numeric;numeric_schema;numeric_data;numeric_options]

-1"\n+----------|| Read the schema back and compare ||----------+\n";
numeric_options[`WITH_NULL_BITMAP]:1;

orc_numeric_schema:.arrowkdb.orc.readOrcSchema[orc_numeric];
.arrowkdb.sc.equalSchemas[numeric_schema;orc_numeric_schema]
numeric_schema~orc_numeric_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
orc_numeric_data:.arrowkdb.orc.readOrcData[orc_numeric;numeric_options];
numeric_data~first orc_numeric_data

-1"\n+----------|| Compare null bitmaps of arrow data ||----------+\n";
numeric_nulls:(00000b;10000b;01000b;00100b;00010b);
orc_numeric_nulls:last orc_numeric_data;
numeric_nulls~numeric_nulls & orc_numeric_nulls

rm orc_numeric;


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
