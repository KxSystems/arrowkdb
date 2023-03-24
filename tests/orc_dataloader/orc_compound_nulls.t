// orc_compound_nulls.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping in Apache ORC ||----------+\n";
comp_opts:(`bool`int8`int64`float32`float64`date32)!(1b;0x02;5;9.87e;6.54;2012.11.10);

compound_options:(``NULL_MAPPING)!((::);comp_opts);

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
i8_dt:.arrowkdb.dt.int8[];

bool_dt:.arrowkdb.dt.boolean[];
f32_dt:.arrowkdb.dt.float32[];
d32_dt:.arrowkdb.dt.date32[];

i64_dt:.arrowkdb.dt.int64[];
f64_dt:.arrowkdb.dt.float64[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
i8_fd:.arrowkdb.fd.field[`int8;i8_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
f32_fd:.arrowkdb.fd.field[`float32;f32_dt];
d32_fd:.arrowkdb.fd.field[`date32;d32_dt];

i64_fd:.arrowkdb.fd.field[`int64;i64_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];

-1"\n+----------|| Create a field containing the list datatype ||----------+\n";
list_dt:.arrowkdb.dt.list[i8_fd];
list_fd:.arrowkdb.fd.field[`list_field;list_dt];

-1"\n+----------|| Create a field containing the struct datatype ||----------+\n";
struct_dt:.arrowkdb.dt.struct[(bool_fd,f32_fd,d32_fd)];
struct_fd:.arrowkdb.fd.field[`struct_field;struct_dt];

-1"\n+----------|| Create fields containing the map datatype ||----------+\n";
map_dt:.arrowkdb.dt.map[i64_dt;f64_dt]
map_fd:.arrowkdb.fd.field[`map;map_dt];

-1"\n+----------|| Create the schema containing the list and struct fields ||----------+\n";
compound_schema:.arrowkdb.sc.schema[(list_fd,struct_fd,map_fd)];

-1"\n+----------|| Number of items in each array ||----------+\n";
N:3

bool_data:N?(0b;1b);
bool_data[0]:1b;
f32_data:N?100e;
f32_data[1]:9.87e;
d32_data:N?(2012.11.10;2010.07.18;2011.07.16;2014.07.15;2016.07.11);
d32_data[2]:2012.11.10;

-1"\n+----------|| Combine the array data for the list and struct columns ||----------+\n";
list_array:(enlist 0x00;(0x0102);(0x030405));
struct_array:(bool_data;f32_data;d32_data);
map_array:((enlist 1)!(enlist 1.23);(2 2)!(4.56 7.89);(3 3 3)!(9.87 6.54 3.21))
compound_data:(list_array;struct_array;map_array);

-1"\n+----------|| Write the schema and array data to a ORC file ||----------+\n";
compound_options[`ORC_CHUNK_SIZE]:1024

orc_compound:"compound_bitmap.orc";
.arrowkdb.orc.writeOrc[orc_compound;compound_schema;compound_data;compound_options]

-1"\n+----------|| Read the schema back and compare ||----------+\n";
compound_options[`WITH_NULL_BITMAP]:1;

orc_compound_schema:.arrowkdb.orc.readOrcSchema[orc_compound];
.arrowkdb.sc.equalSchemas[compound_schema;orc_compound_schema]
compound_schema~orc_compound_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
orc_compound_data:.arrowkdb.orc.readOrcData[orc_compound;compound_options];
compound_data~first orc_compound_data

-1"\n+----------|| Compare null bitmaps of arrow data ||----------+\n";
list_nulls:(enlist 0b;01b;000b);
struct_nulls:(100b;010b;001b);
map_nulls:((enlist 0b)!(enlist 0b);00b!00b;000b!010b)

orc_list_nulls:last[orc_compound_data][0]
orc_struct_nulls:last[orc_compound_data][1]
orc_map_nulls:last[orc_compound_data][2]

list_nulls~orc_list_nulls
struct_nulls~struct_nulls & orc_struct_nulls
map_nulls~orc_map_nulls

rm orc_compound;


-1 "\n+----------|| Test utils ||----------+\n";

.arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
