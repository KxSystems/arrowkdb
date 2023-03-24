// orc_null_support.q
// Examples of creating a schema supporting null mapping and using it to read/write
// Apache ORC file with exposing null bitmap as a separate structure to kdb 

-1"\n+----------|| orc_null_support.q ||----------+\n";

// import the arrowkdb library
\l q/arrowkdb.q

// Filesystem functions for Linux/MacOS/Windows
ls:{[filename] $[.z.o like "w*";system "dir /b ",filename;system "ls ",filename]};
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

/////////////////////////
// CONSTRUCTED SCHEMAS //
/////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Support null mapping in Apache ORC
int_opts:(`bool`int8`int16`int32`int64)!(1b;0x02;3h;4i;5);
float_opts:(`float32`float64`decimal)!(9.87e;6.54;3.21f);
cont_opts:(`utf8`binary)!("start";"x"$"alert");
time_opts:(`date32`timestamp)!(2012.11.10;2011.01.01D00:00:00.000000000);

compound_options:(``NULL_MAPPING)!((::);int_opts,float_opts,cont_opts,time_opts);

// Create the datatype identifiers
ts_dt:.arrowkdb.dt.timestamp[`nano];

i8_dt:.arrowkdb.dt.int8[];
i16_dt:.arrowkdb.dt.int16[];
i32_dt:.arrowkdb.dt.int32[];
i64_dt:.arrowkdb.dt.int64[];
f64_dt:.arrowkdb.dt.float64[];

str_dt:.arrowkdb.dt.utf8[];
bin_dt:.arrowkdb.dt.binary[];
dec_dt:.arrowkdb.dt.decimal128[38i;2i];

bool_dt:.arrowkdb.dt.boolean[];
f32_dt:.arrowkdb.dt.float32[];
d32_dt:.arrowkdb.dt.date32[];

// Create the field identifiers
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];

str_fd:.arrowkdb.fd.field[`string;str_dt];
bin_fd:.arrowkdb.fd.field[`binary;bin_dt];
dec_fd:.arrowkdb.fd.field[`decimal;dec_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
f32_fd:.arrowkdb.fd.field[`float32;f32_dt];
d32_fd:.arrowkdb.fd.field[`date32;d32_dt];

numeric_schema:.arrowkdb.sc.schema[(ts_fd, i16_fd, i32_fd, i64_fd, f64_fd)];
contiguous_schema:.arrowkdb.sc.schema[(str_fd, bin_fd, dec_fd)];

// Create a field containing the list datatype
list_dt:.arrowkdb.dt.list[i8_fd];
list_fd:.arrowkdb.fd.field[`list_field;list_dt];

// Create a field containing the struct datatype
struct_dt:.arrowkdb.dt.struct[(bool_fd,f32_fd,d32_fd)];
struct_fd:.arrowkdb.fd.field[`struct_field;struct_dt];

// Create fields containing the map datatype
map_dt:.arrowkdb.dt.map[i64_dt;f64_dt]
map_fd:.arrowkdb.fd.field[`map;map_dt];

// Create the schema containing the list and struct fields
compound_schema:.arrowkdb.sc.schema[(list_fd,struct_fd,map_fd)];

// Print the schema
-1"\nNumeric schema:";
.arrowkdb.sc.printSchema[numeric_schema];

-1"\nContiguous schema:";
.arrowkdb.sc.printSchema[contiguous_schema];

-1"\nCompound schema:";
.arrowkdb.sc.printSchema[compound_schema];

// Number of items in each array
N:5

// Create data for each column in the table
ts_data:asc N?0p;

i16_data:N?100h;
i16_data[0]:3h;
i32_data:N?100i;
i32_data[1]:4i;
i64_data:N?100;
i64_data[2]:5;
f64_data:N?100f;
f64_data[3]:6.54f;

str_data:N?("start";"stop";"alert";"acknowledge";"");
str_data[0]:"start"
bin_data:N?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
bin_data[1]:"x"$"alert"
dec_data:{"F"$.Q.f[2]x} each N?(10f)
dec_data[2]:3.21f

N:3
bool_data:N?(0b;1b);
bool_data[0]:1b;
f32_data:N?100e;
f32_data[1]:9.87e;
d32_data:N?(2012.11.10;2010.07.18;2011.07.16;2014.07.15;2016.07.11);
d32_data[2]:2012.11.10;

// Combine the data for numeric columns
numeric_data:(ts_data;i16_data;i32_data;i64_data;f64_data);
// Combine the data for contiguous columns
contiguous_data:(str_data;bin_data;dec_data);

// Combine the array data for the list and struct columns
list_array:(enlist 0x00;(0x0102);(0x030405));
struct_array:(bool_data;f32_data;d32_data);
map_array:((enlist 1)!(enlist 1.23);(2 2)!(4.56 7.89);(3 3 3)!(9.87 6.54 3.21))
compound_data:(list_array;struct_array;map_array);

// Pretty print the Arrow table populated from the numeric data
compound_options[`DECIMAL128_AS_DOUBLE]:1 

-1"\nNumeric table:";
.arrowkdb.tb.prettyPrintTable[numeric_schema;numeric_data;compound_options];

// Show the string data as an arrow table
-1"\nContiguous table:";
.arrowkdb.tb.prettyPrintTable[contiguous_schema;contiguous_data;compound_options]

// Show the list data as an arrow table
-1"\nCompound table:";
.arrowkdb.tb.prettyPrintTable[compound_schema;compound_data;compound_options]

//-------------------------//
// Example-1. Arrow IPC file //
//-------------------------//

// Write the schema and array data to a arrow file
arrow_numeric:"numeric_bitmap.arrow";
arrow_contiguous:"contiguous_bitmap.arrow";
arrow_compound:"compound_bitmap.arrow";

.arrowkdb.ipc.writeArrow[arrow_numeric;numeric_schema;numeric_data;compound_options];
.arrowkdb.ipc.writeArrow[arrow_contiguous;contiguous_schema;contiguous_data;compound_options];
.arrowkdb.ipc.writeArrow[arrow_compound;compound_schema;compound_data;compound_options];

show ls arrow_numeric
show ls arrow_contiguous
show ls arrow_compound

// Read the schema back and compare
compound_options[`WITH_NULL_BITMAP]:1;

arrow_numeric_schema:.arrowkdb.ipc.readArrowSchema[arrow_numeric];
arrow_contiguous_schema:.arrowkdb.ipc.readArrowSchema[arrow_contiguous];
arrow_compound_schema:.arrowkdb.ipc.readArrowSchema[arrow_compound];

show .arrowkdb.sc.equalSchemas[numeric_schema;arrow_numeric_schema]
show .arrowkdb.sc.equalSchemas[contiguous_schema;arrow_contiguous_schema]
show .arrowkdb.sc.equalSchemas[compound_schema;arrow_compound_schema]

show numeric_schema~arrow_numeric_schema
show contiguous_schema~arrow_contiguous_schema
show compound_schema~arrow_compound_schema

// Read the array data back and compare
arrow_numeric_data:.arrowkdb.ipc.readArrowData[arrow_numeric;compound_options];
arrow_contiguous_data:.arrowkdb.ipc.readArrowData[arrow_contiguous;compound_options];
arrow_compound_data:.arrowkdb.ipc.readArrowData[arrow_compound;compound_options];

show numeric_data~first arrow_numeric_data
show contiguous_data~first arrow_contiguous_data
show compound_data~first arrow_compound_data

// Compare null bitmaps of arrow data
numeric_nulls:(00000b;10000b;01000b;00100b;00010b);
contiguous_nulls:(10000b;01000b;00100b);
list_nulls:(enlist 0b;01b;000b);
struct_nulls:(100b;010b;001b);
map_nulls:((enlist 0b)!(enlist 0b);00b!00b;000b!010b)

arrow_numeric_nulls:last arrow_numeric_data;
arrow_contiguous_nulls:last arrow_contiguous_data;
arrow_list_nulls:last[arrow_compound_data][0]
arrow_struct_nulls:last[arrow_compound_data][1]
arrow_map_nulls:last[arrow_compound_data][2]

show numeric_nulls~numeric_nulls & arrow_numeric_nulls
show contiguous_nulls~contiguous_nulls & arrow_contiguous_nulls
show list_nulls~arrow_list_nulls
show struct_nulls~struct_nulls & arrow_struct_nulls
show map_nulls~arrow_map_nulls

rm arrow_numeric;
rm arrow_contiguous;
rm arrow_compound;

//---------------------------//
// Example-2. Apache ORC file//
//---------------------------//

// Write the schema and array data to a ORC file
compound_options[`ORC_CHUNK_SIZE]:1024

orc_numeric:"numeric_bitmap.orc";
orc_contiguous:"contiguous_bitmap.orc";
orc_compound:"compound_bitmap.orc";

.arrowkdb.orc.writeOrc[orc_numeric;numeric_schema;numeric_data;compound_options]
.arrowkdb.orc.writeOrc[orc_contiguous;contiguous_schema;contiguous_data;compound_options]
.arrowkdb.orc.writeOrc[orc_compound;compound_schema;compound_data;compound_options]

show ls orc_numeric
show ls orc_contiguous
show ls orc_compound

// Read the schema back and compare
orc_numeric_schema:.arrowkdb.orc.readOrcSchema[orc_numeric];
orc_contiguous_schema:.arrowkdb.orc.readOrcSchema[orc_contiguous];
orc_compound_schema:.arrowkdb.orc.readOrcSchema[orc_compound];

show .arrowkdb.sc.equalSchemas[numeric_schema;orc_numeric_schema]
show .arrowkdb.sc.equalSchemas[contiguous_schema;orc_contiguous_schema]
show .arrowkdb.sc.equalSchemas[compound_schema;orc_compound_schema]

show numeric_schema~orc_numeric_schema
show contiguous_schema~orc_contiguous_schema
show compound_schema~orc_compound_schema

// Read the array data back and compare
orc_numeric_data:.arrowkdb.orc.readOrcData[orc_numeric;compound_options];
orc_contiguous_data:.arrowkdb.orc.readOrcData[orc_contiguous;compound_options];
orc_compound_data:.arrowkdb.orc.readOrcData[orc_compound;compound_options];

show numeric_data~first orc_numeric_data
show contiguous_data~first orc_contiguous_data
show compound_data~first orc_compound_data

// Compare null bitmaps of arrow data
orc_numeric_nulls:last orc_numeric_data;
orc_contiguous_nulls:last orc_contiguous_data;
orc_list_nulls:last[orc_compound_data][0]
orc_struct_nulls:last[orc_compound_data][1]
orc_map_nulls:last[orc_compound_data][2]

show numeric_nulls~numeric_nulls & orc_numeric_nulls
show contiguous_nulls~contiguous_nulls & orc_contiguous_nulls
show list_nulls~orc_list_nulls
show struct_nulls~struct_nulls & orc_struct_nulls
show map_nulls~orc_map_nulls

rm orc_numeric;
rm orc_contiguous;
rm orc_compound;


-1 "\n+----------------------------------------+\n";

// Process off
exit 0;
