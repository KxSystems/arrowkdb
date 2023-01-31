/////////////////////////
// CONSTRUCTED SCHEMAS //
/////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Support null mapping
short_opts:(`bool`uint8`int8`uint16`int16)!(0b;0x01;0x02;3h;4h);
long_opts:(`uint32`int32`uint64`int64)!(5i;6i;7;8);
float_opts:(`float32`float64`decimal)!(1.23e;4.56;7.89);
str_opts:(`string`large_string`binary`large_binary`fixed_binary)!("start";"stop";"x"$"alert";"x"$"acknowledge";0Ng);
time_opts:(`date32`date64`timestamp`time64`duration)!(2006.07.21;2015.01.01D00:00:00.000000000;2011.01.01D00:00:00.000000000;12:00:00.000000000;12:00:00.000000000);
other_opts:(`float16`time32`month_interval`day_time_interval)!(9h;09:01:02.042;2006.07m;12:00:00.000000000);

options:(``NULL_MAPPING)!((::);short_opts,long_opts,float_opts,str_opts,time_opts,other_opts);

ts_dt:.arrowkdb.dt.timestamp[`nano];

// Create the datatype identifiers
bool_dt:.arrowkdb.dt.boolean[];
ui8_dt:.arrowkdb.dt.uint8[];
i8_dt:.arrowkdb.dt.int8[];
ui16_dt:.arrowkdb.dt.uint16[];
i16_dt:.arrowkdb.dt.int16[];

ui32_dt:.arrowkdb.dt.uint32[];
i32_dt:.arrowkdb.dt.int32[];
ui64_dt:.arrowkdb.dt.uint64[];
i64_dt:.arrowkdb.dt.int64[];

f32_dt:.arrowkdb.dt.float32[];
f64_dt:.arrowkdb.dt.float64[];
dec_dt:.arrowkdb.dt.decimal128[38i;2i];

str_dt:.arrowkdb.dt.utf8[];
lstr_dt:.arrowkdb.dt.large_utf8[];
bin_dt:.arrowkdb.dt.binary[];
lbin_dt:.arrowkdb.dt.large_binary[];
fbin_dt:.arrowkdb.dt.fixed_size_binary[16i];

d32_dt:.arrowkdb.dt.date32[];
d64_dt:.arrowkdb.dt.date64[];
tstamp_dt:.arrowkdb.dt.timestamp[`nano];
t64_dt:.arrowkdb.dt.time64[`nano];
dur_dt:.arrowkdb.dt.duration[`milli];

f16_dt:.arrowkdb.dt.float16[];
t32_dt:.arrowkdb.dt.time32[`milli];
mint_dt:.arrowkdb.dt.month_interval[];
dtint_dt:.arrowkdb.dt.day_time_interval[];

// Create the field identifiers
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
ui8_fd:.arrowkdb.fd.field[`uint8;ui8_dt];
i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
ui16_fd:.arrowkdb.fd.field[`uint16;ui16_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];

ui32_fd:.arrowkdb.fd.field[`uint32;ui32_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
ui64_fd:.arrowkdb.fd.field[`uint64;ui64_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

f32_fd:.arrowkdb.fd.field[`float32;f32_dt];
f64_fd:.arrowkdb.fd.field[`float64;f64_dt];
dec_fd:.arrowkdb.fd.field[`decimal;dec_dt];

str_fd:.arrowkdb.fd.field[`string;str_dt];
lstr_fd:.arrowkdb.fd.field[`long_string;lstr_dt];
bin_fd:.arrowkdb.fd.field[`binary;bin_dt];
lbin_fd:.arrowkdb.fd.field[`long_binary;lbin_dt];
fbin_fd:.arrowkdb.fd.field[`fixed_binary;fbin_dt];

d32_fd:.arrowkdb.fd.field[`date32;d32_dt];
d64_fd:.arrowkdb.fd.field[`date64;d64_dt];
tstamp_fd:.arrowkdb.fd.field[`timestamp;tstamp_dt];
t64_fd:.arrowkdb.fd.field[`time64;t64_dt];
dur_fd:.arrowkdb.fd.field[`duration;dur_dt];

f16_fd:.arrowkdb.fd.field[`float16;f16_dt];
t32_fd:.arrowkdb.fd.field[`time32;t32_dt];
mint_fd:.arrowkdb.fd.field[`month_interval;mint_dt];
dtint_fd:.arrowkdb.fd.field[`day_time_interval;dtint_dt];

// Create the schemas for the list of fields
short_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,ui8_fd,i8_fd,ui16_fd,i16_fd)];
long_schema:.arrowkdb.sc.schema[(ts_fd,ui32_fd,i32_fd,ui64_fd,i64_fd)];
float_schema:.arrowkdb.sc.schema[(ts_fd,f32_fd,f64_fd,dec_fd)];
str_schema:.arrowkdb.sc.schema[(ts_fd,str_fd,lstr_fd,bin_fd,lbin_fd,fbin_fd)];
time_schema:.arrowkdb.sc.schema[(ts_fd,d32_fd,d64_fd,tstamp_fd,t64_fd,dur_fd)];
other_schema:.arrowkdb.sc.schema[(ts_fd,f16_fd,t32_fd,mint_fd,dtint_fd)];

// Print the schemas
.arrowkdb.sc.printSchema[short_schema]
.arrowkdb.sc.printSchema[long_schema]
.arrowkdb.sc.printSchema[float_schema]
.arrowkdb.sc.printSchema[str_schema]
.arrowkdb.sc.printSchema[time_schema]
.arrowkdb.sc.printSchema[other_schema]

//-----------------------//
// Create the array data //
//-----------------------//

// Create data for each column in the table
ts_data:asc N?0p;

bool_data:N?(0b;1b);
bool_data[0]:0b;
ui8_data:N?0x64;
ui8_data[1]:0x01;
i8_data:N?0x64;
i8_data[2]:0x02;
ui16_data:N?100h;
ui16_data[3]:3h;
i16_data:N?100h;
i16_data[4]:4h;

ui32_data:N?100i;
ui32_data[0]:5i;
i32_data:N?100i;
i32_data[1]:6i;
ui64_data:N?100;
ui64_data[2]:7;
i64_data:N?100;
i64_data[3]:8;

f32_data:N?100e;
f32_data[0]:1.23e;
f64_data:N?100f;
f64_data[1]:4.56f;
dec_data:N?(10f);
dec_data[2]:7.89f

str_data:N?("start";"stop";"alert";"acknowledge";"");
str_data[0]:"start"
lstr_data:N?("start";"stop";"alert";"acknowledge";"");
lstr_data[1]:"stop"
bin_data:N?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
bin_data[2]:"x"$"alert"
lbin_data:N?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
lbin_data[3]:"x"$"acknowledge"
fbin_data:N?0Ng;
fbin_data[4]:0Ng;

d32_data:N?(2006.07.21;2008.07.18;2012.07.16;2014.07.15;2016.07.11);
d32_data[0]:2006.07.21;
d64_data:N?(2015.01.01D00:00:00.000000000;2017.01.01D00:00:00.000000000;2018.01.01D00:00:00.000000000;2019.01.01D00:00:00.000000000;2020.01.01D00:00:00.000000000);
d64_data[1]:2015.01.01D00:00:00.000000000;
tstamp_data:N?(2015.01.01D00:00:00.000000000;2014.01.01D00:00:00.000000000;2013.01.01D00:00:00.000000000;2012.01.01D00:00:00.000000000;2011.01.01D00:00:00.000000000);
tstamp_data[2]:2011.01.01D00:00:00.000000000;
t64_data:N?(12:00:00.000000000;13:00:00.000000000;14:00:00.000000000;15:00:00.000000000;16:00:00.000000000);
t64_data[3]:12:00:00.000000000;
dur_data:N?(12:00:00.000000000;13:00:00.000000000;14:00:00.000000000;15:00:00.000000000;16:00:00.000000000);
dur_data[4]:12:00:00.000000000;

f16_data:N?100h;
f16_data[0]:9h;
t32_data:N?(09:01:02.042;08:01:02.042;07:01:02.042;06:01:02.042;05:01:02.042);
t32_data[1]:09:01:02.042;
mint_data:N?(2006.07m;2006.06m;2006.05m;2006.04m;2006.03m);
mint_data[2]:2006.07m;
dtint_data:N?(12:00:00.000000000;11:00:00.000000000;10:00:00.000000000;09:00:00.000000000;08:00:00.000000000);
dtint_data[3]:12:00:00.000000000;

// Combine the data for all columns
short_data:(ts_data;bool_data;ui8_data;i8_data;ui16_data;i16_data);
long_data:(ts_data;ui32_data;i32_data;ui64_data;i64_data);
float_data:(ts_data;f32_data;f64_data;dec_data);
str_data:(ts_data;str_data;lstr_data;bin_data;lbin_data;fbin_data);
time_data:(ts_data;d32_data;d64_data;tstamp_data;t64_data;dur_data)
other_data:(ts_data;f16_data;t32_data;mint_data;dtint_data)

// Pretty print the Arrow table populated from the array data
options[`DECIMAL128_AS_DOUBLE]:1
.arrowkdb.tb.prettyPrintTable[short_schema;short_data;options]
.arrowkdb.tb.prettyPrintTable[long_schema;long_data;options]
.arrowkdb.tb.prettyPrintTable[float_schema;float_data;options]
.arrowkdb.tb.prettyPrintTable[str_schema;str_data;options]
.arrowkdb.tb.prettyPrintTable[time_schema;time_data;options]
.arrowkdb.tb.prettyPrintTable[other_schema;other_data;options]

options[`PARQUET_VERSION]:`V2.0
.arrowkdb.pq.writeParquet["null_mapping_short.parquet";short_schema;short_data;options]
.arrowkdb.pq.writeParquet["null_mapping_long.parquet";long_schema;long_data;options]
.arrowkdb.pq.writeParquet["null_mapping_float.parquet";float_schema;float_data;options]
.arrowkdb.pq.writeParquet["null_mapping_str.parquet";str_schema;str_data;options]
.arrowkdb.pq.writeParquet["null_mapping_time.parquet";time_schema;time_data;options]
