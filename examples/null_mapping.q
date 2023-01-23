/////////////////////////
// CONSTRUCTED SCHEMAS //
/////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Support null mapping
short_opts:(`bool`uint8`int8`uint16`int16)!(0b;0x01;0x02;3h;4h)
long_opts:(`uint32`int32`uint64`int64)!(5i;6i;7;8)
float_opts:(`float16`float32`float64)!(9h;1.23e;4.56)
string_opts:(`string`large_string`binary`large_binary`fixed_binary)!("start";"stop";"x"$"alert";"x"$"acknowledge";0Ng)
date_opts:(`date32`date64`timestamp)!("i"$2006.07.21;"j"$2015.01.01D00:00:00.000000000;"j"$12:00:00.000000000)
time_opts:(`time32`time64`decimal`duration)!("i"$09:01:02.042;"j"$2015.01.01D00:00:00.000000000;"f"$7.89;"j"$12:00:00.000000000)
interval_opts:(`month_interval`day_time_interval)!("i"$2006.07m;"j"$12:00:00.000000000)

options:(``NULL_MAPPING)!((::);short_opts,long_opts,float_opts,string_opts,date_opts,time_opts,interval_opts)

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

f64_dt:.arrowkdb.dt.float16[];
f64_dt:.arrowkdb.dt.float64[];
f64_dt:.arrowkdb.dt.float64[];



ts_dt:.arrowkdb.dt.timestamp[`nano];
str_dt:.arrowkdb.dt.utf8[];

// Create the field identifiers
tstamp_fd:.arrowkdb.fd.field[`tstamp;ts_dt];
temp_fd:.arrowkdb.fd.field[`temperature;f64_dt];
fill_fd:.arrowkdb.fd.field[`fill_level;i32_dt];
pump_fd:.arrowkdb.fd.field[`pump_status;bool_dt];
comment_fd:.arrowkdb.fd.field[`comment;str_dt];

// Create the schema for the list of fields
schema:.arrowkdb.sc.schema[(tstamp_fd,temp_fd,fill_fd,pump_fd,comment_fd)];

// Print the schema
.arrowkdb.sc.printSchema[schema]

//-----------------------//
// Create the array data //
//-----------------------//

// Create data for each column in the table
tstamp_data:asc N?0p;
temp_data:N?100f;
fill_data:N?100i;
fill_data[0]:0Ni
pump_data:N?0b;
comment_data:N?("start";"stop";"alert";"acknowledge";"");

// Combine the data for all columns
array_data:(tstamp_data;temp_data;fill_data;pump_data;comment_data);

// Pretty print the Arrow table populated from the array data
.arrowkdb.tb.prettyPrintTable[schema;array_data;options]

options[`PARQUET_VERSION]:`V2.0
.arrowkdb.pq.writeParquet["null_mapping.parquet";schema;array_data;options]
