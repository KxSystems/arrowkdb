/////////////////////////
// CONSTRUCTED SCHEMAS //
/////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Create the datatype identifiers
ts_dt:.arrowkdb.dt.timestamp[`nano];
f64_dt:.arrowkdb.dt.float64[];
i32_dt:.arrowkdb.dt.int32[];
bool_dt:.arrowkdb.dt.boolean[];
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

// Support null mapping
null_opts:(`int16`int32`string)!(0Nh;0Ni;"start")
options:(``NULL_MAPPING)!((::);null_opts)

// Pretty print the Arrow table populated from the array data
.arrowkdb.tb.prettyPrintTable[schema;array_data;options]

options[`PARQUET_VERSION]:`V2.0
.arrowkdb.pq.writeParquet["null_mapping.parquet";schema;array_data;options]
