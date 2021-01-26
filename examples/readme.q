\l ../q/arrowkdb.q


//////////////////////
// INFERRED SCHEMAS //
//////////////////////

//------------------//
// Create a dataset //
//------------------//

// Create table with dummy data
N:5
table:([]tstamp:asc N?0p;temperature:N?100f;fill_level:N?100;pump_status:N?0b;comment:N?("start";"stop";"alert";"acknowledge";""))
table

// Pretty print the Arrow table populated from a kdb+ table
// The schema is inferred from the kdb+ table structure
.arrowkdb.tb.prettyPrintTableFromTable[table]

//---------------//
// Parquet files //
//---------------//

// Use Parquet v2.0
// This is required otherwise the timestamp(ns) datatype will be converted to timestamp(us) resulting in a loss of precision
parquet_write_options:(enlist `PARQUET_VERSION)!(enlist `V2.0)

// Write the table to a parquet file
.arrowkdb.pq.writeParquetFromTable["inferred_schema.parquet";table;parquet_write_options]
show system "ls inferred_schema.parquet"

// Read the parquet file into another table
new_table:.arrowkdb.pq.readParquetToTable["inferred_schema.parquet";::]

// Compare the kdb+ tables
show table~new_table

//-----------------//
// Arrow IPC files //
//-----------------//

// Write the table to an arrow file
.arrowkdb.ipc.writeArrowFromTable["inferred_schema.arrow";table]

// Read the arrow file into another table
new_table:.arrowkdb.ipc.readArrowToTable["inferred_schema.arrow"]

// Compare the kdb+ tables
show table~new_table

//-------------------//
// Arrow IPC streams //
//-------------------//

// Serialize the table to an arrow stream
serialized:.arrowkdb.ipc.serializeArrowFromTable[table]
show serialized

// Parse the arrow stream into another table
new_table:.arrowkdb.ipc.parseArrowToTable[serialized]

// Compare the kdb+ tables
show table~new_table


/////////////////////////
// CONSTRUCTED SCHEMAS //
/////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Create the datatype identifiers
ts_dt:.arrowkdb.dt.timestamp[`nano]
f64_dt:.arrowkdb.dt.float64[]
i64_dt:.arrowkdb.dt.int64[]
bool_dt:.arrowkdb.dt.boolean[]
str_dt:.arrowkdb.dt.utf8[]

// Create the field identifiers
tstamp_fd:.arrowkdb.fd.field[`tstamp;ts_dt]
temp_fd:.arrowkdb.fd.field[`temperature;f64_dt]
fill_fd:.arrowkdb.fd.field[`fill_level;i64_dt]
pump_fd:.arrowkdb.fd.field[`pump_status;bool_dt]
comment_fd:.arrowkdb.fd.field[`comment;str_dt]

// Create the schema for the list of fields
schema:.arrowkdb.sc.schema[(tstamp_fd,temp_fd,fill_fd,pump_fd,comment_fd)]

// Print the schema
.arrowkdb.sc.printSchema[schema]

//-----------------------//
// Create the array data //
//-----------------------//

// Create data for each column in the table
tstamp_data:asc N?0p
temp_data:N?100f
fill_data:N?100
pump_data:N?0b
comment_data:N?("start";"stop";"alert";"acknowledge";"")

// Combine the data for all columns
array_data:(tstamp_data;temp_data;fill_data;pump_data;comment_data)

// Pretty print the Arrow table populated from the array data
.arrowkdb.tb.prettyPrintTable[schema;array_data]

//---------------//
// Parquet files //
//---------------//

// Use Parquet v2.0
// This is required otherwise the timestamp(ns) datatype will be converted to timestamp(us) resulting in a loss of precision
parquet_write_options:(enlist `PARQUET_VERSION)!(enlist `V2.0)

// Write the schema and array data to a parquet file
.arrowkdb.pq.writeParquet["constructed_schema.parquet";schema;array_data;parquet_write_options]
show system "ls constructed_schema.parquet"

// Read the schema back from the parquet file
new_schema:.arrowkdb.pq.readParquetSchema["constructed_schema.parquet"]

// Compare the schemas
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back from the parquet file
new_array_data:.arrowkdb.pq.readParquetData["constructed_schema.parquet";::]

// Compare the array data
show array_data~new_array_data

//-----------------//
// Arrow IPC files //
//-----------------//

// Write the schema and array data to an arrow file
.arrowkdb.ipc.writeArrow["constructed_schema.arrow";schema;array_data]
show system "ls constructed_schema.arrow"

// Read the schema back from the arrow file
new_schema:.arrowkdb.ipc.readArrowSchema["constructed_schema.arrow"]

// Compare the schemas
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back from the arrow file
new_array_data:.arrowkdb.ipc.readArrowData["constructed_schema.arrow"]

// Compare the array data
show array_data~new_array_data

//-------------------//
// Arrow IPC streams //
//-------------------//

// Serialize the schema and array data to an arrow stream
serialized:.arrowkdb.ipc.serializeArrow[schema;array_data]
show serialized

// Parse the schema back for the arrow stream
new_schema:.arrowkdb.ipc.parseArrowSchema[serialized]

// Compare the schemas
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back from the arrow file
new_array_data:.arrowkdb.ipc.parseArrowData[serialized]

// Compare the array data
show array_data~new_array_data


//////////////////////////////
// CONSTRUCTED SCHEMAS WITH //
// NESTED DATATYPES         //
//////////////////////////////

//-------------------//
// Create the schema //
//-------------------//

// Taken from previous example:
ts_dt:.arrowkdb.dt.timestamp[`nano]
f64_dt:.arrowkdb.dt.float64[]
i64_dt:.arrowkdb.dt.int64[]
bool_dt:.arrowkdb.dt.boolean[]
str_dt:.arrowkdb.dt.utf8[]
tstamp_fd:.arrowkdb.fd.field[`tstamp;ts_dt]
temp_fd:.arrowkdb.fd.field[`temperature;f64_dt]
fill_fd:.arrowkdb.fd.field[`fill_level;i64_dt]
pump_fd:.arrowkdb.fd.field[`pump_status;bool_dt]

// Create a struct datatype which bundles the temperature and fill level fields
struct_dt:.arrowkdb.dt.struct[(temp_fd,fill_fd)]

// Create a list datatype which repeats the utf8 datatype
list_dt:.arrowkdb.dt.list[str_dt]

// Create the struct and list field identifiers
sensors_fd:.arrowkdb.fd.field[`sensors_data;struct_dt]
multi_comments_fd:.arrowkdb.fd.field[`multi_comments;list_dt]

// Create the nested schema
nested_schema:.arrowkdb.sc.schema[(tstamp_fd,sensors_fd,pump_fd,multi_comments_fd)]

// Print the schema
.arrowkdb.sc.printSchema[nested_schema]

//-----------------------//
// Create the array data //
//-----------------------//

// Taken from previous example:
tstamp_data:asc N?0p
temp_data:N?100f
fill_data:N?100
pump_data:N?0b

// The sensors struct array data is composed from its child arrays
sensors_data:(temp_data;fill_data);

// Generate the multi-comments array data as lists of strings
getCommentsSet:{
    []
    n:(1?5)[0]+1;
    enlist (n?("start";"stop";"alert";"acknowledge"; ""))
    }
multi_comments_data:getCommentsSet[]
x:N
while[x-:1;multi_comments_data:multi_comments_data,getCommentsSet[]]

// Combine the arrays data for all columns, including the struct and list data
nested_array_data:(tstamp_data;sensors_data;pump_data;multi_comments_data)

// Pretty print the Arrow table populated from the array data
.arrowkdb.tb.prettyPrintTable[nested_schema;nested_array_data]
