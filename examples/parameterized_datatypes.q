// parameterized_datatypes.q
// Examples of creating a schema with parameterized datatypes and using it to read/write parquet and arrow tables

-1"\n+----------|| parameterized_datatypes.q ||----------+\n";

// import the arrowkdb library
\l ../q/arrowkdb.q


//---------------------//
// Create arrow schema //
//---------------------//

// Create a fixed_size_binary[2] field
fsb_dt:.arrowkdb.dt.fixed_size_binary[2i];
fsb_fd:.arrowkdb.fd.field[`fixed_bin_field;fsb_dt];

// Create a time64[ns] field
time_dt:.arrowkdb.dt.time64[`nano];
time_fd:.arrowkdb.fd.field[`time_field;time_dt];

// Create a decimal128(38,2) field
dec_dt:.arrowkdb.dt.decimal128[38i;2i];
dec_fd:.arrowkdb.fd.field[`decimal_field;dec_dt];

// Create the schema containing these fields
schema:.arrowkdb.sc.schema[(fsb_fd,time_fd,dec_fd)];

// Display the schema
-1"\nSchema:";
.arrowkdb.sc.printSchema[schema]


//-----------------------//
// Create the array data //
//-----------------------//

// Create the data for each field
col1:(0x1111;0x2222;0x3333);
col2:(1D01:01:01.100000000 2D02:02:02.200000000 3D03:03:03.300000000);
col3:(0x00000000000000000000000000000000; 0x01000000000000000000000000000000; 0x00000000000000000000000000000080); // zero, lsb set, msb set

// Combine the array data for all columns
array_data:(col1;col2;col3);

// Show the array data as an arrow table
-1"\nTable:";
.arrowkdb.tb.prettyPrintTable[schema;array_data]


//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
.arrowkdb.pq.writeParquet["parameterized_datatypes.parquet";schema;array_data;::];
show system "ls parameterized_datatypes.parquet"

// Read the schema back and compare
new_schema:.arrowkdb.pq.readParquetSchema["parameterized_datatypes.parquet"];
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back and compare
new_array_data:.arrowkdb.pq.readParquetData["parameterized_datatypes.parquet";::];
show array_data~new_array_data


//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Write the schema and array data to an arrow file
.arrowkdb.ipc.writeArrow["parameterized_datatypes.arrow";schema;array_data];
show system "ls parameterized_datatypes.arrow"

// Read the schema back and compare
new_schema:.arrowkdb.ipc.readArrowSchema["parameterized_datatypes.arrow"];
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back and compare
new_array_data:.arrowkdb.ipc.readArrowData["parameterized_datatypes.arrow"];
show array_data~new_array_data


//-----------------------------//
// Example-3. Arrow IPC stream //
//-----------------------------//

// Serialize the schema and array data to an arrow stream
serialized:.arrowkdb.ipc.serializeArrow[schema;array_data];
show serialized

// Parse the schema back abd compare
new_schema:.arrowkdb.ipc.parseArrowSchema[serialized];
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Parse the array data back and compare
new_array_data:.arrowkdb.ipc.parseArrowData[serialized];
show array_data~new_array_data


-1 "\n+----------------------------------------+\n";

// Process off
exit 0;