// concrete_datatypes.q
// Examples of creating a schema with concrete datatypes and using it to read/write parquet and arrow tables

-1"\n+----------|| concrete_datatypes.q ||----------+\n";

// import the arrowkdb library
\l q/arrowkdb.q

// Filesystem functions for Linux/MacOS/Windows
ls:{[filename] $[.z.o like "w*";system "dir /b ",filename;system "ls ",filename]};
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};


//---------------------//
// Create arrow schema //
//---------------------//

// Create an int64 field
i64_dt:.arrowkdb.dt.int64[];
i64_fd:.arrowkdb.fd.field[`int_field;i64_dt];

// Create a float64 field
f64_dt:.arrowkdb.dt.float64[];
f64_fd:.arrowkdb.fd.field[`float_field;f64_dt];

// Create a utf8 field
utf8_dt:.arrowkdb.dt.utf8[];
utf8_fd:.arrowkdb.fd.field[`str_field;utf8_dt];

// Create a binary field
bin_dt:.arrowkdb.dt.binary[];
bin_fd:.arrowkdb.fd.field[`bin_field;bin_dt];

// Create a date32 field
d32_dt:.arrowkdb.dt.date32[];
d32_fd:.arrowkdb.fd.field[`date_field;d32_dt];

// Create the schema containing these fields
schema:.arrowkdb.sc.schema[(i64_fd,f64_fd,utf8_fd,bin_fd,d32_fd)];

// Display the schema
-1"\nSchema:";
.arrowkdb.sc.printSchema[schema]


//-----------------------//
// Create the array data //
//-----------------------//

// Create the data for each field
col1:(1 2 3j);
col2:(4 5 6f);
col3:(enlist "a";"bb";"ccc");
col4:(enlist 0x11; 0x2222; 0x333333);
col5:(2001.01.01 2002.02.02 2003.03.03);

// Combine the array data for all columns
array_data:(col1;col2;col3;col4;col5);

// Show the array data as an arrow table
-1"\nTable:";
.arrowkdb.tb.prettyPrintTable[schema;array_data;::]


//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
filename:"concrete_datatypes.parquet";
.arrowkdb.pq.writeParquet[filename;schema;array_data;::];
show ls filename

// Read the schema back and compare
new_schema:.arrowkdb.pq.readParquetSchema[filename];
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back and compare
new_array_data:.arrowkdb.pq.readParquetData[filename;::];
show array_data~new_array_data
rm filename;


//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Write the schema and array data to an arrow file
filename:"concrete_datatypes.arrow";
.arrowkdb.ipc.writeArrow[filename;schema;array_data;::];
show ls filename

// Read the schema back and compare
new_schema:.arrowkdb.ipc.readArrowSchema[filename];
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Read the array data back and compare
new_array_data:.arrowkdb.ipc.readArrowData[filename;::];
show array_data~new_array_data
rm filename;


//-----------------------------//
// Example-3. Arrow IPC stream //
//-----------------------------//

// Serialize the schema and array data to an arrow stream
serialized:.arrowkdb.ipc.serializeArrow[schema;array_data;::];
show serialized

// Parse the schema back abd compare
new_schema:.arrowkdb.ipc.parseArrowSchema[serialized];
show .arrowkdb.sc.equalSchemas[schema;new_schema]
show schema~new_schema

// Parse the array data back and compare
new_array_data:.arrowkdb.ipc.parseArrowData[serialized;::];
show array_data~new_array_data


-1 "\n+----------------------------------------+\n";

// Process off
exit 0;