// nested_datatypes.q
// Examples of creating a schema with nested datatypes and using it to read/write parquet and arrow tables

-1"\n+----------|| nested_datatypes.q ||----------+\n";

// import the arrowkdb library
\l q/arrowkdb.q

// Filesystem functions for Linux/MacOS/Windows
ls:{[filename] $[.z.o like "w*";system "dir /b ",filename;system "ls ",filename]};
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};


//---------------------//
// Create arrow schema //
//---------------------//

// Nested arrow datatypes are created in two ways:
// 1. List/map/dictionary datatypes are specified in terms of their child datatypes
// 2. Struct/union datatypes are specified in terms of their child fields

// Create a list<int64> using a child datatype

// Create an int64 datatype
int_dt:.arrowkdb.dt.int64[];

// Create a list datatype, using the int64 datatype as its child
list_dt:.arrowkdb.dt.list[int_dt];

// Create a field containing the list datatype
list_fd:.arrowkdb.fd.field[`list_field;list_dt];

// Create a struct<date32, float64, utf8> using child fields

// Create a date32 field
date_dt:.arrowkdb.dt.date32[];
date_fd:.arrowkdb.fd.field[`date_field;date_dt];

// Create a float64 field
float_dt:.arrowkdb.dt.float64[];
float_fd:.arrowkdb.fd.field[`float_field;float_dt];

// Create a utf8 field
string_dt:.arrowkdb.dt.utf8[];
string_fd:.arrowkdb.fd.field[`string_field;string_dt];

// Create a struct datatype using the date32, float64 and utf8 fields as its children
struct_dt:.arrowkdb.dt.struct[(date_fd,float_fd,string_fd)];

// Create a field containing the struct datatype
struct_fd:.arrowkdb.fd.field[`struct_field;struct_dt];

// Create the schema containing the list and struct fields
schema:.arrowkdb.sc.schema[(list_fd,struct_fd)];

// Display the schema
-1"\nSchema:";
.arrowkdb.sc.printSchema[schema]


//-----------------------//
// Create the array data //
//-----------------------//

// Create the data for the list array
list_data:(enlist 1;2 2;3 3 3);

// Create the data for each of the struct child fields
date_data:(2001.01.01 2002.02.02 2003.03.03);
float_data:(1.1 2.2 3.3f);
string_data:(enlist "a"; "bb"; "ccc");

// Create the data for the struct array from its child arrays
struct_data:(date_data;float_data;string_data);

// Combine the array data for the list and struct columns
array_data:(list_data;struct_data);

// Show the array data as an arrow table
-1"\nTable:";
.arrowkdb.tb.prettyPrintTable[schema;array_data;::]


//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Write the schema and array data to a parquet file
filename:"nested_datatypes.parquet";
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
filename:"nested_datatypes.arrow";
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