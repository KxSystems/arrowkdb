// inferred_schema.q
// Examples of using kdb+ tables with inferred schemas to read/write parquet and arrow tables

-1"\n+----------|| inferred_schema.q ||----------+\n";

// import the arrowkdb library
\l q/arrowkdb.q

// Filesystem functions for Linux/MacOS/Windows
ls:{[filename] $[.z.o like "w*";system "dir /b ",filename;system "ls ",filename]};
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};


//-------------------------//
// Example-1. Parquet file //
//-------------------------//

// Create a kdb+ table
N:5;
table:([]tstamp:asc N?0p;temperature:N?100f;fill_level:N?100;pump_status:N?0b;comment:N?("start";"stop";"alert";"acknowledge";""));
show table;

// Show the schema which is inferred from the table
.arrowkdb.sc.printSchema[.arrowkdb.sc.inferSchema[table]];

// Use Parquet v2.0
// This is required otherwise the timestamp(ns) datatype will be converted to timestamp(us) resulting in a loss of precision
parquet_write_options:(enlist `PARQUET_VERSION)!(enlist `V2.0);

// Write the table to a parquet file
filename:"inferred_schema.parquet";
.arrowkdb.pq.writeParquetFromTable[filename;table;parquet_write_options];
show ls filename

// Read the parquet file into another table
new_table:.arrowkdb.pq.readParquetToTable[filename;::];

// Compare the kdb+ tables
show table~new_table
rm filename;


//---------------------------//
// Example-2. Arrow IPC file //
//---------------------------//

// Create a kdb+ table
N:5;
table:([]tstamp:asc N?0p;temperature:N?100f;fill_level:N?100;pump_status:N?0b;comment:N?("start";"stop";"alert";"acknowledge";""));
show table;

// Show the schema which is inferred from the table
.arrowkdb.sc.printSchema[.arrowkdb.sc.inferSchema[table]];

// Write the table to an arrow file
filename:"inferred_schema.arrow";
.arrowkdb.ipc.writeArrowFromTable[filename;table];
show ls filename

// Read the arrow file into another table
new_table:.arrowkdb.ipc.readArrowToTable["inferred_schema.arrow";::];

// Compare the kdb+ tables
show table~new_table
rm filename;


//-----------------------------//
// Example-3. Arrow IPC stream //
//-----------------------------//

// Create a kdb+ table
N:5;
table:([]tstamp:asc N?0p;temperature:N?100f;fill_level:N?100;pump_status:N?0b;comment:N?("start";"stop";"alert";"acknowledge";""));
show table;

// Show the schema which is inferred from the table
.arrowkdb.sc.printSchema[.arrowkdb.sc.inferSchema[table]];

// Serialize the table to an arrow stream
serialized:.arrowkdb.ipc.serializeArrowFromTable[table];
show serialized

// Parse the arrow stream into another table
new_table:.arrowkdb.ipc.parseArrowToTable[serialized];

// Compare the kdb+ tables
show table~new_table


-1 "\n+----------------------------------------+\n";

// Process off
exit 0;