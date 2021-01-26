// inferred_schema.q
// Examples of using kdb+ tables with inferred schemas to read/write parquet and arrow tables

-1"\n+----------|| inferred_schema.q ||----------+\n";

// import the arrowkdb library
\l ../q/arrowkdb.q


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
.arrowkdb.pq.writeParquetFromTable["inferred_schema.parquet";table;parquet_write_options];
show system "ls inferred_schema.parquet"

// Read the parquet file into another table
new_table:.arrowkdb.pq.readParquetToTable["inferred_schema.parquet";::];

// Compare the kdb+ tables
show table~new_table


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
.arrowkdb.ipc.writeArrowFromTable["inferred_schema.arrow";table];
show system "ls inferred_schema.arrow"

// Read the arrow file into another table
new_table:.arrowkdb.ipc.readArrowToTable["inferred_schema.arrow"];

// Compare the kdb+ tables
show table~new_table


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