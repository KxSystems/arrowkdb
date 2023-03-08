// batching_tables.q
// Examples of creating a schema supporting null mapping and using it to read/write parquet and arrow tables

-1"\n+----------|| batching_tables.q ||----------+\n";

// import the arrowkdb library
\l q/arrowkdb.q

// Filesystem functions for Linux/MacOS/Windows
ls:{[filename] $[.z.o like "w*";system "dir /b ",filename;system "ls ",filename]};
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

//-------------------//
// Create the table //
//-------------------//

// Support batching of large tables

// Create data for a large column in the table
batching_table:([]col:2147483652#0x00)
.arrowkdb.ts.writeReadArray[.arrowkdb.dt.int8[];batching_table`col;::]

// Write the batching table data to a parquet file
batching_options:(``PARQUET_VERSION)!((::);`V2.0)

parquet_batching:"batching_table.parquet";
.arrowkdb.pq.writeParquetFromTable[parquet_batching;batching_table;batching_options]
show ls parquet_batching
rm parquet_batching

// Write the batching array data to an arrow file
batching_options[`ARROW_CHUNK_ROWS]:214748365

arrow_batching:"batching_table.arrow";
.arrowkdb.ipc.writeArrowFromTable[arrow_batching;batching_table;batching_options]
show ls arrow_batching
rm arrow_batching;

// Serialize the batching array data to an arrow stream
serialized_batching:.arrowkdb.ipc.serializeArrowFromTable[batching_table;batching_options];
show serialized_batching


-1 "\n+----------------------------------------+\n";

// Process off
exit 0;
