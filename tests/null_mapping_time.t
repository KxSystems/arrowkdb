// null_mapping_time.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
time_opts:(`date32`timestamp`time64)!(2006.07.21;2011.01.01D00:00:00.000000000;12:00:00.000000000);

options:(``NULL_MAPPING)!((::);time_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

d32_dt:.arrowkdb.dt.date32[];
tstamp_dt:.arrowkdb.dt.timestamp[`nano];
t64_dt:.arrowkdb.dt.time64[`nano];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

d32_fd:.arrowkdb.fd.field[`date32;d32_dt];
tstamp_fd:.arrowkdb.fd.field[`timestamp;tstamp_dt];
t64_fd:.arrowkdb.fd.field[`time64;t64_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
time_schema:.arrowkdb.sc.schema[(ts_fd,d32_fd,tstamp_fd,t64_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

d32_data:N?(2006.07.21;2008.07.18;2012.07.16;2014.07.15;2016.07.11);
d32_data[0]:2006.07.21;
tstamp_data:N?(2015.01.01D00:00:00.000000000;2014.01.01D00:00:00.000000000;2013.01.01D00:00:00.000000000;2012.01.01D00:00:00.000000000;2011.01.01D00:00:00.000000000);
tstamp_data[2]:2011.01.01D00:00:00.000000000;
t64_data:N?(12:00:00.000000000;13:00:00.000000000;14:00:00.000000000;15:00:00.000000000;16:00:00.000000000);
t64_data[3]:12:00:00.000000000;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
time_data:(ts_data;d32_data;tstamp_data;t64_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
options[`PARQUET_VERSION]:`V2.0

parquet_time:"null_mapping_time.parquet";
.arrowkdb.pq.writeParquet[parquet_time;time_schema;time_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_time_schema:.arrowkdb.pq.readParquetSchema[parquet_time];
.arrowkdb.sc.equalSchemas[time_schema;parquet_time_schema]
time_schema~parquet_time_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_time_data:.arrowkdb.pq.readParquetData[parquet_time;options];
time_data~parquet_time_data
rm parquet_time;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_time:"null_mapping_time.arrow";
.arrowkdb.ipc.writeArrow[arrow_time;time_schema;time_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_time_schema:.arrowkdb.ipc.readArrowSchema[arrow_time];
.arrowkdb.sc.equalSchemas[time_schema;arrow_time_schema]
time_schema~arrow_time_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_time_data:.arrowkdb.ipc.readArrowData[arrow_time;options];
time_data~arrow_time_data
rm arrow_time;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_time:.arrowkdb.ipc.serializeArrow[time_schema;time_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_time_schema:.arrowkdb.ipc.parseArrowSchema[serialized_time];
.arrowkdb.sc.equalSchemas[time_schema;stream_time_schema]
time_schema~stream_time_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_time_data:.arrowkdb.ipc.parseArrowData[serialized_time;options];
time_data~stream_time_data


-1 "\n+----------------------------------------+\n";