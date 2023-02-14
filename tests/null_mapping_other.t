// null_mapping_other.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping only in arrow ||----------+\n";
other_opts:(`date64`time32`month_interval`day_time_interval)!(2015.01.01D00:00:00.000000000;09:01:02.042;2006.07m;12:00:00.000000000);

options:(``NULL_MAPPING)!((::);other_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

d64_dt:.arrowkdb.dt.date64[];
t32_dt:.arrowkdb.dt.time32[`milli];
mint_dt:.arrowkdb.dt.month_interval[];
dtint_dt:.arrowkdb.dt.day_time_interval[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

d64_fd:.arrowkdb.fd.field[`date64;d64_dt];
t32_fd:.arrowkdb.fd.field[`time32;t32_dt];
mint_fd:.arrowkdb.fd.field[`month_interval;mint_dt];
dtint_fd:.arrowkdb.fd.field[`day_time_interval;dtint_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
other_schema:.arrowkdb.sc.schema[(ts_fd,d64_fd,t32_fd,mint_fd,dtint_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

d64_data:N?(2015.01.01D00:00:00.000000000;2017.01.01D00:00:00.000000000;2018.01.01D00:00:00.000000000;2019.01.01D00:00:00.000000000;2020.01.01D00:00:00.000000000);
d64_data[1]:2015.01.01D00:00:00.000000000;
t32_data:N?(09:01:02.042;08:01:02.042;07:01:02.042;06:01:02.042;05:01:02.042);
t32_data[1]:09:01:02.042;
mint_data:N?(2006.07m;2006.06m;2006.05m;2006.04m;2006.03m);
mint_data[2]:2006.07m;
dtint_data:N?(12:00:00.000000000;11:00:00.000000000;10:00:00.000000000;09:00:00.000000000;08:00:00.000000000);
dtint_data[3]:12:00:00.000000000;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
other_data:(ts_data;d64_data;t32_data;mint_data;dtint_data);

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_other:"null_mapping_other.arrow";
.arrowkdb.ipc.writeArrow[arrow_other;other_schema;other_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_other_schema:.arrowkdb.ipc.readArrowSchema[arrow_other];
.arrowkdb.sc.equalSchemas[other_schema;arrow_other_schema]
other_schema~arrow_other_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_other_data:.arrowkdb.ipc.readArrowData[arrow_other;options];
other_data~arrow_other_data
rm arrow_other;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_other:.arrowkdb.ipc.serializeArrow[other_schema;other_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_other_schema:.arrowkdb.ipc.parseArrowSchema[serialized_other];
.arrowkdb.sc.equalSchemas[other_schema;stream_other_schema]
other_schema~stream_other_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_other_data:.arrowkdb.ipc.parseArrowData[serialized_other;options];
other_data~stream_other_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
