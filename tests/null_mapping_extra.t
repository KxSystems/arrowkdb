// null_mapping_extra.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping only in arrow ||----------+\n";
extra_opts:(`float16`large_string`large_binary`duration)!(9h;"stop";"x"$"acknowledge";12:00:00.000000000);

options:(``NULL_MAPPING)!((::);extra_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

f16_dt:.arrowkdb.dt.float16[];
lstr_dt:.arrowkdb.dt.large_utf8[];
lbin_dt:.arrowkdb.dt.large_binary[];
dur_dt:.arrowkdb.dt.duration[`milli];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

f16_fd:.arrowkdb.fd.field[`float16;f16_dt];
lstr_fd:.arrowkdb.fd.field[`large_string;lstr_dt];
lbin_fd:.arrowkdb.fd.field[`large_binary;lbin_dt];
dur_fd:.arrowkdb.fd.field[`duration;dur_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
extra_schema:.arrowkdb.sc.schema[(ts_fd,f16_fd,lstr_fd,lbin_fd,dur_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

f16_data:N?100h;
f16_data[0]:9h;
lstr_data:N?("start";"stop";"alert";"acknowledge";"");
lstr_data[1]:"stop"
lbin_data:N?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
lbin_data[3]:"x"$"acknowledge"
dur_data:N?(12:00:00.000000000;13:00:00.000000000;14:00:00.000000000;15:00:00.000000000;16:00:00.000000000);
dur_data[4]:12:00:00.000000000;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
extra_data:(ts_data;f16_data;lstr_data;lbin_data;dur_data);

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_extra:"null_mapping_extra.arrow";
.arrowkdb.ipc.writeArrow[arrow_extra;extra_schema;extra_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_extra_schema:.arrowkdb.ipc.readArrowSchema[arrow_extra];
.arrowkdb.sc.equalSchemas[extra_schema;arrow_extra_schema]
extra_schema~arrow_extra_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_extra_data:.arrowkdb.ipc.readArrowData[arrow_extra;options];
extra_data~arrow_extra_data
rm arrow_extra;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_extra:.arrowkdb.ipc.serializeArrow[extra_schema;extra_data;options];

-1"\n+----------|| Parse the schema back abd compare ||----------+\n";
stream_extra_schema:.arrowkdb.ipc.parseArrowSchema[serialized_extra];
.arrowkdb.sc.equalSchemas[extra_schema;stream_extra_schema]
extra_schema~stream_extra_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_extra_data:.arrowkdb.ipc.parseArrowData[serialized_extra;options];

extra_data~stream_extra_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
