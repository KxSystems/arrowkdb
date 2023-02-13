// null_mapping_short.t

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping ||----------+\n";
short_opts:(`bool`uint8`int8`uint16`int16)!(0b;0x01;0x02;3h;4h);

options:(``NULL_MAPPING)!((::);short_opts);

N:5

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

bool_dt:.arrowkdb.dt.boolean[];
ui8_dt:.arrowkdb.dt.uint8[];
i8_dt:.arrowkdb.dt.int8[];
ui16_dt:.arrowkdb.dt.uint16[];
i16_dt:.arrowkdb.dt.int16[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

bool_fd:.arrowkdb.fd.field[`bool;bool_dt];
ui8_fd:.arrowkdb.fd.field[`uint8;ui8_dt];
i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
ui16_fd:.arrowkdb.fd.field[`uint16;ui16_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
short_schema:.arrowkdb.sc.schema[(ts_fd,bool_fd,ui8_fd,i8_fd,ui16_fd,i16_fd)];

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

bool_data:N?(0b;1b);
bool_data[0]:0b;
ui8_data:N?0x64;
ui8_data[1]:0x01;
i8_data:N?0x64;
i8_data[2]:0x02;
ui16_data:N?100h;
ui16_data[3]:3h;
i16_data:N?100h;
i16_data[4]:4h;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
short_data:(ts_data;bool_data;ui8_data;i8_data;ui16_data;i16_data);

-1"\n+----------|| Write the schema and array data to a parquet file ||----------+\n";
options[`PARQUET_VERSION]:`V2.0

parquet_short:"null_mapping_short.parquet";
.arrowkdb.pq.writeParquet[parquet_short;short_schema;short_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
parquet_short_schema:.arrowkdb.pq.readParquetSchema[parquet_short];
.arrowkdb.sc.equalSchemas[short_schema;parquet_short_schema]
short_schema~parquet_short_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
parquet_short_data:.arrowkdb.pq.readParquetData[parquet_short;options];
short_data~parquet_short_data
rm parquet_short;

-1"\n+----------|| Write the schema and array data to an arrow file ||----------+\n";
arrow_short:"null_mapping_short.arrow";
.arrowkdb.ipc.writeArrow[arrow_short;short_schema;short_data;options];

-1"\n+----------|| Read the schema back and compare ||----------+\n";
arrow_short_schema:.arrowkdb.ipc.readArrowSchema[arrow_short];
.arrowkdb.sc.equalSchemas[short_schema;arrow_short_schema]
short_schema~arrow_short_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
arrow_short_data:.arrowkdb.ipc.readArrowData[arrow_short;options];
short_data~arrow_short_data
rm arrow_short;

-1"\n+----------|| Serialize the schema and array data to an arrow stream ||----------+\n";
serialized_short:.arrowkdb.ipc.serializeArrow[short_schema;short_data;options];

-1"\n+----------|| Parse the schema back and compare ||----------+\n";
stream_short_schema:.arrowkdb.ipc.parseArrowSchema[serialized_short];
.arrowkdb.sc.equalSchemas[short_schema;stream_short_schema]
short_schema~stream_short_schema

-1"\n+----------|| Parse the array data back and compare ||----------+\n";
stream_short_data:.arrowkdb.ipc.parseArrowData[serialized_short;options];
short_data~stream_short_data


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
