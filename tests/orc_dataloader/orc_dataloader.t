// orc_dataloader.t

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

i8_dt:.arrowkdb.dt.int8[];
i16_dt:.arrowkdb.dt.int16[];
i32_dt:.arrowkdb.dt.int32[];
i64_dt:.arrowkdb.dt.int64[];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

i8_fd:.arrowkdb.fd.field[`int8;i8_dt];
i16_fd:.arrowkdb.fd.field[`int16;i16_dt];
i32_fd:.arrowkdb.fd.field[`int32;i32_dt];
i64_fd:.arrowkdb.fd.field[`int64;i64_dt];

-1"\n+----------|| Create the schemas for the list of fields ||----------+\n";
dataloader_schema:.arrowkdb.sc.schema[(ts_fd,i8_fd,i16_fd,i32_fd,i64_fd)];

-1"\n+----------|| Number of items in each array ||----------+\n";
N:10

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

i8_data:N?0x64;
i16_data:N?100h;
i32_data:N?100i;
i64_data:N?100;

-1"\n+----------|| Combine the data for all columns ||----------+\n";
dataloader_data:(ts_data;i8_data;i16_data;i32_data;i64_data);

-1"\n+----------|| Write the schema and array data to a ORC file ||----------+\n";
orc_options:(``ORC_CHUNK_SIZE)!((::);1024);

orc_dataloader:"orc_dataloader.orc"
.arrowkdb.orc.writeOrc[orc_dataloader;dataloader_schema;dataloader_data;orc_options]

-1"\n+----------|| Read the schema back and compare ||----------+\n";
orc_dataloader_schema:.arrowkdb.orc.readOrcSchema[orc_dataloader];
.arrowkdb.sc.equalSchemas[dataloader_schema;orc_dataloader_schema]
dataloader_schema~orc_dataloader_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
orc_dataloader_data:.arrowkdb.orc.readOrcData[orc_dataloader;orc_options];
dataloader_data~orc_dataloader_data
rm orc_dataloader;

-1 "<--- Read/write GZIP orc --->";
// Use GZIP compression
orc_options:`ORC_CHUNK_SIZE`COMPRESSION!(1024;`GZIP);
.arrowkdb.orc.writeOrc[orc_dataloader;dataloader_schema;dataloader_data;orc_options];
.arrowkdb.orc.readOrcSchema[orc_dataloader]~dataloader_schema
.arrowkdb.orc.readOrcData[orc_dataloader;orc_options]~dataloader_data
rm orc_dataloader;

-1 "<--- Read/write SNAPPY orc --->";
// Use SNAPPY compression
orc_options:`ORC_CHUNK_SIZE`COMPRESSION!(1024;`SNAPPY)
.arrowkdb.orc.writeOrc[orc_dataloader;dataloader_schema;dataloader_data;orc_options];
.arrowkdb.orc.readOrcSchema[orc_dataloader]~dataloader_schema
.arrowkdb.orc.readOrcData[orc_dataloader;orc_options]~dataloader_data
rm orc_dataloader;

-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
