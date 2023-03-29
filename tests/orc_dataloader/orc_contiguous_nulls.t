// orc_contiguous_nulls.q

-1"\n+----------|| Import the arrowkdb library ||----------+\n";
\l q/arrowkdb.q

-1"\n+----------|| Filesystem functions for Linux/MacOS/Windows ||----------+\n";
rm:{[filename] $[.z.o like "w*";system "del ",filename;system "rm ",filename]};

-1"\n+----------|| Support null mapping in Apache ORC ||----------+\n";
cont_opts:(`utf8`binary`decimal)!("start";"x"$"alert";3.21f);

contiguous_options:(``NULL_MAPPING)!((::);cont_opts);

-1"\n+----------|| Create the datatype identifiers ||----------+\n";
ts_dt:.arrowkdb.dt.timestamp[`nano];

str_dt:.arrowkdb.dt.utf8[];
bin_dt:.arrowkdb.dt.binary[];
dec_dt:.arrowkdb.dt.decimal128[38i;2i];

-1"\n+----------|| Create the field identifiers ||----------+\n";
ts_fd:.arrowkdb.fd.field[`tstamp;ts_dt];

str_fd:.arrowkdb.fd.field[`string;str_dt];
bin_fd:.arrowkdb.fd.field[`binary;bin_dt];
dec_fd:.arrowkdb.fd.field[`decimal;dec_dt];

contiguous_schema:.arrowkdb.sc.schema[(str_fd, bin_fd, dec_fd)];

-1"\n+----------|| Number of items in each array ||----------+\n";
N:5

-1"\n+----------|| Create data for each column in the table ||----------+\n";
ts_data:asc N?0p;

str_data:N?("start";"stop";"alert";"acknowledge";"");
str_data[0]:"start"
bin_data:N?("x"$"start";"x"$"stop";"x"$"alert";"x"$"acknowledge";"x"$"");
bin_data[1]:"x"$"alert"
dec_data:{"F"$.Q.f[2]x} each N?(10f)
dec_data[2]:3.21f

-1"\n+----------|| Combine the data for contiguous columns ||----------+\n";
contiguous_options[`DECIMAL128_AS_DOUBLE]:1 

contiguous_data:(str_data;bin_data;dec_data);

-1"\n+----------|| Write the schema and array data to a ORC file ||----------+\n";
contiguous_options[`ORC_CHUNK_SIZE]:1024

orc_contiguous:"contiguous_bitmap.orc";
.arrowkdb.orc.writeOrc[orc_contiguous;contiguous_schema;contiguous_data;contiguous_options]

-1"\n+----------|| Read the schema back and compare ||----------+\n";
contiguous_options[`WITH_NULL_BITMAP]:1;

orc_contiguous_schema:.arrowkdb.orc.readOrcSchema[orc_contiguous];
.arrowkdb.sc.equalSchemas[contiguous_schema;orc_contiguous_schema]
contiguous_schema~orc_contiguous_schema

-1"\n+----------|| Read the array data back and compare ||----------+\n";
orc_contiguous_data:.arrowkdb.orc.readOrcData[orc_contiguous;contiguous_options];
contiguous_data~first orc_contiguous_data

-1"\n+----------|| Compare null bitmaps of arrow data ||----------+\n";
contiguous_nulls:(10000b;01000b;00100b);
orc_contiguous_nulls:last orc_contiguous_data;
contiguous_nulls~contiguous_nulls & orc_contiguous_nulls

rm orc_contiguous;


-1 "\n+----------|| Test utils ||----------+\n";

show .arrowkdb.util.buildInfo[]
(type .arrowkdb.util.buildInfo[])~99h


-1 "\n+----------|| Finished testing ||----------+\n";
