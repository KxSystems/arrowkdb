\d .arrowkdb

// datatype constructors:
// concrete datatypes:
dt.na:`arrowkdb 2:(`null;1);
dt.boolean:`arrowkdb 2:(`boolean;1);
dt.int8:`arrowkdb 2:(`int8;1);
dt.int16:`arrowkdb 2:(`int16;1);
dt.int32:`arrowkdb 2:(`int32;1);
dt.int64:`arrowkdb 2:(`int64;1);
dt.uint8:`arrowkdb 2:(`uint8;1);
dt.uint16:`arrowkdb 2:(`uint16;1);
dt.uint32:`arrowkdb 2:(`uint32;1);
dt.uint64:`arrowkdb 2:(`uint64;1);
dt.float16:`arrowkdb 2:(`float16;1);
dt.float32:`arrowkdb 2:(`float32;1);
dt.float64:`arrowkdb 2:(`float64;1);
dt.date32:`arrowkdb 2:(`date32;1);
dt.date64:`arrowkdb 2:(`date64;1);
dt.month_interval:`arrowkdb 2:(`month_interval;1);
dt.day_time_interval:`arrowkdb 2:(`day_time_interval;1);
dt.binary:`arrowkdb 2:(`binary;1);
dt.utf8:`arrowkdb 2:(`utf8;1);
dt.large_binary:`arrowkdb 2:(`large_binary;1);
dt.large_utf8:`arrowkdb 2:(`large_utf8;1);
// parameterized datatypes:
dt.time32:`arrowkdb 2:(`time32;1);
dt.time64:`arrowkdb 2:(`time64;1);
dt.timestamp:`arrowkdb 2:(`timestamp;1);
dt.duration:`arrowkdb 2:(`duration;1);
dt.fixed_size_binary:`arrowkdb 2:(`fixed_size_binary;1);
dt.decimal128:`arrowkdb 2:(`decimal128;2);
// nested datatype constructors:
// from datatypes:
dt.list:`arrowkdb 2:(`list;1);
dt.large_list:`arrowkdb 2:(`large_list;1);
dt.fixed_size_list:`arrowkdb 2:(`fixed_size_list;2);
dt.map:`arrowkdb 2:(`map;2);
dt.dictionary:`arrowkdb 2:(`dictionary;2);
// from fields:
dt.struct:`arrowkdb 2:(`struct_;1);
dt.sparse_union:`arrowkdb 2:(`sparse_union;1);
dt.dense_union:`arrowkdb 2:(`dense_union;1);
// infer from kdb list:
dt.inferDatatype:`arrowkdb 2:(`inferDatatype;1);

// datatype inspection:
dt.datatypeName:`arrowkdb 2:(`datatypeName;1);
dt.getTimeUnit:`arrowkdb 2:(`getTimeUnit;1);
dt.getByteWidth:`arrowkdb 2:(`getByteWidth;1);
dt.getListSize:`arrowkdb 2:(`getListSize;1);
dt.getPrecisionScale:`arrowkdb 2:(`getPrecisionScale;1);
dt.getListDatatype:`arrowkdb 2:(`getListDatatype;1);
dt.getMapDatatypes:`arrowkdb 2:(`getMapDatatypes;1);
dt.getDictionaryDatatypes:`arrowkdb 2:(`getDictionaryDatatypes;1);
dt.getChildFields:`arrowkdb 2:(`getChildFields;1);

// datatype management:
dt.printDatatype_:`arrowkdb 2:(`printDatatype;1);
dt.printDatatype:{[x] -1 dt.printDatatype_[x];};
dt.listDatatypes:`arrowkdb 2:(`listDatatypes;1);
dt.removeDatatype:`arrowkdb 2:(`removeDatatype;1);
dt.equalDatatypes:`arrowkdb 2:(`equalDatatypes;2);


//field constructor:
fd.field:`arrowkdb 2:(`field;2);

// field inspection:
fd.fieldName:`arrowkdb 2:(`fieldName;1);
fd.fieldDatatype:`arrowkdb 2:(`fieldDatatype;1);

// field management:
fd.printField_:`arrowkdb 2:(`printField;1);
fd.printField:{[x] -1 fd.printField_[x];};
fd.listFields:`arrowkdb 2:(`listFields;1);
fd.removeField:`arrowkdb 2:(`removeField;1);
fd.equalFields:`arrowkdb 2:(`equalFields;2);


// schema constructors:
// from fields:
sc.schema:`arrowkdb 2:(`schema;1);
// inferred from table:
sc.inferSchema:`arrowkdb 2:(`inferSchema;1);

// schema inspection:
sc.schemaFields:`arrowkdb 2:(`schemaFields;1);

// schema management
sc.printSchema_:`arrowkdb 2:(`printSchema;1);
sc.printSchema:{[x] -1 sc.printSchema_[x];};
sc.listSchemas:`arrowkdb 2:(`listSchemas;1);
sc.removeSchema:`arrowkdb 2:(`removeSchema;1);
sc.equalSchemas:`arrowkdb 2:(`equalSchemas;2);


// array data
ar.prettyPrintArray_:`arrowkdb 2:(`prettyPrintArray;3);
ar.prettyPrintArray:{[x;y;z] -1 ar.prettyPrintArray_[x;y;z];};
ar.prettyPrintArrayFromList:{[list;options] ar.prettyPrintArray[dt.inferDatatype[list];list;options]};


// table data
tb.prettyPrintTable_:`arrowkdb 2:(`prettyPrintTable;3);
tb.prettyPrintTable:{[x;y;z] -1 tb.prettyPrintTable_[x;y;z];};
tb.prettyPrintTableFromTable:{[table;options] tb.prettyPrintTable[sc.inferSchema[table];value flip table;options]};


// parquet files
pq.writeParquet:`arrowkdb 2:(`writeParquet;4);
pq.writeParquetFromTable:{[filename;table;options] pq.writeParquet[filename;sc.inferSchema[table];value flip table;options]};
pq.readParquetSchema:`arrowkdb 2:(`readParquetSchema;1);
pq.readParquetData:`arrowkdb 2:(`readParquetData;2);
pq.readParquetToTable:{[filename;options] flip (fd.fieldName each sc.schemaFields[pq.readParquetSchema[filename]])!(pq.readParquetData[filename;options])};
pq.readParquetColumn:`arrowkdb 2:(`readParquetColumn;3);
pq.readParquetNumRowGroups:`arrowkdb 2:(`readParquetNumRowGroups;1);
pq.readParquetRowGroups:`arrowkdb 2:(`readParquetRowGroups;4);
pq.readParquetRowGroupsToTable:{[filename;row_groups;columns;options] flip (fd.fieldName each sc.schemaFields[pq.readParquetSchema[filename]](columns))!(pq.readParquetRowGroups[filename;row_groups;columns;options])};


// arrow files
ipc.writeArrow:`arrowkdb 2:(`writeArrow;4);
ipc.writeArrowFromTable:{[filename;table;options] ipc.writeArrow[filename;sc.inferSchema[table];value flip table;options]};
ipc.readArrowSchema:`arrowkdb 2:(`readArrowSchema;1);
ipc.readArrowData:`arrowkdb 2:(`readArrowData;2);
ipc.readArrowToTable:{[filename;options] flip (fd.fieldName each sc.schemaFields[ipc.readArrowSchema[filename]])!(ipc.readArrowData[filename;options])};


// arrow streams
ipc.serializeArrow:`arrowkdb 2:(`serializeArrow;3);
ipc.serializeArrowFromTable:{[table;options] ipc.serializeArrow[sc.inferSchema[table];value flip table;options]};
ipc.parseArrowSchema:`arrowkdb 2:(`parseArrowSchema;1);
ipc.parseArrowData:`arrowkdb 2:(`parseArrowData;2);
ipc.parseArrowToTable:{[serialized;options] flip (fd.fieldName each sc.schemaFields[ipc.parseArrowSchema[serialized]])!(ipc.parseArrowData[serialized;options])};


// utils
util.buildInfo:`arrowkdb 2:(`buildInfo;1);
util.init:`arrowkdb 2:(`init;1);


// testing
ts.writeReadArray:`arrowkdb 2:(`writeReadArray;3);
ts.writeReadTable:`arrowkdb 2:(`writeReadTable;3);


// initialise
util.init[];
