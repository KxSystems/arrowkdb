\d .arrowkdb

// datatype constructors:
// concrete datatypes:
dt.na:`arrowkdb 2:(`null;1);
dt.boolean:`arrowkdb 2:(`boolean;1);
dt.uint8:`arrowkdb 2:(`uint8;1);
dt.int8:`arrowkdb 2:(`int8;1);
dt.uint16:`arrowkdb 2:(`uint16;1);
dt.int16:`arrowkdb 2:(`int16;1);
dt.uint32:`arrowkdb 2:(`uint32;1);
dt.int32:`arrowkdb 2:(`int32;1);
dt.uint64:`arrowkdb 2:(`uint64;1);
dt.int64:`arrowkdb 2:(`int64;1);
dt.float16:`arrowkdb 2:(`float16;1);
dt.float32:`arrowkdb 2:(`float32;1);
dt.float64:`arrowkdb 2:(`float64;1);
dt.utf8:`arrowkdb 2:(`utf8;1);
dt.large_utf8:`arrowkdb 2:(`large_utf8;1);
dt.binary:`arrowkdb 2:(`binary;1);
dt.large_binary:`arrowkdb 2:(`large_binary;1);
dt.date32:`arrowkdb 2:(`date32;1);
dt.date64:`arrowkdb 2:(`date64;1);
dt.month_interval:`arrowkdb 2:(`month_interval;1);
dt.day_time_interval:`arrowkdb 2:(`day_time_interval;1);
// parameterised datatypes:
dt.fixed_size_binary:`arrowkdb 2:(`fixed_size_binary;1);
dt.timestamp:`arrowkdb 2:(`timestamp;1);
dt.time32:`arrowkdb 2:(`time32;1);
dt.time64:`arrowkdb 2:(`time64;1);
dt.duration:`arrowkdb 2:(`duration;1);
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
// derive from kdb list:
dt.deriveDatatype:`arrowkdb 2:(`deriveDatatype;1);

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
dt.listDatatypes:`arrowkdb 2:(`listDatatypes;1);
dt.printDatatype_:`arrowkdb 2:(`printDatatype;1);
dt.printDatatype:{[x] -1 dt.printDatatype_[x];};
dt.removeDatatype:`arrowkdb 2:(`removeDatatype;1);
dt.equalDatatypes:`arrowkdb 2:(`equalDatatypes;2);


//field constructor:
fd.field:`arrowkdb 2:(`field;2);

// field inspection:
fd.fieldName:`arrowkdb 2:(`fieldName;1);
fd.fieldDatatype:`arrowkdb 2:(`fieldDatatype;1);

// field management:
fd.listFields:`arrowkdb 2:(`listFields;1);
fd.printField_:`arrowkdb 2:(`printField;1);
fd.printField:{[x] -1 fd.printField_[x];};
fd.removeField:`arrowkdb 2:(`removeField;1);
fd.equalFields:`arrowkdb 2:(`equalFields;2);


// schema constructors:
// from fields:
sc.schema:`arrowkdb 2:(`schema;1);
// derived from dictionary:
sc.deriveSchema:`arrowkdb 2:(`deriveSchema;1);

// schema inspection:
sc.schemaFields:`arrowkdb 2:(`schemaFields;1);

// schema management
sc.listSchemas:`arrowkdb 2:(`listSchemas;1);
sc.printSchema_:`arrowkdb 2:(`printSchema;1);
sc.printSchema:{[x] -1 sc.printSchema_[x];};
sc.removeSchema:`arrowkdb 2:(`removeSchema;1);
sc.equalSchemas:`arrowkdb 2:(`equalSchemas;2);


// array data
ar.prettyPrintArray_:`arrowkdb 2:(`prettyPrintArray;2);
ar.prettyPrintArray:{[x;y] -1 ar.prettyPrintArray_[x;y];};
ar.prettyPrintArrayFromList:{[list] ar.prettyPrintArray[dt.deriveDatatype[list];list]};


// table data
tb.prettyPrintTable_:`arrowkdb 2:(`prettyPrintTable;2);
tb.prettyPrintTable:{[x;y] -1 tb.prettyPrintTable_[x;y];};
tb.prettyPrintTableFromDict:{[dict] tb.prettyPrintTable[sc.deriveSchema[dict];value dict]};
tb.prettyPrintTableFromTable:{[table] tb.prettyPrintTable[sc.deriveSchema[table];value flip table]};


// parquet files
pq.writeParquet:`arrowkdb 2:(`writeParquet;4);
pq.writeParquetFromDict:{[filename;dict;options] pq.writeParquet[filename;sc.deriveSchema[dict];value dict;options]};
pq.writeParquetFromTable:{[filename;table;options] pq.writeParquet[filename;sc.deriveSchema[table];value flip table;options]};
pq.readParquetSchema:`arrowkdb 2:(`readParquetSchema;1);
pq.readParquetData:`arrowkdb 2:(`readParquetData;2);


// arrow files
ipc.writeArrow:`arrowkdb 2:(`writeArrow;3);
ipc.writeArrowFromDict:{[filename;dict] ipc.writeArrow[filename;sc.deriveSchema[dict];value dict]};
ipc.writeArrowFromTable:{[filename;table] ipc.writeArrow[filename;sc.deriveSchema[table];value flip table]};
ipc.readArrowSchema:`arrowkdb 2:(`readArrowSchema;1);
ipc.readArrowData:`arrowkdb 2:(`readArrowData;1);


// arrow streams
ipc.serializeArrow:`arrowkdb 2:(`serializeArrow;2);
ipc.serializeArrowFromDict:{[dict] ipc.serializeArrow[sc.deriveSchema[dict];value dict]};
ipc.serializeArrowFromTable:{[dict] ipc.serializeArrow[sc.deriveSchema[table];value flip table]};
ipc.parseArrowSchema:`arrowkdb 2:(`parseArrowSchema;1);
ipc.parseArrowData:`arrowkdb 2:(`parseArrowData;1);

