// datatype constructors:
// concrete datatypes:
na:`arrowkdb 2:(`null;1);
boolean:`arrowkdb 2:(`boolean;1);
uint8:`arrowkdb 2:(`uint8;1);
int8:`arrowkdb 2:(`int8;1);
uint16:`arrowkdb 2:(`uint16;1);
int16:`arrowkdb 2:(`int16;1);
uint32:`arrowkdb 2:(`uint32;1);
int32:`arrowkdb 2:(`int32;1);
uint64:`arrowkdb 2:(`uint64;1);
int64:`arrowkdb 2:(`int64;1);
float16:`arrowkdb 2:(`float16;1);
float32:`arrowkdb 2:(`float32;1);
float64:`arrowkdb 2:(`float64;1);
utf8:`arrowkdb 2:(`utf8;1);
large_utf8:`arrowkdb 2:(`large_utf8;1);
binary:`arrowkdb 2:(`binary;1);
large_binary:`arrowkdb 2:(`large_binary;1);
date32:`arrowkdb 2:(`date32;1);
date64:`arrowkdb 2:(`date64;1);
month_interval:`arrowkdb 2:(`month_interval;1);
day_time_interval:`arrowkdb 2:(`day_time_interval;1);
// parameterised datatypes:
fixed_size_binary:`arrowkdb 2:(`fixed_size_binary;1);
timestamp:`arrowkdb 2:(`timestamp;1);
time32:`arrowkdb 2:(`time32;1);
time64:`arrowkdb 2:(`time64;1);
duration:`arrowkdb 2:(`duration;1);
decimal128:`arrowkdb 2:(`decimal128;2);
// nested datatype constructors:
// from datatypes:
list:`arrowkdb 2:(`list;1);
large_list:`arrowkdb 2:(`large_list;1);
fixed_size_list:`arrowkdb 2:(`fixed_size_list;2);
map:`arrowkdb 2:(`map;2);
dictionary:`arrowkdb 2:(`dictionary;2);
// from fields:
struct:`arrowkdb 2:(`struct_;1);
sparse_union:`arrowkdb 2:(`sparse_union;1);
dense_union:`arrowkdb 2:(`dense_union;1);
// derive from kdb list:
deriveDatatype:`arrowkdb 2:(`deriveDatatype;1);

// datatype inspection:
datatypeName:`arrowkdb 2:(`datatypeName;1);
getTimeUnit:`arrowkdb 2:(`getTimeUnit;1);
getByteWidth:`arrowkdb 2:(`getByteWidth;1);
getListSize:`arrowkdb 2:(`getListSize;1);
getPrecisionScale:`arrowkdb 2:(`getPrecisionScale;1);
getListDatatype:`arrowkdb 2:(`getListDatatype;1);
getMapDatatypes:`arrowkdb 2:(`getMapDatatypes;1);
getChildFields:`arrowkdb 2:(`getChildFields;1);

// datatype management:
listDatatypes:`arrowkdb 2:(`listDatatypes;1);
printDatatype_:`arrowkdb 2:(`printDatatype;1);
printDatatype:{[x] -1 printDatatype_[x];};
removeDatatype:`arrowkdb 2:(`removeDatatype;1);
equalDatatypes:`arrowkdb 2:(`equalDatatypes;2);


//field constructor:
field:`arrowkdb 2:(`field;2);

// field inspection:
fieldName:`arrowkdb 2:(`fieldName;1);
fieldDatatype:`arrowkdb 2:(`fieldDatatype;1);

// field management:
listFields:`arrowkdb 2:(`listFields;1);
printField_:`arrowkdb 2:(`printField;1);
printField:{[x] -1 printField_[x];};
removeField:`arrowkdb 2:(`removeField;1);
equalFields:`arrowkdb 2:(`equalFields;2);


// schema constructors:
// from fields:
schema:`arrowkdb 2:(`schema;1);
// derived from dictionary:
deriveSchema:`arrowkdb 2:(`deriveSchema;1);

// schema inspection:
schemaFields:`arrowkdb 2:(`schemaFields;1);

// schema management
listSchemas:`arrowkdb 2:(`listSchemas;1);
printSchema_:`arrowkdb 2:(`printSchema;1);
printSchema:{[x] -1 printSchema_[x];};
removeSchema:`arrowkdb 2:(`removeSchema;1);
equalSchemas:`arrowkdb 2:(`equalSchemas;2);


// array data
prettyPrintArray_:`arrowkdb 2:(`prettyPrintArray;2);
prettyPrintArray:{[x;y] -1 prettyPrintArray_[x;y];};
prettyPrintArrayFromList:{[list] prettyPrintArray[deriveDatatype[list];list]};
writeReadArray:`arrowkdb 2:(`writeReadArray;2);
writeReadArrayFromList:{[list] writeReadArray[deriveDatatype[list];list]};


// table data
prettyPrintTable_:`arrowkdb 2:(`prettyPrintTable;2);
prettyPrintTable:{[x;y] -1 prettyPrintTable_[x;y];};
prettyPrintTableFromDict:{[dict] prettyPrintTable[deriveSchema[dict];value dict]};
prettyPrintTableFromTable:{[table] prettyPrintTable[deriveSchema[table];value flip table]};
writeReadTable:`arrowkdb 2:(`writeReadTable;2);
writeReadTableFromDict:{[dict] writeReadTable[deriveSchema[dict];value dict]};
writeReadTableFromTable:{[table] writeReadTable[deriveSchema[table];value flip table]};


// parquet files
writeParquet:`arrowkdb 2:(`writeParquet;4);
writeParquetFromDict:{[filename;dict;options] writeParquet[filename;deriveSchema[dict];value dict;options]};
writeParquetFromTable:{[filename;table;options] writeParquet[filename;deriveSchema[table];value flip table;options]};
readParquetSchema:`arrowkdb 2:(`readParquetSchema;1);
readParquetData:`arrowkdb 2:(`readParquetData;2);


// arrow files
writeArrow:`arrowkdb 2:(`writeArrow;3);
writeArrowFromDict:{[filename;dict] writeArrow[filename;deriveSchema[dict];value dict]};
writeArrowFromTable:{[filename;table] writeArrow[filename;deriveSchema[table];value flip table]};
readArrowSchema:`arrowkdb 2:(`readArrowSchema;1);
readArrowData:`arrowkdb 2:(`readArrowData;1);


// arrow streams
serializeArrow:`arrowkdb 2:(`serializeArrow;2);
serializeArrowFromDict:{[dict] serializeArrow[deriveSchema[dict];value dict]};
serializeArrowFromTable:{[dict] serializeArrow[deriveSchema[table];value flip table]};
parseArrowSchema:`arrowkdb 2:(`parseArrowSchema;1);
parseArrowData:`arrowkdb 2:(`parseArrowData;1);


// force a one element mixed list from existing kdb object
mixed:`arrowkdb 2:(`mixed;1);

getMemoryPoolStats:`arrowkdb 2:(`getMemoryPoolStats;1);
oneOneTwo:`arrowkdb 2:(`oneOneTwo;1);
datatypes:`arrowkdb 2:(`datatypes;1);

copyPrealloc:`arrowkdb 2:(`copyPrealloc;1);
copyPreallocBulk:`arrowkdb 2:(`copyPreallocBulk;1);
copyJoin:`arrowkdb 2:(`copyJoin;1);

\d .arrowkdb

datatype.float64:`arrowkdb 2:(`float64;1);
datatype.int64:`arrowkdb 2:(`int64;1);
datatype.utf8:`arrowkdb 2:(`utf8;1);

field.field:`arrowkdb 2:(`field;2);

schema.schema:`arrowkdb 2:(`schema;1);