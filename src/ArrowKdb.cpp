#include <iostream>
#include <chrono>

#include <parquet/exception.h>
#include <arrow/config.h>

#include "TableData.h"
#include "HelperFunctions.h"

#include "ArrowKdb.h"
#include "DatatypeStore.h"
#include "FieldStore.h"
#include "SchemaStore.h"
#include "ArrayReader.h"
#include "ArrayWriter.h"
#include "TableData.h"


// Main is only used for profiling on windows with arrowkdb.exe
int main(int argc, char* argv[])
{
  // khp needs to link with: legacy_stdio_definitions.lib;c_static.lib;ws2_32.lib;Iphlpapi.lib
  // khp((S)"", -1);
  K file;
  if (argc > 1)
    file = ks((S)argv[1]);
  else
    file = ks((S)"C:/Git/arrowkdb/x64/Release/file.parquet");


  auto start = std::chrono::steady_clock::now();
  K data = readParquetData(file, NULL);
  auto end = std::chrono::steady_clock::now();
  std::cout << "ReadTable: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms" << std::endl;

  return 0;

  K result;
  K schema = readParquetSchema(file);
  for (auto i = 0; i < 10; ++i) {
    result = writeReadTable(schema, data, NULL);
  }

  std::cout << "Read " << kK(result)[0]->n << std::endl;

  return 0;
}

K buildInfo(K unused)
{
  auto info = arrow::GetBuildInfo();

  // Not performance critical so just use join
  K keys = ktn(KS, 0);
  K values = ktn(0, 0);

  js(&keys, ss((S)"version"));
  jk(&values, ki(info.version));

  js(&keys, ss((S)"version_string"));
  jk(&values, ks((S)info.version_string.c_str()));

  js(&keys, ss((S)"full_so_version"));
  jk(&values, ks((S)info.full_so_version.c_str()));

  js(&keys, ss((S)"compiler_id"));
  jk(&values, ks((S)info.compiler_id.c_str()));

  js(&keys, ss((S)"compiler_version"));
  jk(&values, ks((S)info.compiler_version.c_str()));

  js(&keys, ss((S)"compiler_flags"));
  jk(&values, ks((S)info.compiler_flags.c_str()));

  js(&keys, ss((S)"git_id"));
  jk(&values, ks((S)info.git_id.c_str()));

  js(&keys, ss((S)"git_description"));
  jk(&values, ks((S)info.git_description.c_str()));

  js(&keys, ss((S)"package_kind"));
  jk(&values, ks((S)info.package_kind.c_str()));

  return xD(keys, values);
}

K init(K unused)
{
  // Turn on symbol locking
  setm(1);

  // Create the singletons
  kx::arrowkdb::GetDatatypeStore();
  kx::arrowkdb::GetFieldStore();
  kx::arrowkdb::GetSchemaStore();

  return (K)0;
}

K kexport(K _) 
{
  K x = xD(ktn(KS,0),ktn(0,0));
  #define REG(m,...) {K n=ktn(KS,0),f=ktn(0,0);__VA_ARGS__;K d=xD(n,f);js(&xx,ss((char *)m));jk(&xy,r1(d));}
  #define F(s,a) js(&n,ss((char *)#s));jk(&f,dl((V*)s,a));

  REG((char*)"dt",js(&n,ss((char*)"na"));jk(&f,dl((V*)null,1));F(boolean,1)
    F(int8,1)F(int16,1)F(int32,1)F(int64,1)
    F(uint8,1)F(uint16,1)F(uint32,1)F(uint64,1)
    F(float16,1)F(float32,1)F(float64,1)
    F(date32,1)F(date64,1)F(float64,1)F(month_interval,1)F(day_time_interval,1)
    F(binary,1)F(utf8,1)F(large_binary,1)F(large_utf8,1)
    F(time32,1)F(time64,1)F(timestamp,1)F(duration,1)F(fixed_size_binary,1)F(decimal128,2)
    F(list,1)F(large_list,1)F(fixed_size_list,2)F(map,2)F(dictionary,2)js(&n,ss((char*)"struct"));jk(&f,dl((V*)struct_,1));F(sparse_union,1)
    F(dense_union,1)F(inferDatatype,1)F(datatypeName,1)F(getTimeUnit,1)F(getByteWidth,1)F(getListSize,1)
    F(getPrecisionScale,1)F(getListDatatype,1)F(getMapDatatypes,1)F(getDictionaryDatatypes,1)F(getChildFields,1)
    js(&n,ss((char*)"printDatatype_"));jk(&f,dl((V*)printDatatype,1));F(listDatatypes,1)F(removeDatatype,1)F(equalDatatypes,2))

  REG((char*)"fd",F(field,2)F(fieldName,1)F(fieldDatatype,1)js(&n,ss((char*)"printField_"));jk(&f,dl((V*)printField,1));
    F(listFields,1)F(removeField,1)F(equalFields,2))

  REG((char*)"sc",F(schema,1)F(inferSchema,1)F(schemaFields,1)
    js(&n,ss((char*)"printSchema_"));jk(&f,dl((V*)printSchema,1));
    F(listSchemas,1)F(removeSchema,1)F(equalSchemas,2))

  REG((char*)"ar",js(&n,ss((char*)"prettyPrintArray_"));jk(&f,dl((V*)prettyPrintArray,3)))

  REG((char*)"tb",js(&n,ss((char*)"prettyPrintTable_"));jk(&f,dl((V*)prettyPrintTable,3)))

  REG((char*)"orc",js(&n,ss((char*)"writeOrc"));jk(&f,dl((V*)writeORC,4));js(&n,ss((char*)"readOrcSchema"));jk(&f,dl((V*)readORCSchema,1));
    js(&n,ss((char*)"readOrcData"));jk(&f,dl((V*)readORCData,2));)

  REG((char*)"pq",F(writeParquet,4)F(readParquetSchema,1)F(readParquetData,2)F(readParquetColumn,3)F(readParquetNumRowGroups,1)F(readParquetRowGroups,4))

  REG((char*)"ipc",F(writeArrow,4)F(readArrowSchema,1)F(readArrowData,2)F(serializeArrow,3)F(parseArrowSchema,1)F(parseArrowData,2))

  REG((char*)"util",F(buildInfo,1)F(init,1))

  REG((char*)"ts0",F(writeReadArray,3))

  REG((char*)"ts1",F(writeReadTable,3))

  R x;
}