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

EXP K buildInfo(K unused)
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

EXP K init(K unused)
{
  // Turn on symbol locking
  setm(1);

  // Create the singletons
  kx::arrowkdb::GetDatatypeStore();
  kx::arrowkdb::GetFieldStore();
  kx::arrowkdb::GetSchemaStore();

  return (K)0;
}
