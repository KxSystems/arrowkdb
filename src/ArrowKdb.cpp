#include <iostream>
#include <chrono>

#include <parquet/exception.h>
#include <arrow/config.h>

#include "TableData.h"
#include "HelperFunctions.h"

#include "ArrowKdb.h"


int main(int argc, char* argv[])
{
  // khp needs to link with: legacy_stdio_definitions.lib;c_static.lib;ws2_32.lib;Iphlpapi.lib
  //khp((S)"", -1);
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
    result = writeReadTable(schema, data);
  }

  std::cout << "Read " << kK(result)[0]->n << std::endl;

  return 0;
}

K getMemoryPoolStats(K unused)
{
  auto pool = arrow::default_memory_pool();

  std::cout << "Bytes allocated: " << pool->bytes_allocated() << std::endl;
  std::cout << "Max memory: " << pool->max_memory() << std::endl;
  std::cout << "Backend name:" << pool->backend_name() << std::endl;

  return (K)0;
}

EXP K buildInfo(K unused)
{
  auto info = arrow::GetBuildInfo();

  K version = ki(info.version);
  K so_version = ks((S)info.full_so_version.c_str());
  K git_desc = ks((S)info.git_description.c_str());
  K compiler_id = ks((S)(info.compiler_id + info.compiler_version).c_str());

  return knk(4, version, so_version, git_desc, compiler_id);
}
