#include <iostream>
#include <chrono>

#include <parquet/exception.h>

#include "TableData.h"
#include "HelperFunctions.h"

#include "ArrowKdb.h"


K mixed(K value)
{
  r1(value);
  return knk(1, value);
}

K destructor(K item)
{
  std::cout << "Destroying " << (size_t)kK(item)[1] << std::endl;
  return (K)0;
}

K oneOneTwo(K value)
{
  K result = knk(2, destructor, value->j);
  result->t = 112;
  return result;
}

K datatypes(K unused)
{
  {
    auto first = arrow::int32();
    auto second = arrow::int32();
    std::cout << first->ToString() << " == " << second->ToString() << " : " << (first == second) << std::endl;
    std::cout << first->ToString() << " ->Equals() " << second->ToString() << " : " << first->Equals(second) << std::endl;
  }
  {
    auto first = arrow::fixed_size_binary(4);
    auto second = arrow::fixed_size_binary(4);
    std::cout << first->ToString() << " == " << second->ToString() << " : " << (first == second) << std::endl;
    std::cout << first->ToString() << " ->Equals() " << second->ToString() << " : " << first->Equals(second) << std::endl;
  }
  return (K)0;
}

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

K copyPrealloc(K k_list)
{
  K result = ktn(KJ, k_list->n);

  for (auto i = 0; i < k_list->n; ++i)
    kJ(result)[i] = kJ(k_list)[i];

  return result;
}

K copyPreallocBulk(K k_list)
{
  K result = ktn(KJ, k_list->n);

  memcpy(kJ(result), kJ(k_list), k_list->n * sizeof(J));

  return result;
}

K copyJoin(K k_list)
{
  K result = ktn(KJ, 0);

  for (auto i = 0; i < k_list->n; ++i)
    ja(&result, &kJ(k_list)[i]);

  return result;
}
