#include <vector>
#include <memory>
#include <iostream>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/pretty_print.h>

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>

#include "TableData.h"
#include "HelperFunctions.h"
#include "SchemaStore.h"
#include "FieldStore.h"
#include "DatatypeStore.h"
#include "ArrayWriter.h"
#include "ArrayReader.h"
#include "TypeCheck.h"
#include "KdbOptions.h"


// @@@ 
// It is possible to check a loaded schema (from parquet file/arrow file/arrow
// stream) to see if any of the fields have been defined as nullable. But what do you do
// with nullable fields in externlly loaded schemas: nothing, warning, error?
// See also field() constuctor.
bool SchemaContainsNullable(const std::shared_ptr<arrow::Schema> schema)
{
  for (auto i : schema->fields())
    if (i->nullable()) {
      // std::cout << "Warning: schema contains nullable fields" << std::endl;
      return true;
    }

  return false;
}

// Create a vector of arrow arrays from the arrow schema and mixed list of kdb array objects
std::vector<std::shared_ptr<arrow::Array>> MakeArrays(std::shared_ptr<arrow::Schema> schema, K array_data)
{
  if (array_data->t != 0)
    throw kx::arrowkdb::TypeCheck("array_data not mixed list");
  if (array_data->n < schema->num_fields())
    throw kx::arrowkdb::TypeCheck("array_data length less than number of schema fields");

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  if (array_data->t == 0 && array_data->n == 0) {
    // Empty table
  } else {
    // Only count up to the number of schema fields.  Additional trailing data 
    // in the kdb mixed list is ignored (to allow for ::)
    for (auto i = 0; i < schema->num_fields(); ++i) {
      auto k_array = kK(array_data)[i];
      arrays.push_back(kx::arrowkdb::MakeArray(schema->field(i)->type(), k_array));
    }
  }

  return arrays;
}

// Create a an arrow table from the arrow schema and mixed list of kdb array objects
std::shared_ptr<arrow::Table> MakeTable(std::shared_ptr<arrow::Schema> schema, K array_data)
{
  return arrow::Table::Make(schema, MakeArrays(schema, array_data));
}

K prettyPrintTable(K schema_id, K array_data)
{
  KDB_EXCEPTION_TRY;

  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  auto table = MakeTable(schema, array_data);

  return kp((S)table->ToString().c_str());

  KDB_EXCEPTION_CATCH;
}

K writeReadTable(K schema_id, K array_data)
{
  KDB_EXCEPTION_TRY;

  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  auto table = MakeTable(schema, array_data);

  const auto col_num = table->num_columns();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i)
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(table->column(i));

  return data;

  KDB_EXCEPTION_CATCH;
}

K writeParquet(K parquet_file, K schema_id, K array_data, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  const auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
    outfile,
    arrow::io::FileOutputStream::Open(kx::arrowkdb::GetKdbString(parquet_file)));

  // Create the arrow table
  auto table = MakeTable(schema, array_data);

  // Parse the options
  auto write_options = kx::arrowkdb::KdbOptions(options);
  int64_t parquet_chunk_size = 1024 * 1024; // default to 1MB
  write_options.GetIntOption("parquet_chunk_size", parquet_chunk_size);

  // Set writer properties
  parquet::WriterProperties::Builder parquet_props_builder;
  parquet::ArrowWriterProperties::Builder arrow_props_builder;

  // Parquet version
  std::string parquet_version;
  write_options.GetStringOption("parquet_version", parquet_version);
  if (parquet_version == "V2.0") {
    parquet_props_builder.version(parquet::ParquetVersion::PARQUET_2_0);
    parquet_props_builder.data_page_version(parquet::ParquetDataPageVersion::V2);
  } else {
    // Not using v2.0 so map timestamp(ns) to timestamp(us) with truncation
    arrow_props_builder.coerce_timestamps(arrow::TimeUnit::MICRO);
    arrow_props_builder.allow_truncated_timestamps();
  }

  auto parquet_props = parquet_props_builder.build();
  auto arrow_props = arrow_props_builder.build();

  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, parquet_chunk_size, parquet_props, arrow_props));

  return (K)0;

  KDB_EXCEPTION_CATCH;
}

K readParquetSchema(K parquet_file)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");

  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
    infile,
    arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(parquet_file),
      arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Schema> schema;
  PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));
  
  // Add each field from the table to the field store
  // Add each datatype from the table to the datatype store
  //const auto schema = table->schema();
  SchemaContainsNullable(schema);
  for (auto field : schema->fields()) {
    kx::arrowkdb::GetFieldStore()->Add(field);
    kx::arrowkdb::GetDatatypeStore()->Add(field->type());
  }

  // Return the new schema_id
  return ki(kx::arrowkdb::GetSchemaStore()->Add(schema));

  KDB_EXCEPTION_CATCH;
}

K readParquetData(K parquet_file, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");

  auto read_options = kx::arrowkdb::KdbOptions(options);
  int64_t parquet_multithreaded_read = 0;
  read_options.GetIntOption("parquet_multithreaded_read", parquet_multithreaded_read);
  int64_t use_mmap = 0;
  read_options.GetIntOption("use_mmap", use_mmap);

  std::shared_ptr<arrow::io::RandomAccessFile> infile;
  if (use_mmap) {
    PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::MemoryMappedFile::Open(kx::arrowkdb::GetKdbString(parquet_file),
        arrow::io::FileMode::READ));
  } else {
    PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(parquet_file),
        arrow::default_memory_pool()));
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  reader->set_use_threads(parquet_multithreaded_read);

  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

  const auto schema = table->schema();
  SchemaContainsNullable(schema);
  const auto col_num = schema->num_fields();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i) {
    auto chunked_array = table->column(i);
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array);
  }

  return data;

  KDB_EXCEPTION_CATCH;
}

K readParquetColumn(K parquet_file, K column_index)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");
  if (column_index->t != -KI)
    return krr((S)"column not -6h");

  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
    infile,
    arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(parquet_file),
      arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  std::shared_ptr<::arrow::ChunkedArray> chunked_array;
  PARQUET_THROW_NOT_OK(reader->ReadColumn(column_index->i, &chunked_array));

  return kx::arrowkdb::ReadChunkedArray(chunked_array);

  KDB_EXCEPTION_CATCH;
}

K writeArrow(K arrow_file, K schema_id, K array_data)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(arrow_file))
    return krr((S)"arrow_file not 11h or 0 of 10h");
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  const auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
    outfile,
    arrow::io::FileOutputStream::Open(kx::arrowkdb::GetKdbString(arrow_file)));

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  PARQUET_ASSIGN_OR_THROW(writer, arrow::ipc::MakeFileWriter(outfile.get(), schema));

  auto arrays = MakeArrays(schema, array_data);

  // Check all arrays are same length
  int64_t len = -1;
  for (auto i : arrays) {
    if (len == -1)
      len = i->length();
    else if (len != i->length())
      return krr((S)"unequal length arrays");
  }

  auto batch = arrow::RecordBatch::Make(schema, len, arrays);
  PARQUET_THROW_NOT_OK(writer->WriteRecordBatch(*batch));

  PARQUET_THROW_NOT_OK(writer->Close());

  return (K)0;

  KDB_EXCEPTION_CATCH;
}

K readArrowSchema(K arrow_file)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(arrow_file))
    return krr((S)"arrow_file not 11h or 0 of 10h");

  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
    infile,
    arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(arrow_file),
      arrow::default_memory_pool()));

  std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
  PARQUET_ASSIGN_OR_THROW(reader, arrow::ipc::RecordBatchFileReader::Open(infile));

  // Add each field from the table to the field store
  // Add each datatype from the table to the datatype store
  const auto schema = reader->schema();
  SchemaContainsNullable(schema);
  for (auto field : schema->fields()) {
    kx::arrowkdb::GetFieldStore()->Add(field);
    kx::arrowkdb::GetDatatypeStore()->Instance()->Add(field->type());
  }

  // Return the new schema_id
  return ki(kx::arrowkdb::GetSchemaStore()->Add(schema));

  KDB_EXCEPTION_CATCH;
}

K readArrowData(K arrow_file, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(arrow_file))
    return krr((S)"arrow_file not 11h or 0 of 10h");

  auto read_options = kx::arrowkdb::KdbOptions(options);
  int64_t use_mmap = 0;
  read_options.GetIntOption("use_mmap", use_mmap);

  std::shared_ptr<arrow::io::RandomAccessFile> infile;
  if (use_mmap) {
    PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::MemoryMappedFile::Open(kx::arrowkdb::GetKdbString(arrow_file), 
        arrow::io::FileMode::READ));
  } else {
    PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(arrow_file),
        arrow::default_memory_pool()));
  }

  std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
  PARQUET_ASSIGN_OR_THROW(reader, arrow::ipc::RecordBatchFileReader::Open(infile));

  // Get all the record batches in advance
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  for (auto i = 0; i < reader->num_record_batches(); ++i) {
    std::shared_ptr<arrow::RecordBatch> batch;
    PARQUET_ASSIGN_OR_THROW(batch, reader->ReadRecordBatch(i));
    all_batches.push_back(batch);
  }

  // Created a chunked array for each column by amalgamating each column's
  // arrays across all batches
  const auto schema = reader->schema();
  SchemaContainsNullable(schema);
  const auto col_num = schema->num_fields();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i) {
    arrow::ArrayVector column_arrays;
    for (auto batch : all_batches)
      column_arrays.push_back(batch->column(i));
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(column_arrays);
    // Convert the chunked array to kdb object
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array);
  }

  return data;

  KDB_EXCEPTION_CATCH;
}

K serializeArrow(K schema_id, K array_data)
{
  KDB_EXCEPTION_TRY;

  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  const auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  std::shared_ptr<arrow::ResizableBuffer> buffer;
  std::unique_ptr<arrow::io::BufferOutputStream> sink;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  PARQUET_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(0));
  sink.reset(new arrow::io::BufferOutputStream(buffer));
  PARQUET_ASSIGN_OR_THROW(writer, arrow::ipc::MakeStreamWriter(sink.get(), schema));

  auto arrays = MakeArrays(schema, array_data);

  // Check all arrays are same length
  int64_t len = -1;
  for (auto i : arrays) {
    if (len == -1)
      len = i->length();
    else if (len != i->length())
      return krr((S)"unequal length arrays");
  }

  auto batch = arrow::RecordBatch::Make(schema, len, arrays);
  PARQUET_THROW_NOT_OK(writer->WriteRecordBatch(*batch));

  PARQUET_THROW_NOT_OK(writer->Close());
  std::shared_ptr<arrow::Buffer> final_buffer;
  PARQUET_ASSIGN_OR_THROW(final_buffer, sink->Finish());

  K result = ktn(KG, final_buffer->size());
  memcpy(kG(result), buffer->data(), buffer->size());;

  return result;

  KDB_EXCEPTION_CATCH;
}

K parseArrowSchema(K char_array)
{
  KDB_EXCEPTION_TRY;

  if (char_array->t != KG && char_array->t != KC)
    return krr((S)"char_array not 4|10h");

  auto buf_reader = std::make_shared<arrow::io::BufferReader>(kG(char_array), char_array->n);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  PARQUET_ASSIGN_OR_THROW(reader, arrow::ipc::RecordBatchStreamReader::Open(buf_reader));

  // Add each field from the table to the field store
  // Add each datatype from the table to the datatype store
  const auto schema = reader->schema();
  SchemaContainsNullable(schema);
  for (auto field : schema->fields()) {
    kx::arrowkdb::GetFieldStore()->Add(field);
    kx::arrowkdb::GetDatatypeStore()->Add(field->type());
  }

  // Return the new schema_id
  return ki(kx::arrowkdb::GetSchemaStore()->Add(schema));

  KDB_EXCEPTION_CATCH;
}

K parseArrowData(K char_array)
{
  KDB_EXCEPTION_TRY;

  if (char_array->t != KG && char_array->t != KC)
    return krr((S)"char_array not 4|10h");

  auto buf_reader = std::make_shared<arrow::io::BufferReader>(kG(char_array), char_array->n);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  PARQUET_ASSIGN_OR_THROW(reader, arrow::ipc::RecordBatchStreamReader::Open(buf_reader));

  // Get all the record batches in advance
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  PARQUET_THROW_NOT_OK(reader->ReadAll(&all_batches));

  // Created a chunked array for each column by amalgamating each column's
  // arrays across all batches
  auto schema = reader->schema();
  SchemaContainsNullable(schema);
  auto col_num = schema->num_fields();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i) {
    arrow::ArrayVector column_arrays;
    for (auto batch : all_batches)
      column_arrays.push_back(batch->column(i));
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(column_arrays);
    // Convert the chunked array to kdb object
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array);
  }

  return data;

  KDB_EXCEPTION_CATCH;
}
