#include <vector>
#include <memory>
#include <iostream>

#ifndef _WIN32
#include <arrow/adapters/orc/adapter.h>
#endif

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/pretty_print.h>

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h>

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
std::vector<std::shared_ptr<arrow::Array>> MakeArrays(std::shared_ptr<arrow::Schema> schema, K array_data, kx::arrowkdb::TypeMappingOverride& type_overrides)
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
      arrays.push_back(kx::arrowkdb::MakeArray(schema->field(i)->type(), k_array, type_overrides));
    }
  }

  return arrays;
}

std::vector<std::shared_ptr<arrow::ChunkedArray>> MakeChunkedArrays(
      std::shared_ptr<arrow::Schema> schema
    , K array_data
    , kx::arrowkdb::TypeMappingOverride& type_overrides )
{
  if( array_data->t != 0 )
    throw kx::arrowkdb::TypeCheck( "array_data not mixed list" );
  if( array_data->n < schema->num_fields() )
    throw kx::arrowkdb::TypeCheck( "array_data length less than number of schema fields" );
  std::vector<std::shared_ptr<arrow::ChunkedArray>> chunked_arrays;
  if( array_data->t == 0 && array_data->n == 0 ){
    // Empty table
  }
  else{
    // Only count up to the number of schema fields.  Additional trailing data
    // in the kdb mixed list is ignored (to allow for ::)
    for( auto i = 0; i < schema->num_fields(); ++i ){
      auto k_array = kK( array_data )[i];
      chunked_arrays.push_back( kx::arrowkdb::MakeChunkedArray( schema->field(i)->type(), k_array, type_overrides ) );
    }
  }

  return chunked_arrays;
}

// Create a an arrow table from the arrow schema and mixed list of kdb array objects
std::shared_ptr<arrow::Table> MakeTable(std::shared_ptr<arrow::Schema> schema, K array_data, kx::arrowkdb::TypeMappingOverride& type_overrides)
{
  return arrow::Table::Make(schema, MakeArrays(schema, array_data, type_overrides));
}

K prettyPrintTable(K schema_id, K array_data, K options)
{
  KDB_EXCEPTION_TRY;

  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  auto table = MakeTable(schema, array_data, type_overrides);

  return kp((S)table->ToString().c_str());

  KDB_EXCEPTION_CATCH;
}

K writeReadTable(K schema_id, K array_data, K options)
{
  KDB_EXCEPTION_TRY;

  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  auto table = MakeTable(schema, array_data, type_overrides);

  const auto col_num = table->num_columns();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i)
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(table->column(i), type_overrides);

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

  // Parse the options
  auto write_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Chunk size
  int64_t parquet_chunk_size = 1024 * 1024; // default to 1MB
  write_options.GetIntOption(kx::arrowkdb::Options::PARQUET_CHUNK_SIZE, parquet_chunk_size);

  // Set writer properties
  parquet::WriterProperties::Builder parquet_props_builder;
  parquet::ArrowWriterProperties::Builder arrow_props_builder;

  // Parquet version
  std::string parquet_version;
  write_options.GetStringOption(kx::arrowkdb::Options::PARQUET_VERSION, parquet_version);
  if (parquet_version == "V2.0") {
    parquet_props_builder.version(parquet::ParquetVersion::PARQUET_2_0);
    parquet_props_builder.data_page_version(parquet::ParquetDataPageVersion::V2);
  } else if (parquet_version == "V2.4") {
    parquet_props_builder.version(parquet::ParquetVersion::PARQUET_2_4);
    parquet_props_builder.data_page_version(parquet::ParquetDataPageVersion::V2);
  } else if (parquet_version == "V2.6") {
    parquet_props_builder.version(parquet::ParquetVersion::PARQUET_2_6);
    parquet_props_builder.data_page_version(parquet::ParquetDataPageVersion::V2);
  } else if (parquet_version == "V2.LATEST") {
    parquet_props_builder.version(parquet::ParquetVersion::PARQUET_2_LATEST);
    parquet_props_builder.data_page_version(parquet::ParquetDataPageVersion::V2);
  } else {
    // Not using v2.0 so map timestamp(ns) to timestamp(us) with truncation
    arrow_props_builder.coerce_timestamps(arrow::TimeUnit::MICRO);
    arrow_props_builder.allow_truncated_timestamps();
  }

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ write_options };

  auto parquet_props = parquet_props_builder.build();
  auto arrow_props = arrow_props_builder.build();

  // Create the arrow table
  auto table = MakeTable(schema, array_data, type_overrides);

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

K readParquetNumRowGroups(K parquet_file)
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

  return ki(reader->num_row_groups());

  KDB_EXCEPTION_CATCH;
}

K readParquetData(K parquet_file, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Use multi threading
  int64_t parquet_multithreaded_read = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::PARQUET_MULTITHREADED_READ, parquet_multithreaded_read);

  // Use memmap
  int64_t use_mmap = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::USE_MMAP, use_mmap);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

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
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array, type_overrides);
  }

  int64_t with_null_bitmap = 0;
  read_options.GetIntOption( kx::arrowkdb::Options::WITH_NULL_BITMAP, with_null_bitmap );
  if( with_null_bitmap ){
    K bitmap = ktn( 0, col_num );
    for( auto i = 0; i < col_num; ++i ){
        auto chunked_array = table->column( i );
        kK( bitmap )[i] = kx::arrowkdb::ReadChunkedArrayNullBitmap( chunked_array, type_overrides );
    }
    K array = data;
    data = ktn( 0, 2 );
    kK( data )[0] = array;
    kK( data )[1] = bitmap;
  }

  return data;

  KDB_EXCEPTION_CATCH;
}

K readParquetColumn(K parquet_file, K column_index, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");
  if (column_index->t != -KI)
    return krr((S)"column not -6h");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
    infile,
    arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(parquet_file),
      arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  std::shared_ptr<::arrow::ChunkedArray> chunked_array;
  PARQUET_THROW_NOT_OK(reader->ReadColumn(column_index->i, &chunked_array));

  return kx::arrowkdb::ReadChunkedArray(chunked_array, type_overrides);

  KDB_EXCEPTION_CATCH;
}

K readParquetRowGroups(K parquet_file, K row_groups, K columns, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(parquet_file))
    return krr((S)"parquet_file not 11h or 0 of 10h");
  if (row_groups->t != 101 && row_groups->t != KI)
    return krr((S)"row_groups not 101h or 6h");
  if (columns->t != 101 && columns->t != KI)
    return krr((S)"columns not 101h or 6h");

  // Function to convert a KI to vector<int>
  auto ki_to_vector_int = [](K ki, std::vector<int>& vec) {
    for (auto i = 0; i < ki->n; ++i)
      vec.push_back(kI(ki)[i]);
  };
  
  // Convert row_groups and columns
  std::vector<int> rows;
  if (row_groups->t == KI) 
    ki_to_vector_int(row_groups, rows);
  std::vector<int> cols;
  if (columns->t == KI)
    ki_to_vector_int(columns, cols);

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Use multi threading
  int64_t parquet_multithreaded_read = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::PARQUET_MULTITHREADED_READ, parquet_multithreaded_read);

  // Use memmap
  int64_t use_mmap = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::USE_MMAP, use_mmap);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

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
  if (row_groups->t == 101 && columns->t == 101)
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  else if (row_groups->t == 101)
    PARQUET_THROW_NOT_OK(reader->ReadTable(cols, &table));
  else if (columns->t == 101)
    PARQUET_THROW_NOT_OK(reader->ReadRowGroups(rows, &table));
  else
    PARQUET_THROW_NOT_OK(reader->ReadRowGroups(rows, cols, &table));

  const auto schema = table->schema();
  SchemaContainsNullable(schema);
  const auto col_num = schema->num_fields();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i) {
    auto chunked_array = table->column(i);
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array, type_overrides);
  }

  int64_t with_null_bitmap = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::WITH_NULL_BITMAP, with_null_bitmap);
  if (with_null_bitmap) {
    K bitmap = ktn(0, col_num);
    for (auto i = 0; i < col_num; ++i) {
      auto chunked_array = table->column(i);
      kK(bitmap)[i] = kx::arrowkdb::ReadChunkedArrayNullBitmap(chunked_array, type_overrides);
    }
    K array = data;
    data = ktn(0, 2);
    kK(data)[0] = array;
    kK(data)[1] = bitmap;
  }

  return data;

  KDB_EXCEPTION_CATCH;
}

K writeArrow(K arrow_file, K schema_id, K array_data, K options)
{
  KDB_EXCEPTION_TRY;

  if (!kx::arrowkdb::IsKdbString(arrow_file))
    return krr((S)"arrow_file not 11h or 0 of 10h");
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  const auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
    outfile,
    arrow::io::FileOutputStream::Open(kx::arrowkdb::GetKdbString(arrow_file)));

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  PARQUET_ASSIGN_OR_THROW(writer, arrow::ipc::MakeFileWriter(outfile.get(), schema));

  // Chunk size
  read_options.GetIntOption( kx::arrowkdb::Options::ARROW_CHUNK_ROWS, type_overrides.chunk_length );

  auto check_length = []( const auto& arrays ) -> int64_t {
    // Check all arrays are same length
    int64_t len = -1;
    for (auto i : arrays) {
      if (len == -1)
        len = i->length();
      else if (len != i->length())
        return -1l;
    }

    return len;
  };

  if( !type_overrides.chunk_length ){ // arrow not chunked
    auto arrays = MakeArrays(schema, array_data, type_overrides);

    auto len = check_length( arrays );
    if( len < 0 ){
      return krr((S)"unequal length arrays");
    }

    auto batch = arrow::RecordBatch::Make(schema, len, arrays);
    PARQUET_THROW_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  else{
    auto chunked_arrays = MakeChunkedArrays( schema, array_data, type_overrides );

    auto len = check_length( chunked_arrays );
    if( len < 0 ){
      return krr((S)"unequal length arrays");
    }

    auto table = arrow::Table::Make( schema, chunked_arrays );
    PARQUET_THROW_NOT_OK( writer->WriteTable( *table ) );
  }

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

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Use memmap
  int64_t use_mmap = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::USE_MMAP, use_mmap);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

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
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array, type_overrides);
  }

  int64_t with_null_bitmap = 0;
  read_options.GetIntOption( kx::arrowkdb::Options::WITH_NULL_BITMAP, with_null_bitmap );
  if( with_null_bitmap ){
    K bitmap = ktn( 0, col_num );
    for( auto i = 0; i < col_num; ++i ){
        arrow::ArrayVector column_arrays;
        for( auto batch: all_batches )
          column_arrays.push_back( batch->column( i ) );
        auto chunked_array = std::make_shared<arrow::ChunkedArray>( column_arrays );
        kK( bitmap )[i] = kx::arrowkdb::ReadChunkedArrayNullBitmap( chunked_array, type_overrides );
    }
    K array = data;
    data = ktn( 0, 2 );
    kK( data )[0] = array;
    kK( data )[1] = bitmap;
  }

  return data;

  KDB_EXCEPTION_CATCH;
}

K serializeArrow(K schema_id, K array_data, K options)
{
  KDB_EXCEPTION_TRY;

  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  const auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  std::shared_ptr<arrow::ResizableBuffer> buffer;
  std::unique_ptr<arrow::io::BufferOutputStream> sink;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  PARQUET_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(0));
  sink.reset(new arrow::io::BufferOutputStream(buffer));
  PARQUET_ASSIGN_OR_THROW(writer, arrow::ipc::MakeStreamWriter(sink.get(), schema));

  // Chunk size
  read_options.GetIntOption( kx::arrowkdb::Options::ARROW_CHUNK_ROWS, type_overrides.chunk_length );

  auto check_length = []( const auto& arrays ) -> int64_t {
    // Check all arrays are same length
    int64_t len = -1;
    for (auto i : arrays) {
      if (len == -1)
        len = i->length();
      else if (len != i->length())
        return -1l;
    }

    return len;
  };

  if( !type_overrides.chunk_length ){ // arrow not chunked
    auto arrays = MakeArrays(schema, array_data, type_overrides);

    auto len = check_length( arrays );
    if( len < 0 ){
      return krr((S)"unequal length arrays");
    }

    auto batch = arrow::RecordBatch::Make(schema, len, arrays);
    PARQUET_THROW_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  else{
    auto chunked_arrays = MakeChunkedArrays( schema, array_data, type_overrides );

    auto len = check_length( chunked_arrays );
    if( len < 0 ){
      return krr((S)"unequal length arrays");
    }

    auto table = arrow::Table::Make( schema, chunked_arrays );
    PARQUET_THROW_NOT_OK( writer->WriteTable( *table ) );
  }

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

K parseArrowData(K char_array, K options)
{
  KDB_EXCEPTION_TRY;

  if (char_array->t != KG && char_array->t != KC)
    return krr((S)"char_array not 4|10h");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  auto buf_reader = std::make_shared<arrow::io::BufferReader>(kG(char_array), char_array->n);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  PARQUET_ASSIGN_OR_THROW(reader, arrow::ipc::RecordBatchStreamReader::Open(buf_reader));

  // Get all the record batches in advance
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  PARQUET_ASSIGN_OR_THROW(all_batches, reader->ToRecordBatches());

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
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array, type_overrides);
  }

  int64_t with_null_bitmap = 0;
  read_options.GetIntOption( kx::arrowkdb::Options::WITH_NULL_BITMAP, with_null_bitmap );
  if( with_null_bitmap ){
    K bitmap = ktn( 0, col_num );
    for( auto i = 0; i < col_num; ++i ){
        arrow::ArrayVector column_arrays;
        for( auto batch: all_batches )
          column_arrays.push_back( batch->column( i ) );
        auto chunked_array = std::make_shared<arrow::ChunkedArray>( column_arrays );
        kK( bitmap )[i] = kx::arrowkdb::ReadChunkedArrayNullBitmap( chunked_array, type_overrides );
    }
    K array = data;
    data = ktn( 0, 2 );
    kK( data )[0] = array;
    kK( data )[1] = bitmap;
  }

  return data;

  KDB_EXCEPTION_CATCH;
}

K readORCData(K orc_file, K options)
{
  KDB_EXCEPTION_TRY;

#ifdef _WIN32
  return krr((S)"ORC files are not supported on Windows");
#else
  if (!kx::arrowkdb::IsKdbString(orc_file))
    return krr((S)"orc_file not 11h or 0 of 10h");

  // Parse the options
  auto read_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);

  // Use memmap
  int64_t use_mmap = 0;
  read_options.GetIntOption(kx::arrowkdb::Options::USE_MMAP, use_mmap);

  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ read_options };

  std::shared_ptr<arrow::io::RandomAccessFile> infile;
  if (use_mmap) {
    PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::MemoryMappedFile::Open(kx::arrowkdb::GetKdbString(orc_file),
        arrow::io::FileMode::READ));
  } else {
    PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(orc_file),
        arrow::default_memory_pool()));
  }
  // Open ORC file reader
  auto maybe_reader = arrow::adapters::orc::ORCFileReader::Open(infile, arrow::default_memory_pool());

  std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader = std::move(maybe_reader.ValueOrDie());

  // Read entire file as a single Arrow table
  auto maybe_table = reader->Read();

  std::shared_ptr<arrow::Table> table = maybe_table.ValueOrDie();

  const auto schema = table->schema();
  SchemaContainsNullable(schema);
  const auto col_num = schema->num_fields();
  K data = ktn(0, col_num);
  for (auto i = 0; i < col_num; ++i) {
    auto chunked_array = table->column(i);
    kK(data)[i] = kx::arrowkdb::ReadChunkedArray(chunked_array, type_overrides);
  }

  int64_t with_null_bitmap = 0;
  read_options.GetIntOption( kx::arrowkdb::Options::WITH_NULL_BITMAP, with_null_bitmap );
  if( with_null_bitmap ){
    K bitmap = ktn( 0, col_num );
    for( auto i = 0; i < col_num; ++i ){
        auto chunked_array = table->column( i );
        kK( bitmap )[i] = kx::arrowkdb::ReadChunkedArrayNullBitmap( chunked_array, type_overrides );
    }
    K array = data;
    data = ktn( 0, 2 );
    kK( data )[0] = array;
    kK( data )[1] = bitmap;
  }

  return data;
#endif

  KDB_EXCEPTION_CATCH;
}

K readORCSchema(K orc_file)
{
  KDB_EXCEPTION_TRY;

#ifdef _WIN32
  return krr((S)"ORC files are not supported on Windows");
#else
  if (!kx::arrowkdb::IsKdbString(orc_file))
    return krr((S)"orc_file not 11h or 0 of 10h");

  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
    infile,
    arrow::io::ReadableFile::Open(kx::arrowkdb::GetKdbString(orc_file),
      arrow::default_memory_pool()));

  auto maybe_reader = arrow::adapters::orc::ORCFileReader::Open(infile, arrow::default_memory_pool());

  std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader = std::move(maybe_reader.ValueOrDie());

  auto maybe_schema = reader->ReadSchema();

  std::shared_ptr<arrow::Schema> schema = maybe_schema.ValueOrDie();
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
#endif

  KDB_EXCEPTION_CATCH;
}


K writeORC(K orc_file, K schema_id, K array_data, K options)
{
  KDB_EXCEPTION_TRY;

#ifdef _WIN32
  return krr((S)"ORC files are not supported on Windows");
#else
  if (!kx::arrowkdb::IsKdbString(orc_file))
    return krr((S)"orc_file not 11h or 0 of 10h");
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  const auto schema = kx::arrowkdb::GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"unknown schema");

  std::string path = kx::arrowkdb::GetKdbString( orc_file );
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
    outfile,
    arrow::io::FileOutputStream::Open( path ) );

  // Parse the options
  auto write_options = kx::arrowkdb::KdbOptions(options, kx::arrowkdb::Options::string_options, kx::arrowkdb::Options::int_options);
  
  int64_t orc_chunk_size = 1024*1024;
  write_options.GetIntOption(kx::arrowkdb::Options::ORC_CHUNK_SIZE, orc_chunk_size);

  auto used_write = arrow::adapters::orc::WriteOptions();
  used_write.batch_size = orc_chunk_size;

  auto maybe_writer = arrow::adapters::orc::ORCFileWriter::Open(outfile.get(), used_write);

  std::unique_ptr<arrow::adapters::orc::ORCFileWriter> writer = std::move(maybe_writer.ValueOrDie());


  // Type mapping overrides
  kx::arrowkdb::TypeMappingOverride type_overrides{ write_options };

  // Create the arrow table
  auto table = MakeTable(schema, array_data, type_overrides);

  PARQUET_THROW_NOT_OK( writer->Write( *table ) );

  PARQUET_THROW_NOT_OK( writer->Close() );

  return ( K )0;
#endif

  KDB_EXCEPTION_CATCH;
}
