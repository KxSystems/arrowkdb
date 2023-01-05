#include <memory>
#include <unordered_map>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/pretty_print.h>
#include <arrow/util/decimal.h>

#include "ArrayReader.h"
#include "ArrayWriter.h"
#include "DatatypeStore.h"
#include "HelperFunctions.h"
#include "TypeCheck.h"

using namespace std;
using namespace kx::arrowkdb;

namespace {

// An arrow list array is a nested set of child lists.  This is represented in
// kdb as a mixed list for the parent list array containing a set of sub-lists,
// one for each of the list value sets.
template <typename ListArrayType>
void AppendList(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  for (auto i = 0; i < array_data->length(); ++i) {
    // Slice the parent array to get the list value set at the specified index
    auto value_slice = static_pointer_cast<ListArrayType>(array_data)->value_slice(i);

    // Recursively populate the kdb parent mixed list from that slice
    kK(k_array)[index++] = ReadArray(value_slice, type_overrides);
  }
}

// An arrow map array is a nested set of key/item paired child arrays.  This is
// represented in kdb as a mixed list for the parent map array, with a
// dictionary for each map value set.
void AppendMap(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto map_array = static_pointer_cast<arrow::MapArray>(array_data);
  auto keys = map_array->keys();
  auto items = map_array->items();
  for (auto i = 0; i < array_data->length(); ++i) {
    // Slice the parent key/items arrays to get the map value set child list at
    // the specified index
    auto keys_slice = keys->Slice(map_array->value_offset(i), map_array->value_length(i));
    auto items_slice = items->Slice(map_array->value_offset(i), map_array->value_length(i));
    // Recursively populate the kdb parent mixed list with a dictionary
    // populated from those slices
    kK(k_array)[index++] = xD(ReadArray(keys_slice, type_overrides), ReadArray(items_slice, type_overrides));
  }
}

// An arrow struct array is a logical grouping of child arrays with each child
// array corresponding to one of the fields in the struct.  A single struct
// value is obtaining by slicing across all the child arrays at a given index.
// This is represented in kdb as a mixed list for the parent struct array,
// containing child lists for each field in the struct.
void AppendStruct(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto struct_array = static_pointer_cast<arrow::StructArray>(array_data);
  auto num_fields = struct_array->type()->num_fields();
  for (auto i = 0; i < num_fields; ++i) {
    auto field_array = struct_array->field(i);
    // Only advance the index into the kdb mixed list at the end once all child
    // lists have been populated from the same initial index
    auto temp_index = index;
    AppendArray(field_array, kK(k_array)[i], temp_index, type_overrides);
  }
  index += array_data->length();
}

// An arrow union array is similar to a struct array except that it has an
// additional type id array which identifies the live field in each union value
// set.
void AppendUnion(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto union_array = static_pointer_cast<arrow::UnionArray>(array_data);

  // The type_id array is represented as a KH list at the start of the parent mixed list. 
  K type_ids = kK(k_array)[0];
  auto temp_type_index = index;
  for (auto i = 0; i < union_array->length(); ++i)
    kH(type_ids)[temp_type_index++] = union_array->child_id(i);

  auto num_fields = union_array->type()->num_fields();
  for (auto i = 0; i < num_fields; ++i) {
    auto field_array = union_array->field(i);
    // Only advance the index into the kdb mixed list at the end once all child
    // lists have been populated from the same initial index
    auto temp_index = index;
    AppendArray(field_array, kK(k_array)[i + 1], temp_index, type_overrides);
  }
  index += array_data->length();
}

// An arrow dictionary array is represented in kdb as a mixed list for the
// parent dictionary array containing the values and indicies sub-lists.
void AppendDictionary(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dictionary_array = static_pointer_cast<arrow::DictionaryArray>(array_data);

  // Append the dictionary and indicies arrays.  Have to use a join since the
  // two child arrays could be a different length to each other and the parent
  // dictionary array which makes it difficult to preallocate the kdb lists of
  // the correct length.
  K values = ReadArray(dictionary_array->dictionary(), type_overrides);
  jv(&kK(k_array)[0], values);
  K indices = ReadArray(dictionary_array->indices(), type_overrides);
  jv(&kK(k_array)[1], indices);
}

void AppendArray_NA(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto null_array = static_pointer_cast<arrow::NullArray>(array_data);
  for (auto i = 0; i < null_array->length(); ++i)
    kK(k_array)[index++] = knk(0);
}

void AppendArray_BOOL(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto bool_array = static_pointer_cast<arrow::BooleanArray>(array_data);
  // BooleanArray doesn't have a bulk reader since arrow BooleanType is only 1 bit
  for (auto i = 0; i < bool_array->length(); ++i)
    kG(k_array)[index++] = bool_array->Value(i);
}

void AppendArray_UINT8(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto uint8_array = static_pointer_cast<arrow::UInt8Array>(array_data);
  memcpy(kG(k_array), uint8_array->raw_values(), uint8_array->length() * sizeof(arrow::UInt8Array::value_type));
}

void AppendArray_INT8(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto int8_array = static_pointer_cast<arrow::Int8Array>(array_data);
  memcpy(kG(k_array), int8_array->raw_values(), int8_array->length() * sizeof(arrow::Int8Array::value_type));
}

void AppendArray_UINT16(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto uint16_array = static_pointer_cast<arrow::UInt16Array>(array_data);
  memcpy(kH(k_array), uint16_array->raw_values(), uint16_array->length() * sizeof(arrow::UInt16Array::value_type));
}

void AppendArray_INT16(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto int16_array = static_pointer_cast<arrow::Int16Array>(array_data);
  memcpy(kH(k_array), int16_array->raw_values(), int16_array->length() * sizeof(arrow::Int16Array::value_type));
}

void AppendArray_UINT32(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto uint32_array = static_pointer_cast<arrow::UInt32Array>(array_data);
  memcpy(kI(k_array), uint32_array->raw_values(), uint32_array->length() * sizeof(arrow::UInt32Array::value_type));
}

void AppendArray_INT32(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto int32_array = static_pointer_cast<arrow::Int32Array>(array_data);
  memcpy(kI(k_array), int32_array->raw_values(), int32_array->length() * sizeof(arrow::Int32Array::value_type));
}

void AppendArray_UINT64(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto uint64_array = static_pointer_cast<arrow::UInt64Array>(array_data);
  memcpy(kJ(k_array), uint64_array->raw_values(), uint64_array->length() * sizeof(arrow::UInt64Array::value_type));
}

void AppendArray_INT64(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto int64_array = static_pointer_cast<arrow::Int64Array>(array_data);
  memcpy(kJ(k_array), int64_array->raw_values(), int64_array->length() * sizeof(arrow::Int64Array::value_type));
}

void AppendArray_HALF_FLOAT(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto hfl_array = static_pointer_cast<arrow::HalfFloatArray>(array_data);
  memcpy(kH(k_array), hfl_array->raw_values(), hfl_array->length() * sizeof(arrow::HalfFloatArray::value_type));
}

void AppendArray_FLOAT(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto fl_array = static_pointer_cast<arrow::FloatArray>(array_data);
  memcpy(kE(k_array), fl_array->raw_values(), fl_array->length() * sizeof(arrow::FloatArray::value_type));
}

void AppendArray_DOUBLE(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dbl_array = static_pointer_cast<arrow::DoubleArray>(array_data);
  memcpy(kF(k_array), dbl_array->raw_values(), dbl_array->length() * sizeof(arrow::DoubleArray::value_type));
}

void AppendArray_STRING(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto str_array = static_pointer_cast<arrow::StringArray>(array_data);
  for (auto i = 0; i < str_array->length(); ++i) {
    auto str_data = str_array->GetString(i);
    K k_str = ktn(KC, str_data.length());
    memcpy(kG(k_str), str_data.data(), str_data.length());
    kK(k_array)[index++] = k_str;
  }
}

void AppendArray_LARGE_STRING(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto str_array = static_pointer_cast<arrow::LargeStringArray>(array_data);
  for (auto i = 0; i < str_array->length(); ++i) {
    auto str_data = str_array->GetString(i);
    K k_str = ktn(KC, str_data.length());
    memcpy(kG(k_str), str_data.data(), str_data.length());
    kK(k_array)[index++] = k_str;
  }
}

void AppendArray_BINARY(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto bin_array = static_pointer_cast<arrow::BinaryArray>(array_data);
  for (auto i = 0; i < bin_array->length(); ++i) {
    auto bin_data = bin_array->GetString(i);
    K k_bin = ktn(KG, bin_data.length());
    memcpy(kG(k_bin), bin_data.data(), bin_data.length());
    kK(k_array)[index++] = k_bin;
  }
}

void AppendArray_LARGE_BINARY(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto bin_array = static_pointer_cast<arrow::LargeBinaryArray>(array_data);
  for (auto i = 0; i < bin_array->length(); ++i) {
    auto bin_data = bin_array->GetString(i);
    K k_bin = ktn(KG, bin_data.length());
    memcpy(kG(k_bin), bin_data.data(), bin_data.length());
    kK(k_array)[index++] = k_bin;
  }
}

void AppendArray_FIXED_SIZE_BINARY(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto fixed_bin_array = static_pointer_cast<arrow::FixedSizeBinaryArray>(array_data);
  for (auto i = 0; i < fixed_bin_array->length(); ++i) {
    auto bin_data = fixed_bin_array->GetString(i);
    K k_bin = ktn(KG, bin_data.length());
    memcpy(kG(k_bin), bin_data.data(), bin_data.length());
    kK(k_array)[index++] = k_bin;
  }
}

void AppendArray_DATE32(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto d32_array = static_pointer_cast<arrow::Date32Array>(array_data);
  for (auto i = 0; i < d32_array->length(); ++i)
    kI(k_array)[index++] = tc.ArrowToKdb(d32_array->Value(i));
}

void AppendArray_DATE64(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto d64_array = static_pointer_cast<arrow::Date64Array>(array_data);
  for (auto i = 0; i < d64_array->length(); ++i)
    kJ(k_array)[index++] = tc.ArrowToKdb(d64_array->Value(i));
}

void AppendArray_TIMESTAMP(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto ts_array = static_pointer_cast<arrow::TimestampArray>(array_data);
  auto timestamp_type = static_pointer_cast<arrow::TimestampType>(ts_array->type());
  for (auto i = 0; i < ts_array->length(); ++i)
    kJ(k_array)[index++] = tc.ArrowToKdb(ts_array->Value(i));
}

void AppendArray_TIME32(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto t32_array = static_pointer_cast<arrow::Time32Array>(array_data);
  auto time32_type = static_pointer_cast<arrow::Time32Type>(t32_array->type());
  for (auto i = 0; i < t32_array->length(); ++i)
    kI(k_array)[index++] = tc.ArrowToKdb(t32_array->Value(i));
}

void AppendArray_TIME64(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto t64_array = static_pointer_cast<arrow::Time64Array>(array_data);
  auto time64_type = static_pointer_cast<arrow::Time64Type>(t64_array->type());
  for (auto i = 0; i < t64_array->length(); ++i)
    kJ(k_array)[index++] = tc.ArrowToKdb(t64_array->Value(i));
}

void AppendArray_DECIMAL(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dec_array = static_pointer_cast<arrow::Decimal128Array>(array_data);
  auto dec_type = static_pointer_cast<arrow::Decimal128Type>(dec_array->type());
  for (auto i = 0; i < dec_array->length(); ++i) {
    auto decimal = arrow::Decimal128(dec_array->Value(i));
    if (type_overrides.decimal128_as_double) {
      // Convert the decimal to a double
      auto dec_as_double = decimal.ToDouble(dec_type->scale());
      kF(k_array)[index++] = dec_as_double;
    } else {
      // Each decimal is a list of 16 bytes
      K k_dec = ktn(KG, 16);
      decimal.ToBytes(kG(k_dec));
      kK(k_array)[index++] = k_dec;
    }
  }
}

void AppendArray_DURATION(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto dur_array = static_pointer_cast<arrow::DurationArray>(array_data);
  auto duration_type = static_pointer_cast<arrow::DurationType>(dur_array->type());
  for (auto i = 0; i < dur_array->length(); ++i)
    kJ(k_array)[index++] = tc.ArrowToKdb(dur_array->Value(i));
}

void AppendArray_INTERVAL_MONTHS(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto month_array = static_pointer_cast<arrow::MonthIntervalArray>(array_data);
  memcpy(kI(k_array), month_array->raw_values(), month_array->length() * sizeof(arrow::MonthIntervalArray::value_type));
}

void AppendArray_INTERVAL_DAY_TIME(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dt_array = static_pointer_cast<arrow::DayTimeIntervalArray>(array_data);
  for (auto i = 0; i < dt_array->length(); ++i)
    kJ(k_array)[index++] = DayTimeInterval_KTimespan(dt_array->Value(i));
}

void AppendArray_LIST(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::ListArray>(array_data, k_array, index, type_overrides);
}

void AppendArray_LARGE_LIST(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::LargeListArray>(array_data, k_array, index, type_overrides);
}

void AppendArray_FIXED_SIZE_LIST(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::FixedSizeListArray>(array_data, k_array, index, type_overrides);
}

void AppendArray_MAP(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendMap(array_data, k_array, index, type_overrides);
}

void AppendArray_STRUCT(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendStruct(array_data, k_array, index, type_overrides);
}

void AppendArray_SPARSE_UNION(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendUnion(array_data, k_array, index, type_overrides);
}

void AppendArray_DENSE_UNION(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendArray_SPARSE_UNION(array_data, k_array, index, type_overrides);
}

void AppendArray_DICTIONARY(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendDictionary(array_data, k_array, index, type_overrides);
}

using ArrayHandler = void (*) (shared_ptr<arrow::Array>, K, size_t&, TypeMappingOverride&);

unordered_map<arrow::Type::type, ArrayHandler>  ArrayHandlers {
    make_pair( arrow::Type::NA, &AppendArray_NA )
  , make_pair( arrow::Type::BOOL, &AppendArray_BOOL )
  , make_pair( arrow::Type::UINT8, &AppendArray_UINT8 )
  , make_pair( arrow::Type::INT8, &AppendArray_INT8 )
  , make_pair( arrow::Type::UINT16, &AppendArray_UINT16 )
  , make_pair( arrow::Type::INT16, &AppendArray_INT16 )
  , make_pair( arrow::Type::UINT32, &AppendArray_UINT32 )
  , make_pair( arrow::Type::INT32, &AppendArray_INT32 )
  , make_pair( arrow::Type::UINT64, &AppendArray_UINT64 )
  , make_pair( arrow::Type::INT64, &AppendArray_INT64 )
  , make_pair( arrow::Type::HALF_FLOAT, &AppendArray_HALF_FLOAT )
  , make_pair( arrow::Type::FLOAT, &AppendArray_FLOAT )
  , make_pair( arrow::Type::DOUBLE, &AppendArray_DOUBLE )
  , make_pair( arrow::Type::STRING, &AppendArray_STRING )
  , make_pair( arrow::Type::LARGE_STRING, &AppendArray_LARGE_STRING )
  , make_pair( arrow::Type::BINARY, &AppendArray_BINARY )
  , make_pair( arrow::Type::LARGE_BINARY, &AppendArray_LARGE_BINARY )
  , make_pair( arrow::Type::FIXED_SIZE_BINARY, &AppendArray_FIXED_SIZE_BINARY )
  , make_pair( arrow::Type::DATE32, &AppendArray_DATE32 )
  , make_pair( arrow::Type::DATE64, &AppendArray_DATE64 )
  , make_pair( arrow::Type::TIMESTAMP, &AppendArray_TIMESTAMP )
  , make_pair( arrow::Type::TIME32, &AppendArray_TIME32 )
  , make_pair( arrow::Type::TIME64, &AppendArray_TIME64 )
  , make_pair( arrow::Type::DECIMAL, &AppendArray_DECIMAL )
  , make_pair( arrow::Type::DURATION, &AppendArray_DURATION )
  , make_pair( arrow::Type::INTERVAL_MONTHS, &AppendArray_INTERVAL_MONTHS )
  , make_pair( arrow::Type::INTERVAL_DAY_TIME, &AppendArray_INTERVAL_DAY_TIME )
  , make_pair( arrow::Type::LIST, &AppendArray_LIST )
  , make_pair( arrow::Type::LARGE_LIST, &AppendArray_LARGE_LIST )
  , make_pair( arrow::Type::FIXED_SIZE_LIST, &AppendArray_FIXED_SIZE_LIST )
  , make_pair( arrow::Type::MAP, &AppendArray_MAP )
  , make_pair( arrow::Type::STRUCT, &AppendArray_STRUCT )
  , make_pair( arrow::Type::SPARSE_UNION, &AppendArray_SPARSE_UNION )
  , make_pair( arrow::Type::DENSE_UNION, &AppendArray_DENSE_UNION )
  , make_pair( arrow::Type::DICTIONARY, &AppendArray_DICTIONARY )
};

} // namespace

namespace kx {
namespace arrowkdb {

void AppendArray(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto type_id = array_data->type_id();
  if( ArrayHandlers.find( type_id ) == ArrayHandlers.end() )
  {
    TYPE_CHECK_UNSUPPORTED(array_data->type()->ToString());
  }
  else
  {
    ArrayHandlers[type_id]( array_data, k_array, index, type_overrides );
  }
}

K InitKdbForArray(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides)
{
  switch (datatype->id()) {
  case arrow::Type::STRUCT: 
  {
    // Arrow struct becomes a mixed list of lists so create necessary lists
    auto num_fields = datatype->num_fields();
    K result = knk(num_fields);
    for (auto i = 0; i < num_fields; ++i) {
      auto field = datatype->field(i);
      kK(result)[i] = InitKdbForArray(field->type(), length, type_overrides);
    }
    return result;
  }
  case arrow::Type::SPARSE_UNION: 
  case arrow::Type::DENSE_UNION: 
  {
    // Arrow union becomes a mixed list of type_id list plus the child lists
    auto num_fields = datatype->num_fields();
    K result = knk(num_fields + 1);
    kK(result)[0] = ktn(KH, length); // type_id list
    for (auto i = 0; i < num_fields; ++i) {
      auto field = datatype->field(i);
      kK(result)[i + 1] = InitKdbForArray(field->type(), length, type_overrides);
    }
    return result;
  }
  case arrow::Type::DICTIONARY:
  {
    // Arrow dictionary becomes a two item mixed list
    auto dictionary_type = static_pointer_cast<arrow::DictionaryType>(datatype);
    K result = ktn(0, 2);

    // Do not preallocate the child lists since AppendDictionary has to join to the
    // indicies and values lists
    kK(result)[0] = InitKdbForArray(dictionary_type->value_type(), 0, type_overrides);
    kK(result)[1] = InitKdbForArray(dictionary_type->index_type(), 0, type_overrides);

    return result;
  }
  default:
    return ktn(GetKdbType(datatype, type_overrides), length);
  }
}

K ReadArray(shared_ptr<arrow::Array> array, TypeMappingOverride& type_overrides)
{
  K k_array = InitKdbForArray(array->type(), array->length(), type_overrides);
  size_t index = 0;
  AppendArray(array, k_array, index, type_overrides);
  return k_array;
}

K ReadChunkedArray(shared_ptr<arrow::ChunkedArray> chunked_array, TypeMappingOverride& type_overrides)
{
  K k_array = InitKdbForArray(chunked_array->type(), chunked_array->length(), type_overrides);
  size_t index = 0;
  for (auto j = 0; j < chunked_array->num_chunks(); ++j)
    AppendArray(chunked_array->chunk(j), k_array, index, type_overrides);
  return k_array;
}

} // namespace arrowkdb
} // namspace kx


K writeReadArray(K datatype_id, K array, K options)
{
  KDB_EXCEPTION_TRY;

  if (datatype_id->t != -KI)
    return krr((S)"datatype_id not -6h");

  auto datatype = GetDatatypeStore()->Find(datatype_id->i);
  if (!datatype)
    return krr((S)"datatype not found");

  // Parse the options
  auto read_options = KdbOptions(options, Options::string_options, Options::int_options);

  // Type mapping overrides
  TypeMappingOverride type_overrides{ read_options };

  auto arrow_array = MakeArray(datatype, array, type_overrides);

  return ReadArray(arrow_array, type_overrides);

  KDB_EXCEPTION_CATCH;
}
