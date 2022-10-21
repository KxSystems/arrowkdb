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

typedef K(*ReadArrayCommon)(std::shared_ptr<arrow::Array> array_data, TypeMappingOverride& type_overrides);
typedef void(*AppendArrayCommon)(std::shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides);

// An arrow list array is a nested set of child lists.  This is represented in
// kdb as a mixed list for the parent list array containing a set of sub-lists,
// one for each of the list value sets.
template <typename ListArrayType>
void AppendList(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides, ReadArrayCommon read_array)
{
  for (auto i = 0; i < array_data->length(); ++i) {
    // Slice the parent array to get the list value set at the specified index
    auto value_slice = static_pointer_cast<ListArrayType>(array_data)->value_slice(i);

    // Recursively populate the kdb parent mixed list from that slice
    kK(k_array)[index++] = read_array(value_slice, type_overrides);
  }
}

// An arrow map array is a nested set of key/item paired child arrays.  This is
// represented in kdb as a mixed list for the parent map array, with a
// dictionary for each map value set.
void AppendMap(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides, ReadArrayCommon read_array)
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
    kK(k_array)[index++] = xD(read_array(keys_slice, type_overrides), read_array(items_slice, type_overrides));
  }
}

// An arrow struct array is a logical grouping of child arrays with each child
// array corresponding to one of the fields in the struct.  A single struct
// value is obtaining by slicing across all the child arrays at a given index.
// This is represented in kdb as a mixed list for the parent struct array,
// containing child lists for each field in the struct.
void AppendStruct(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides, AppendArrayCommon append_array)
{
  auto struct_array = static_pointer_cast<arrow::StructArray>(array_data);
  auto num_fields = struct_array->type()->num_fields();
  for (auto i = 0; i < num_fields; ++i) {
    auto field_array = struct_array->field(i);
    // Only advance the index into the kdb mixed list at the end once all child
    // lists have been populated from the same initial index
    auto temp_index = index;
    append_array(field_array, kK(k_array)[i], temp_index, type_overrides);
  }
  index += array_data->length();
}

// An arrow union array is similar to a struct array except that it has an
// additional type id array which identifies the live field in each union value
// set.
void AppendUnion(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides, AppendArrayCommon append_array)
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
    append_array(field_array, kK(k_array)[i + 1], temp_index, type_overrides);
  }
  index += array_data->length();
}

// An arrow dictionary array is represented in kdb as a mixed list for the
// parent dictionary array containing the values and indicies sub-lists.
void AppendDictionary(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides, ReadArrayCommon read_array)
{
  auto dictionary_array = static_pointer_cast<arrow::DictionaryArray>(array_data);

  // Append the dictionary and indicies arrays.  Have to use a join since the
  // two child arrays could be a different length to each other and the parent
  // dictionary array which makes it difficult to preallocate the kdb lists of
  // the correct length.
  K values = read_array(dictionary_array->dictionary(), type_overrides);
  jv(&kK(k_array)[0], values);
  K indices = read_array(dictionary_array->indices(), type_overrides);
  jv(&kK(k_array)[1], indices);
}

template<arrow::Type::type TypeId>
void AppendArray(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides);

template<>
void AppendArray<arrow::Type::NA>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto null_array = static_pointer_cast<arrow::NullArray>(array_data);
  for (auto i = 0; i < null_array->length(); ++i)
    kK(k_array)[index++] = knk(0);
}

template<>
void AppendArray<arrow::Type::BOOL>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto bool_array = static_pointer_cast<arrow::BooleanArray>(array_data);
  // BooleanArray doesn't have a bulk reader since arrow BooleanType is only 1 bit
  for (auto i = 0; i < bool_array->length(); ++i){
    kG(k_array)[index++] = // preventing branch prediction failures
        ( ( type_overrides.null_mapping.have_boolean && bool_array->IsNull( i ) ) * type_overrides.null_mapping.boolean_null )
        + ( !( type_overrides.null_mapping.have_boolean && bool_array->IsNull( i ) ) * bool_array->Value( i ) );
  }
}

template<>
void AppendArray<arrow::Type::UINT8>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"UINT8");
  auto uint8_array = static_pointer_cast<arrow::UInt8Array>(array_data);
  auto length = uint8_array->length();
  if( type_overrides.null_mapping.have_uint8 && uint8_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kG( k_array )[i] = ( uint8_array->IsNull( i ) * type_overrides.null_mapping.uint8_null )
        + ( !uint8_array->IsNull( i ) * uint8_array->Value( i ) );
    }
  }
  else {
    memcpy( &kG( k_array )[index], uint8_array->raw_values(), length * sizeof( arrow::UInt8Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::INT8>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"INT8");
  auto int8_array = static_pointer_cast<arrow::Int8Array>(array_data);
  auto length = int8_array->length();
  if( type_overrides.null_mapping.have_int8 && int8_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kG( k_array )[i] = ( int8_array->IsNull( i ) * type_overrides.null_mapping.int8_null )
        + ( !int8_array->IsNull( i ) * int8_array->Value( i ) );
    }
  }
  else {
    memcpy( &kG( k_array )[index], int8_array->raw_values(), length * sizeof( arrow::Int8Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::UINT16>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"UINT16");
  auto uint16_array = static_pointer_cast<arrow::UInt16Array>(array_data);
  auto length = uint16_array->length();
  if( type_overrides.null_mapping.have_uint16 && uint16_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kH( k_array )[i] = ( uint16_array->IsNull( i ) * type_overrides.null_mapping.uint16_null )
        + ( !uint16_array->IsNull( i ) * uint16_array->Value( i ) );
    }
  }
  else {
    memcpy( &kH( k_array )[index], uint16_array->raw_values(), length * sizeof( arrow::UInt16Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::INT16>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"INT16");
  auto int16_array = static_pointer_cast<arrow::Int16Array>(array_data);
  auto length = int16_array->length();
  if( type_overrides.null_mapping.have_int16 && int16_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kH( k_array )[i] = ( int16_array->IsNull( i ) * type_overrides.null_mapping.int16_null )
        + ( !int16_array->IsNull( i ) * int16_array->Value( i ) );
    }
  }
  else {
    memcpy( &kH( k_array )[index], int16_array->raw_values(), length * sizeof( arrow::Int16Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::UINT32>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"UINT32");
  auto uint32_array = static_pointer_cast<arrow::UInt32Array>(array_data);
  auto length = uint32_array->length();
  if( type_overrides.null_mapping.have_uint32 && uint32_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kI( k_array )[i] = ( uint32_array->IsNull( i ) * type_overrides.null_mapping.uint32_null )
        + ( !uint32_array->IsNull( i ) * uint32_array->Value( i ) );
    }
  }
  else {
    memcpy( &kI( k_array )[index], uint32_array->raw_values(), length * sizeof( arrow::UInt32Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::INT32>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"INT32");
  auto int32_array = static_pointer_cast<arrow::Int32Array>(array_data);
  auto int32_type = std::static_pointer_cast<arrow::Int32Array>(int32_array->type());
  auto length = int32_array->length();
  if( type_overrides.null_mapping.have_int32 && int32_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kI( k_array )[i] = ( int32_array->IsNull( i ) * type_overrides.null_mapping.int32_null )
        + (!int32_array->IsNull( i ) * int32_array->Value( i ) );
    }
  }
  else {
    for (auto i = 0; i < int32_array->length(); ++i) {  
      // auto integer32 = arrow::int32(int32_array->Value(i)); // Doesn't work because it can't take arguments
      // K k_int = ktn(KG, 16); // Can change the second argument to 16 or 32 or 64 to test
      // integer32.ToBytes(kG(k_int));
      kI(k_array)[index++] = int32_array->Value(i); // Might have to change kI? - Check back on Decimal way 
    }
    // memcpy( &kI( k_array )[index], int32_array->raw_values(), length * sizeof( arrow::Int32Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::UINT64>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"UINT64");
  auto uint64_array = static_pointer_cast<arrow::UInt64Array>(array_data);
  auto length = uint64_array->length();
  if( type_overrides.null_mapping.have_uint64 && uint64_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kJ( k_array )[i] = ( uint64_array->IsNull( i ) * type_overrides.null_mapping.uint64_null )
        + ( !uint64_array->IsNull( i ) * uint64_array->Value( i ) );
    }
  }
  else {
    memcpy( &kJ( k_array )[index], uint64_array->raw_values(), length * sizeof( arrow::UInt64Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::INT64>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  // krr((S)"INT64");
  auto int64_array = static_pointer_cast<arrow::Int64Array>(array_data);
  auto length = int64_array->length();
  if( type_overrides.null_mapping.have_int64 && int64_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kJ( k_array )[i] = ( int64_array->IsNull( i ) * type_overrides.null_mapping.int64_null )
        + (!int64_array->IsNull( i ) * int64_array->Value( i ) );
    }
  }
  else {
    memcpy( &kJ( k_array )[index], int64_array->raw_values(), length * sizeof( arrow::Int64Array::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::HALF_FLOAT>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto hfl_array = static_pointer_cast<arrow::HalfFloatArray>(array_data);
  auto length = hfl_array->length();
  if( type_overrides.null_mapping.have_float16 && hfl_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kH( k_array )[i] = ( hfl_array->IsNull( i ) * type_overrides.null_mapping.float16_null )
        + ( !hfl_array->IsNull( i ) * hfl_array->Value( i ) );
    }
  }
  else {
    memcpy( &kH( k_array )[index], hfl_array->raw_values(), length * sizeof( arrow::HalfFloatArray::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::FLOAT>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto fl_array = static_pointer_cast<arrow::FloatArray>(array_data);
  auto length = fl_array->length();
  if( type_overrides.null_mapping.have_float32 && fl_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kE( k_array )[i] = ( fl_array->IsNull( i ) * type_overrides.null_mapping.float32_null )
        + ( !fl_array->IsNull( i ) * fl_array->Value( i ) );
    }
  }
  else {
    memcpy( &kE( k_array )[index], fl_array->raw_values(), length * sizeof( arrow::FloatArray::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::DOUBLE>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dbl_array = static_pointer_cast<arrow::DoubleArray>(array_data);
  auto length = dbl_array->length();
  if( type_overrides.null_mapping.have_float64 && dbl_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kF( k_array )[i] = ( dbl_array->IsNull( i ) * type_overrides.null_mapping.float64_null )
        + ( !dbl_array->IsNull( i ) * dbl_array->Value( i ) );
    }
  }
  else {
    memcpy( &kF( k_array )[index], dbl_array->raw_values(), length * sizeof( arrow::DoubleArray::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::STRING>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto str_array = static_pointer_cast<arrow::StringArray>(array_data);
  auto length = str_array->length();
  for( auto i = 0; i < length; ++i ){
    K k_str = nullptr;
    if( type_overrides.null_mapping.have_string && str_array->IsNull( i ) ){
      k_str = ktn( KC, type_overrides.null_mapping.string_null.length() );
      memcpy( kG( k_str ), type_overrides.null_mapping.string_null.data(), type_overrides.null_mapping.string_null.length() );
    }
    else{
      auto str_data = str_array->GetString(i);
      k_str = ktn(KC, str_data.length());
      memcpy(kG( k_str ), str_data.data(), str_data.length());
    }
    kK( k_array )[index++] = k_str;
  }
}

template<>
void AppendArray<arrow::Type::LARGE_STRING>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto str_array = static_pointer_cast<arrow::LargeStringArray>(array_data);
  auto length = str_array->length();
  for( auto i = 0; i < length; ++i ){
    K k_str = nullptr;
    if( type_overrides.null_mapping.have_large_string && str_array->IsNull( i ) ){
      k_str = ktn( KC, type_overrides.null_mapping.large_string_null.length() );
      memcpy( kG( k_str ), type_overrides.null_mapping.large_string_null.data(), type_overrides.null_mapping.large_string_null.length() );
    }
    else{
      auto str_data = str_array->GetString(i);
      k_str = ktn(KC, str_data.length());
      memcpy(kG(k_str), str_data.data(), str_data.length());
    }
    kK( k_array )[index++] = k_str;
  }
}

template<>
void AppendArray<arrow::Type::BINARY>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto bin_array = static_pointer_cast<arrow::BinaryArray>(array_data);
  auto length = bin_array->length();
  for( auto i = 0; i < length; ++i ){
    K k_bin = nullptr;
    if( type_overrides.null_mapping.have_binary && bin_array->IsNull( i ) ){
      k_bin = ktn( KG, type_overrides.null_mapping.binary_null.length() );
      memcpy( kG( k_bin ), type_overrides.null_mapping.binary_null.data(), type_overrides.null_mapping.binary_null.length() );
    }
    else{
      auto bin_data = bin_array->GetString(i);
      k_bin = ktn(KG, bin_data.length());
      memcpy(kG(k_bin), bin_data.data(), bin_data.length());
    }
    kK(k_array)[index++] = k_bin;
  }
}

template<>
void AppendArray<arrow::Type::LARGE_BINARY>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto bin_array = static_pointer_cast<arrow::LargeBinaryArray>(array_data);
  auto length = bin_array->length();
  for( auto i = 0; i < length; ++i ){
    K k_bin = nullptr;
    if( type_overrides.null_mapping.have_large_binary && bin_array->IsNull( i ) ){
        k_bin = ktn( KG, type_overrides.null_mapping.large_binary_null.length() );
        memcpy( kG( k_bin ), type_overrides.null_mapping.large_binary_null.data(), type_overrides.null_mapping.large_binary_null.length() );
      }
      else{
        auto bin_data = bin_array->GetString(i);
        k_bin = ktn(KG, bin_data.length());
        memcpy(kG(k_bin), bin_data.data(), bin_data.length());
    }
    kK(k_array)[index++] = k_bin;
  }
}

template<>
void AppendArray<arrow::Type::FIXED_SIZE_BINARY>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto fixed_bin_array = static_pointer_cast<arrow::FixedSizeBinaryArray>(array_data);
  auto length = fixed_bin_array->length();
  for( auto i = 0; i < length; ++i ){
    K k_bin = nullptr;
    if( type_overrides.null_mapping.have_fixed_binary && fixed_bin_array->IsNull( i ) ){
      k_bin = ktn( KG, type_overrides.null_mapping.fixed_binary_null.length() );
      memcpy( kG( k_bin ), type_overrides.null_mapping.fixed_binary_null.data(), type_overrides.null_mapping.fixed_binary_null.length() );
    }
    else{
      auto bin_data = fixed_bin_array->GetString(i);
      k_bin = ktn(KG, bin_data.length());
      memcpy(kG(k_bin), bin_data.data(), bin_data.length());
    }
    kK(k_array)[index++] = k_bin;
  }
}

template<>
void AppendArray<arrow::Type::DATE32>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto d32_array = static_pointer_cast<arrow::Date32Array>(array_data);
  auto length = d32_array->length();
  for( auto i = 0; i < length; ++i ){
    kI( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_date32 && d32_array->IsNull( i ) ) * type_overrides.null_mapping.date32_null )
      + ( !( type_overrides.null_mapping.have_date32 && d32_array->IsNull( i ) ) * tc.ArrowToKdb( d32_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::DATE64>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto d64_array = static_pointer_cast<arrow::Date64Array>(array_data);
  auto length = d64_array->length();
  for( auto i = 0; i < length; ++i ){
    kJ( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_date64 && d64_array->IsNull( i ) ) * type_overrides.null_mapping.date64_null )
      + ( !( type_overrides.null_mapping.have_date64 && d64_array->IsNull( i ) ) * tc.ArrowToKdb( d64_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::TIMESTAMP>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto ts_array = static_pointer_cast<arrow::TimestampArray>(array_data);
  auto timestamp_type = static_pointer_cast<arrow::TimestampType>(ts_array->type());
  auto length = ts_array->length();
  for( auto i = 0; i < length; ++i ){
    kJ( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_timestamp && ts_array->IsNull( i ) ) * type_overrides.null_mapping.timestamp_null )
      + ( !( type_overrides.null_mapping.have_timestamp && ts_array->IsNull( i ) ) * tc.ArrowToKdb( ts_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::TIME32>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto t32_array = static_pointer_cast<arrow::Time32Array>(array_data);
  auto time32_type = static_pointer_cast<arrow::Time32Type>(t32_array->type());
  auto length = t32_array->length();
  for( auto i = 0; i < length; ++i ){
    kI( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_time32 && t32_array->IsNull( i ) ) * type_overrides.null_mapping.time32_null )
      + ( !( type_overrides.null_mapping.have_time32 && t32_array->IsNull( i ) ) * tc.ArrowToKdb( t32_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::TIME64>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto t64_array = static_pointer_cast<arrow::Time64Array>(array_data);
  auto time64_type = static_pointer_cast<arrow::Time64Type>(t64_array->type());
  auto length = t64_array->length();
  for( auto i = 0; i < length; ++i ){
    kJ( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_time64 && t64_array->IsNull( i ) ) * type_overrides.null_mapping.time64_null )
      + ( !( type_overrides.null_mapping.have_time64 && t64_array->IsNull( i ) ) * tc.ArrowToKdb( t64_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::DECIMAL>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dec_array = static_pointer_cast<arrow::Decimal128Array>(array_data);
  auto dec_type = static_pointer_cast<arrow::Decimal128Type>(dec_array->type());
  auto length = dec_array->length();
  for( auto i = 0; i < length; ++i ){
    auto decimal = arrow::Decimal128(dec_array->Value(i));
    if (type_overrides.decimal128_as_double) {
      // Convert the decimal to a double
      auto dec_as_double =
        ( ( type_overrides.null_mapping.have_decimal && dec_array->IsNull( i ) ) * type_overrides.null_mapping.decimal_null )
        + ( !( type_overrides.null_mapping.have_decimal && dec_array->IsNull( i ) ) * decimal.ToDouble( dec_type->scale() ) );

      kF(k_array)[index++] = dec_as_double;
    } else {
      // Each decimal is a list of 16 bytes
      K k_dec = ktn(KG, 16);
      decimal.ToBytes(kG(k_dec));
      kK(k_array)[index++] = k_dec;
    }
  }
}

template<>
void AppendArray<arrow::Type::DURATION>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(array_data->type());
  auto dur_array = static_pointer_cast<arrow::DurationArray>(array_data);
  auto duration_type = static_pointer_cast<arrow::DurationType>(dur_array->type());
  auto length = dur_array->length();
  for( auto i = 0; i < length; ++i ){
    kJ( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_duration && dur_array->IsNull( i ) ) * type_overrides.null_mapping.duration_null )
      + ( !( type_overrides.null_mapping.have_duration && dur_array->IsNull( i ) ) * tc.ArrowToKdb( dur_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::INTERVAL_MONTHS>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto month_array = static_pointer_cast<arrow::MonthIntervalArray>(array_data);
  auto length = month_array->length();
  if( type_overrides.null_mapping.have_month_interval && month_array->null_count() ){
    for( auto i = 0ll; i < length; ++i ){
      kI( k_array )[i] = ( month_array->IsNull( i ) * type_overrides.null_mapping.month_interval_null )
        + ( !month_array->IsNull( i ) * month_array->Value( i ) );
    }
  }
  else {
    memcpy( &kI( k_array )[index], month_array->raw_values(), length * sizeof( arrow::MonthIntervalArray::value_type ) );
  }
  index += length;
}

template<>
void AppendArray<arrow::Type::INTERVAL_DAY_TIME>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto dt_array = static_pointer_cast<arrow::DayTimeIntervalArray>(array_data);
  auto length = dt_array->length();
  for( auto i = 0; i < length; ++i ){
    kJ( k_array )[index++] =
      ( ( type_overrides.null_mapping.have_day_time_interval && dt_array->IsNull( i ) ) * type_overrides.null_mapping.day_time_interval_null )
      + ( !( type_overrides.null_mapping.have_day_time_interval && dt_array->IsNull( i ) ) * DayTimeInterval_KTimespan( dt_array->Value( i ) ) );
  }
}

template<>
void AppendArray<arrow::Type::LIST>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::ListArray>(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArray);
}

template<>
void AppendArray<arrow::Type::LARGE_LIST>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::LargeListArray>(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArray);
}

template<>
void AppendArray<arrow::Type::FIXED_SIZE_LIST>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::FixedSizeListArray>(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArray);
}

template<>
void AppendArray<arrow::Type::MAP>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendMap(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArray);
}

template<>
void AppendArray<arrow::Type::STRUCT>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendStruct(array_data, k_array, index, type_overrides, kx::arrowkdb::AppendArray);
}

template<>
void AppendArray<arrow::Type::SPARSE_UNION>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendUnion(array_data, k_array, index, type_overrides, kx::arrowkdb::AppendArray);
}

template<>
void AppendArray<arrow::Type::DENSE_UNION>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendArray<arrow::Type::SPARSE_UNION>(array_data, k_array, index, type_overrides);
}

template<>
void AppendArray<arrow::Type::DICTIONARY>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendDictionary(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArray);
}

using ArrayHandler = void (*) (shared_ptr<arrow::Array>, K, size_t&, TypeMappingOverride&);

template<arrow::Type::type TypeId>
auto make_array_handler()
{
  return make_pair( TypeId, &AppendArray<TypeId> );
}

unordered_map<arrow::Type::type, ArrayHandler>  ArrayHandlers {
    make_array_handler<arrow::Type::NA>()
  , make_array_handler<arrow::Type::BOOL>()
  , make_array_handler<arrow::Type::UINT8>()
  , make_array_handler<arrow::Type::INT8>()
  , make_array_handler<arrow::Type::UINT16>()
  , make_array_handler<arrow::Type::INT16>()
  , make_array_handler<arrow::Type::UINT32>()
  , make_array_handler<arrow::Type::INT32>()
  , make_array_handler<arrow::Type::UINT64>()
  , make_array_handler<arrow::Type::INT64>()
  , make_array_handler<arrow::Type::HALF_FLOAT>()
  , make_array_handler<arrow::Type::FLOAT>()
  , make_array_handler<arrow::Type::DOUBLE>()
  , make_array_handler<arrow::Type::STRING>()
  , make_array_handler<arrow::Type::LARGE_STRING>()
  , make_array_handler<arrow::Type::BINARY>()
  , make_array_handler<arrow::Type::LARGE_BINARY>()
  , make_array_handler<arrow::Type::FIXED_SIZE_BINARY>()
  , make_array_handler<arrow::Type::DATE32>()
  , make_array_handler<arrow::Type::DATE64>()
  , make_array_handler<arrow::Type::TIMESTAMP>()
  , make_array_handler<arrow::Type::TIME32>()
  , make_array_handler<arrow::Type::TIME64>()
  , make_array_handler<arrow::Type::DECIMAL>()
  , make_array_handler<arrow::Type::DURATION>()
  , make_array_handler<arrow::Type::INTERVAL_MONTHS>()
  , make_array_handler<arrow::Type::INTERVAL_DAY_TIME>()
  , make_array_handler<arrow::Type::LIST>()
  , make_array_handler<arrow::Type::LARGE_LIST>()
  , make_array_handler<arrow::Type::FIXED_SIZE_LIST>()
  , make_array_handler<arrow::Type::MAP>()
  , make_array_handler<arrow::Type::STRUCT>()
  , make_array_handler<arrow::Type::SPARSE_UNION>()
  , make_array_handler<arrow::Type::DENSE_UNION>()
  , make_array_handler<arrow::Type::DICTIONARY>()
};

using NullBitmapHandler = void ( * )(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides);

extern unordered_map<arrow::Type::type, NullBitmapHandler> NullBitmapHandlers;

template<arrow::Type::type TypeId>
void AppendArrayNullBitmap(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides);

template<>
void AppendArrayNullBitmap<arrow::Type::LIST>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::ListArray>(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArrayNullBitmap);
}

template<>
void AppendArrayNullBitmap<arrow::Type::LARGE_LIST>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::LargeListArray>(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArrayNullBitmap);
}

template<>
void AppendArrayNullBitmap<arrow::Type::FIXED_SIZE_LIST>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendList<arrow::FixedSizeListArray>(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArrayNullBitmap);
}

template<>
void AppendArrayNullBitmap<arrow::Type::MAP>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendMap(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArrayNullBitmap);
}

template<>
void AppendArrayNullBitmap<arrow::Type::STRUCT>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendStruct(array_data, k_array, index, type_overrides, kx::arrowkdb::AppendArrayNullBitmap);
}

template<>
void AppendArrayNullBitmap<arrow::Type::SPARSE_UNION>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendUnion(array_data, k_array, index, type_overrides, kx::arrowkdb::AppendArrayNullBitmap);
}

template<>
void AppendArrayNullBitmap<arrow::Type::DENSE_UNION>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendArrayNullBitmap<arrow::Type::SPARSE_UNION>(array_data, k_array, index, type_overrides);
}

template<>
void AppendArrayNullBitmap<arrow::Type::DICTIONARY>(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  AppendDictionary(array_data, k_array, index, type_overrides, kx::arrowkdb::ReadArrayNullBitmap);
}

template<arrow::Type::type TypeId>
auto make_append_array_null_bitmap_handler()
{
  return make_pair( TypeId, &AppendArrayNullBitmap<TypeId> );
}

unordered_map<arrow::Type::type, NullBitmapHandler> NullBitmapHandlers{
    make_append_array_null_bitmap_handler<arrow::Type::LIST>()
  , make_append_array_null_bitmap_handler<arrow::Type::LARGE_LIST>()
  , make_append_array_null_bitmap_handler<arrow::Type::FIXED_SIZE_LIST>()
  , make_append_array_null_bitmap_handler<arrow::Type::MAP>()
  , make_append_array_null_bitmap_handler<arrow::Type::STRUCT>()
  , make_append_array_null_bitmap_handler<arrow::Type::SPARSE_UNION>()
  , make_append_array_null_bitmap_handler<arrow::Type::DENSE_UNION>()
  , make_append_array_null_bitmap_handler<arrow::Type::DICTIONARY>()
};

typedef K(*InitKdbForArrayHandler)(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type);

extern unordered_map<arrow::Type::type, InitKdbForArrayHandler> InitKdbForArrayHandlers;

template<arrow::Type::type TypeId>
K InitKdbForArray(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type);

template<>
K InitKdbForArray<arrow::Type::STRUCT>(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type)
{
  // Arrow struct becomes a mixed list of lists so create necessary lists
  auto num_fields = datatype->num_fields();
  K result = knk(num_fields);
  for (auto i = 0; i < num_fields; ++i) {
    auto field = datatype->field(i);
    kK(result)[i] = InitKdbForArray(field->type(), length, type_overrides, get_kdb_type);
  }
  return result;
}

template<>
K InitKdbForArray<arrow::Type::SPARSE_UNION>(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type)
{
  // Arrow union becomes a mixed list of type_id list plus the child lists
  auto num_fields = datatype->num_fields();
  K result = knk(num_fields + 1);
  kK(result)[0] = ktn(KH, length); // type_id list
  for (auto i = 0; i < num_fields; ++i) {
    auto field = datatype->field(i);
    kK(result)[i + 1] = InitKdbForArray(field->type(), length, type_overrides, get_kdb_type);
  }
  return result;
}

template<>
K InitKdbForArray<arrow::Type::DENSE_UNION>(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type)
{
  return InitKdbForArray<arrow::Type::SPARSE_UNION>(datatype, length, type_overrides, get_kdb_type);
}

template<>
K InitKdbForArray<arrow::Type::DICTIONARY>(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type)
{
  // Arrow dictionary becomes a two item mixed list
  auto dictionary_type = static_pointer_cast<arrow::DictionaryType>(datatype);
  K result = ktn(0, 2);

  // Do not preallocate the child lists since AppendDictionary has to join to the
  // indicies and values lists
  kK(result)[0] = InitKdbForArray(dictionary_type->value_type(), 0, type_overrides, get_kdb_type);
  kK(result)[1] = InitKdbForArray(dictionary_type->index_type(), 0, type_overrides, get_kdb_type);

  return result;
}

template<arrow::Type::type TypeId>
auto make_init_kdb_for_array_handler()
{
  return make_pair(TypeId, &InitKdbForArray<TypeId>);
}

unordered_map<arrow::Type::type, InitKdbForArrayHandler> InitKdbForArrayHandlers{
    make_init_kdb_for_array_handler<arrow::Type::STRUCT>()
  , make_init_kdb_for_array_handler<arrow::Type::SPARSE_UNION>()
  , make_init_kdb_for_array_handler<arrow::Type::DENSE_UNION>()
  , make_init_kdb_for_array_handler<arrow::Type::DICTIONARY>()
};


} // namespace

namespace kx {
namespace arrowkdb {

void AppendArray(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto lookup = ArrayHandlers.find(array_data->type_id());
  if (lookup == ArrayHandlers.end()) {
    TYPE_CHECK_UNSUPPORTED(array_data->type()->ToString());
  } else {
    lookup->second(array_data, k_array, index, type_overrides);
  }
}

void AppendArrayNullBitmap(shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides)
{
  auto lookup = NullBitmapHandlers.find(array_data->type_id());
  if (lookup == NullBitmapHandlers.end()) {
    for (int i = 0ll; i < array_data->length(); ++i)
      kG(k_array)[index++] = array_data->IsNull(i);
  } else {
    lookup->second(array_data, k_array, index, type_overrides);
  }
}

KdbType GetKdbTypeNullBitmap(std::shared_ptr<arrow::DataType> datatype, TypeMappingOverride& type_overrides)
{
  if (NullBitmapHandlers.find(datatype->id()) == NullBitmapHandlers.end())
    return KB;
  else
    return 0;
}

K InitKdbForArray(shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type)
{
  auto lookup = InitKdbForArrayHandlers.find(datatype->id());
  if (lookup != InitKdbForArrayHandlers.end()) {
    return lookup->second(datatype, length, type_overrides, get_kdb_type);
  } else {
    return ktn(get_kdb_type(datatype, type_overrides), length);
  }
}

K ReadArray(shared_ptr<arrow::Array> array, TypeMappingOverride& type_overrides)
{
  K k_array = InitKdbForArray(array->type(), array->length(), type_overrides, GetKdbType);
  size_t index = 0;
  AppendArray(array, k_array, index, type_overrides);
  return k_array;
}

K ReadArrayNullBitmap(shared_ptr<arrow::Array> array, TypeMappingOverride& type_overrides)
{
  K k_array = InitKdbForArray(array->type(), array->length(), type_overrides, GetKdbTypeNullBitmap);
  size_t index = 0;
  AppendArrayNullBitmap(array, k_array, index, type_overrides);
  return k_array;
}

K ReadChunkedArray(shared_ptr<arrow::ChunkedArray> chunked_array, TypeMappingOverride& type_overrides)
{
  K k_array = InitKdbForArray(chunked_array->type(), chunked_array->length(), type_overrides, GetKdbType);
  size_t index = 0;
  for (auto j = 0; j < chunked_array->num_chunks(); ++j)
    AppendArray(chunked_array->chunk(j), k_array, index, type_overrides);
  return k_array;
}

K ReadChunkedArrayNullBitmap(shared_ptr<arrow::ChunkedArray> chunked_array, TypeMappingOverride& type_overrides)
{
  K k_array = InitKdbForArray(chunked_array->type(), chunked_array->length(), type_overrides, GetKdbTypeNullBitmap);
  size_t index = 0;
  for (auto j = 0; j < chunked_array->num_chunks(); ++j)
    AppendArrayNullBitmap(chunked_array->chunk(j), k_array, index, type_overrides);
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
