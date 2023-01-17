#include <memory>
#include <unordered_map>
#include <iostream>
#include <stdexcept>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/pretty_print.h>
#include <arrow/util/decimal.h>

#include "ArrayWriter.h"
#include "DatatypeStore.h"
#include "HelperFunctions.h"
#include "TypeCheck.h"

using namespace std;
using namespace kx::arrowkdb;

namespace
{

shared_ptr<arrow::ArrayBuilder> GetBuilder(shared_ptr<arrow::DataType> datatype);

template<arrow::Type::type TypeId>
shared_ptr<arrow::ArrayBuilder> GetBuilder(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool);

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::NA>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::NullBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::BOOL>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::BooleanBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::UINT8>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::UInt8Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::INT8>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Int8Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::UINT16>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::UInt16Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::INT16>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Int16Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::UINT32>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::UInt32Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::INT32>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Int32Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::UINT64>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::UInt64Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::INT64>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Int64Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::HALF_FLOAT>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::HalfFloatBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::FLOAT>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::FloatBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::DOUBLE>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::DoubleBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::STRING>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::StringBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::LARGE_STRING>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::LargeStringBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::BINARY>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::BinaryBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::LARGE_BINARY>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::LargeBinaryBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::FIXED_SIZE_BINARY>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::FixedSizeBinaryBuilder>(datatype, pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::DATE32>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Date32Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::DATE64>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Date64Builder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::TIMESTAMP>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::TimestampBuilder>(datatype, pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::TIME32>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Time32Builder>(datatype, pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::TIME64>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Time64Builder>(datatype, pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::DECIMAL>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::Decimal128Builder>(datatype, pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::DURATION>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::DurationBuilder>(datatype, pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::INTERVAL_MONTHS>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::MonthIntervalBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::INTERVAL_DAY_TIME>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return make_shared<arrow::DayTimeIntervalBuilder>(pool);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::LIST>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  // The parent list datatype details the child datatype so construct the child
  // builder and use it to initialise the parent list builder
  auto list_type = static_pointer_cast<arrow::BaseListType>(datatype);
  auto value_builder = GetBuilder(list_type->value_type());

  // Construct the correct listbuilder
  if (datatype->id() == arrow::Type::LIST)
    return make_shared<arrow::ListBuilder>(pool, value_builder);
  else if (datatype->id() == arrow::Type::LARGE_LIST)
    return make_shared<arrow::LargeListBuilder>(pool, value_builder);
  else
    return make_shared<arrow::FixedSizeListBuilder>(pool, value_builder, datatype);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::LARGE_LIST>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return GetBuilder<arrow::Type::LIST>( datatype, pool );
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::FIXED_SIZE_LIST>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return GetBuilder<arrow::Type::LIST>( datatype, pool );  
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::MAP>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  // The parent map datatype details the key/item child datatypes so construct
  // builders for both and use these to initialise the parent map builder
  auto map_type = static_pointer_cast<arrow::MapType>(datatype);
  auto key_builder = GetBuilder(map_type->key_type());
  auto item_builder = GetBuilder(map_type->item_type());
  return make_shared<arrow::MapBuilder>(pool, key_builder, item_builder);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::STRUCT>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  auto struct_type = static_pointer_cast<arrow::StructType>(datatype);

  // Iterate through all the fields in the struct constructing and adding each
  // field's builder into a vector
  auto fields = struct_type->fields();
  vector<shared_ptr<arrow::ArrayBuilder>> field_builders;
  for (auto field : fields)
    field_builders.push_back(GetBuilder(field->type()));

  // Construct the parent struct builder from this vector of all the child
  // builders
  return make_shared<arrow::StructBuilder>(datatype, pool, field_builders);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::SPARSE_UNION>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  auto union_type = static_pointer_cast<arrow::UnionType>(datatype);

  // Iterate through all the fields in the union constructing and adding each
  // field's builder into a vector
  auto fields = union_type->fields();
  vector<shared_ptr<arrow::ArrayBuilder>> field_builders;
  for (auto field : fields)
    field_builders.push_back(GetBuilder(field->type()));

  // Construct the parent union builder from this vector of all the child
  // builders
  if (datatype->id() == arrow::Type::SPARSE_UNION)
    return make_shared<arrow::SparseUnionBuilder>(pool, field_builders, datatype);
  else
    return make_shared<arrow::DenseUnionBuilder>(pool, field_builders, datatype);
}

template<>
shared_ptr<arrow::ArrayBuilder> GetBuilder<arrow::Type::DENSE_UNION>(shared_ptr<arrow::DataType> datatype, arrow::MemoryPool* pool)
{
  return GetBuilder<arrow::Type::SPARSE_UNION>( datatype, pool );
}

using BuilderHandler = shared_ptr<arrow::ArrayBuilder> ( * ) ( shared_ptr<arrow::DataType>, arrow::MemoryPool* );

template<arrow::Type::type TypeId>
auto make_builder_handler()
{
  return make_pair( TypeId, &GetBuilder<TypeId> );
}

unordered_map<arrow::Type::type, BuilderHandler> BuilderHandlers {
    make_builder_handler<arrow::Type::NA>()
  , make_builder_handler<arrow::Type::BOOL>()
  , make_builder_handler<arrow::Type::UINT8>()
  , make_builder_handler<arrow::Type::INT8>()
  , make_builder_handler<arrow::Type::UINT16>()
  , make_builder_handler<arrow::Type::INT16>()
  , make_builder_handler<arrow::Type::UINT32>()
  , make_builder_handler<arrow::Type::INT32>()
  , make_builder_handler<arrow::Type::UINT64>()
  , make_builder_handler<arrow::Type::INT64>()
  , make_builder_handler<arrow::Type::HALF_FLOAT>()
  , make_builder_handler<arrow::Type::FLOAT>()
  , make_builder_handler<arrow::Type::DOUBLE>()
  , make_builder_handler<arrow::Type::STRING>()
  , make_builder_handler<arrow::Type::LARGE_STRING>()
  , make_builder_handler<arrow::Type::BINARY>()
  , make_builder_handler<arrow::Type::LARGE_BINARY>()
  , make_builder_handler<arrow::Type::FIXED_SIZE_BINARY>()
  , make_builder_handler<arrow::Type::DATE32>()
  , make_builder_handler<arrow::Type::DATE64>()
  , make_builder_handler<arrow::Type::TIMESTAMP>()
  , make_builder_handler<arrow::Type::TIME32>()
  , make_builder_handler<arrow::Type::TIME64>()
  , make_builder_handler<arrow::Type::DECIMAL>()
  , make_builder_handler<arrow::Type::DURATION>()
  , make_builder_handler<arrow::Type::INTERVAL_MONTHS>()
  , make_builder_handler<arrow::Type::INTERVAL_DAY_TIME>()
  , make_builder_handler<arrow::Type::LIST>()
  , make_builder_handler<arrow::Type::LARGE_LIST>()
  , make_builder_handler<arrow::Type::FIXED_SIZE_LIST>()
  , make_builder_handler<arrow::Type::MAP>()
  , make_builder_handler<arrow::Type::STRUCT>()
  , make_builder_handler<arrow::Type::SPARSE_UNION>()
  , make_builder_handler<arrow::Type::DENSE_UNION>()
};

// Constructs and returns the correct arrow array builder for the specified
// datatype.
//
// This handles all datatypes except Dictionary which is handled separately.
shared_ptr<arrow::ArrayBuilder> GetBuilder(shared_ptr<arrow::DataType> datatype)
{
  auto type_id = datatype->id();
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  if( BuilderHandlers.find( type_id ) == BuilderHandlers.end() )
  {
    TYPE_CHECK_UNSUPPORTED(datatype->ToString());
  }
  else
  {
    return BuilderHandlers[type_id]( datatype, pool );
  }
}

} // namespace

namespace
{

// Populate a list/large_list/fixed_size_list builder
//
// An arrow list array is a nested set of child lists.  This is represented in
// kdb as a mixed list for the parent list array containing a set of sub-lists,
// one for each of the list value sets.
template <typename ListBuilderType>
void PopulateListBuilder(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  // Get the value builder from the parent list builder
  auto list_builder = static_cast<ListBuilderType*>(builder);
  auto value_builder = list_builder->value_builder();

  for (auto i = 0; i < k_array->n; ++i) {
    // Ignore any mixed list items set to ::
    if (kK(k_array)[i]->t == 101)
      continue;

    // Delimit the start/end of each child list set
    list_builder->Append();

    if (datatype->id() == arrow::Type::FIXED_SIZE_LIST) {
      // Check each sub-list is the same length as the fixed size
      K list_data = kK(k_array)[i];
      auto fixed_list_type = static_pointer_cast<arrow::FixedSizeListType>(datatype);
      TYPE_CHECK_LENGTH(fixed_list_type->list_size() != list_data->n, datatype->ToString(), fixed_list_type->list_size(), list_data->n);
    }

    // Populate the child builder for this list set
    PopulateBuilder(value_builder->type(), kK(k_array)[i], value_builder, type_overrides);
  }
}

// Populate a sparse_union/dense_union builder
//
// An arrow union array is similar to a struct array except that it has an
// additional type id array which identifies the live field in each union value
// set.
template <typename UnionBuilderType>
void PopulateUnionBuilder(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  // Check that the mixed list length is at least one greater (the additional 
  // first sub-list contains the union type_ids) than the number of union 
  // fields
  auto union_type = static_pointer_cast<arrow::UnionType>(datatype);
  const auto min_length = union_type->num_fields() + 1;
  TYPE_CHECK_LENGTH(min_length > k_array->n, datatype->ToString(), min_length, k_array->n);

  // The first list contains the list of type_ids which denotes the index of the
  // 'live' child builder for each union value
  K type_ids = kK(k_array)[0];
  if (type_ids->t != KH)
    throw TypeCheck("union type_id list not 5h");

  // Get all the child builders from the parent union builder
  auto union_builder = static_cast<UnionBuilderType*>(builder);
  vector<shared_ptr<arrow::ArrayBuilder>> child_builders;
  for (auto i = 0; i < union_builder->num_children(); ++i)
    child_builders.push_back(union_builder->child_builder(i));

  // Delimit each union value in the parent builder by specifying the type_id
  // for this union value
  for (auto index = 0; index < kK(k_array)[0]->n; ++index) {
    int8_t live_type_id = kH(type_ids)[index];
    union_builder->Append(live_type_id);
  }

  // Populate each of the child builders from its kdb list, starting from 1 to
  // ignore the type_id list.  Only count up to the number of union fields
  // plus the type_id list.  Additional trailing data in the kdb mixed list is
  // ignored (to allow for ::)
  for (auto i = 1; i < min_length; ++i) {
    // type_id is zero indexed so used i-1 to reference the field builders
    auto builder_num = i - 1;
    PopulateBuilder(child_builders[builder_num]->type(), kK(k_array)[i], child_builders[builder_num].get(), type_overrides);
  }

  // Check that all the populated child builders have the same length
  for (auto it : child_builders)
    if (it->length() != union_builder->length())
      throw TypeCheck("Mismatched union list lengths");
}

template<arrow::Type::type TypeId>
void PopulateBuilder(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides);

template<>
void PopulateBuilder<arrow::Type::NA>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto null_builder = static_cast<arrow::NullBuilder*>(builder);
  PARQUET_THROW_NOT_OK(null_builder->AppendNulls(k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::BOOL>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto bool_builder = static_cast<arrow::BooleanBuilder*>(builder);
  PARQUET_THROW_NOT_OK(bool_builder->AppendValues((uint8_t*)kG(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::UINT8>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto uint8_builder = static_cast<arrow::UInt8Builder*>(builder);
  PARQUET_THROW_NOT_OK(uint8_builder->AppendValues((uint8_t*)kG(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::INT8>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto int8_builder = static_cast<arrow::Int8Builder*>(builder);
  PARQUET_THROW_NOT_OK(int8_builder->AppendValues((int8_t*)kG(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::UINT16>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto uint16_builder = static_cast<arrow::UInt16Builder*>(builder);
  PARQUET_THROW_NOT_OK(uint16_builder->AppendValues((uint16_t*)kH(k_array), k_array->n));
  arrow::Status s;
}

template<>
void PopulateBuilder<arrow::Type::INT16>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto int16_builder = static_cast<arrow::Int16Builder*>(builder);
  if( type_overrides.null_mapping.have_int16 ){
    for( auto i = 0; i < k_array->n; ++i ){
      if( type_overrides.null_mapping.int16_null == kH( k_array )[i]){
        PARQUET_THROW_NOT_OK( int16_builder->AppendNull() );
      }
      else{
        PARQUET_THROW_NOT_OK( int16_builder->AppendValues( ( int16_t* )&kH( k_array )[i], 1 ) );
      }
    }
  }
  else {
    PARQUET_THROW_NOT_OK( int16_builder->AppendValues( ( int16_t* )kH( k_array), k_array->n ) );
  }
}

template<>
void PopulateBuilder<arrow::Type::UINT32>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto uint32_builder = static_cast<arrow::UInt32Builder*>(builder);
  PARQUET_THROW_NOT_OK(uint32_builder->AppendValues((uint32_t*)kI(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::INT32>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto int32_builder = static_cast<arrow::Int32Builder*>(builder);
  type_overrides.null_mapping.have_int32 = true;
  type_overrides.null_mapping.int32_null = -2147483648;
  if( type_overrides.null_mapping.have_int32 ){
    for( auto i = 0; i < k_array->n; ++i ){
      if( type_overrides.null_mapping.int32_null == kI( k_array )[i] ){
        PARQUET_THROW_NOT_OK( int32_builder->AppendNull() );
      }
      else{
        PARQUET_THROW_NOT_OK( int32_builder->AppendValues( ( int32_t* )&kI( k_array )[i], 1 ) );
      }
    }
  }
  else{
    PARQUET_THROW_NOT_OK(int32_builder->AppendValues((int32_t*)kI(k_array), k_array->n));
  }
}

template<>
void PopulateBuilder<arrow::Type::UINT64>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto uint64_builder = static_cast<arrow::UInt64Builder*>(builder);
  PARQUET_THROW_NOT_OK(uint64_builder->AppendValues((uint64_t*)kJ(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::INT64>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto int64_builder = static_cast<arrow::Int64Builder*>(builder);
  PARQUET_THROW_NOT_OK(int64_builder->AppendValues((int64_t*)kJ(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::HALF_FLOAT>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  arrow::HalfFloatType hft;
  auto hfl_builder = static_cast<arrow::HalfFloatBuilder*>(builder);
  PARQUET_THROW_NOT_OK(hfl_builder->AppendValues((uint16_t*)kH(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::FLOAT>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto fl_builder = static_cast<arrow::FloatBuilder*>(builder);
  PARQUET_THROW_NOT_OK(fl_builder->AppendValues(kE(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::DOUBLE>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto dbl_builder = static_cast<arrow::DoubleBuilder*>(builder);
  PARQUET_THROW_NOT_OK(dbl_builder->AppendValues(kF(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::STRING>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto str_builder = static_cast<arrow::StringBuilder*>(builder);
  if( is_symbol ){
    // Populate from symbol list
    for( auto i = 0ll; i < k_array->n; ++i ){
      if( type_overrides.null_mapping.have_string
          && type_overrides.null_mapping.string_null == kS( k_array )[i] ){
        PARQUET_THROW_NOT_OK( str_builder->AppendNull() );
      }
      else{
        PARQUET_THROW_NOT_OK( str_builder->Append( kS( k_array )[i] ) );
      }
    }
  } else {
    // Populate from mixed list of char lists
    for( auto i = 0ll; i < k_array->n; ++i ){
      K str_data = kK( k_array )[i];
      TYPE_CHECK_ITEM( str_data->t != KC, datatype->ToString(), KC, str_data->t );
      if( type_overrides.null_mapping.have_string
          && type_overrides.null_mapping.string_null == std::string( ( char* )kG( str_data ), str_data->n ) ){
        PARQUET_THROW_NOT_OK( str_builder->AppendNull() );
      }
      else{
        PARQUET_THROW_NOT_OK( str_builder->Append( kG( str_data ), str_data->n ) );
      }
    }
  }
}

template<>
void PopulateBuilder<arrow::Type::LARGE_STRING>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto str_builder = static_cast<arrow::LargeStringBuilder*>(builder);
  if( is_symbol ){
    // Populate from symbol list
    for( auto i = 0ll; i < k_array->n; ++i ){
      if( type_overrides.null_mapping.have_large_string
          && type_overrides.null_mapping.large_string_null == kS( k_array )[i] ){
        PARQUET_THROW_NOT_OK( str_builder->AppendNull() );
      }
      else{
        PARQUET_THROW_NOT_OK( str_builder->Append( kS( k_array )[i] ) );
      }
    }
  } else {
    // Populate from mixed list of char lists
    for( auto i = 0ll; i < k_array->n; ++i ){
      K str_data = kK( k_array )[i];
      TYPE_CHECK_ITEM( str_data->t != KC, datatype->ToString(), KC, str_data->t );
      if( type_overrides.null_mapping.have_string
          && type_overrides.null_mapping.string_null == std::string( ( char* )kG( str_data ), str_data->n ) ){
        PARQUET_THROW_NOT_OK( str_builder->AppendNull() );
      }
      else{
        PARQUET_THROW_NOT_OK( str_builder->Append( kG( str_data ), str_data->n ) );
      }
    }
  }
}

template<>
void PopulateBuilder<arrow::Type::BINARY>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto bin_builder = static_cast<arrow::BinaryBuilder*>(builder);
  for (auto i = 0; i < k_array->n; ++i) {
    K bin_data = kK(k_array)[i];
    TYPE_CHECK_ITEM(bin_data->t != KG, datatype->ToString(), KG, bin_data->t);
    PARQUET_THROW_NOT_OK(bin_builder->Append(kG(bin_data), bin_data->n));
  }
}

template<>
void PopulateBuilder<arrow::Type::LARGE_BINARY>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto bin_builder = static_cast<arrow::LargeBinaryBuilder*>(builder);
  for (auto i = 0; i < k_array->n; ++i) {
    K bin_data = kK(k_array)[i];
    TYPE_CHECK_ITEM(bin_data->t != KG, datatype->ToString(), KG, bin_data->t);
    PARQUET_THROW_NOT_OK(bin_builder->Append(kG(bin_data), bin_data->n));
  }
}

template<>
void PopulateBuilder<arrow::Type::FIXED_SIZE_BINARY>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  bool is_guid = k_array->t == UU && datatype->id() == arrow::Type::FIXED_SIZE_BINARY && static_cast<arrow::FixedSizeBinaryBuilder*>(builder)->byte_width() == sizeof(U);
  auto fixed_bin_builder = static_cast<arrow::FixedSizeBinaryBuilder*>(builder);
  if (is_guid) {
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(fixed_bin_builder->Append((char*)&kU(k_array)[i]));
  } else {
    for (auto i = 0; i < k_array->n; ++i) {
      K bin_data = kK(k_array)[i];
      TYPE_CHECK_ITEM(bin_data->t != KG, datatype->ToString(), KG, bin_data->t);
      TYPE_CHECK_LENGTH(fixed_bin_builder->byte_width() != bin_data->n, builder->type()->ToString(), fixed_bin_builder->byte_width(), bin_data->n);
      PARQUET_THROW_NOT_OK(fixed_bin_builder->Append(kG(bin_data)));
    }
  }
}

template<>
void PopulateBuilder<arrow::Type::DATE32>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(datatype);
  auto d32_builder = static_cast<arrow::Date32Builder*>(builder);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(d32_builder->Append(tc.KdbToArrow(kI(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::DATE64>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(datatype);
  auto d64_builder = static_cast<arrow::Date64Builder*>(builder);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(d64_builder->Append(tc.KdbToArrow(kJ(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::TIMESTAMP>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(datatype);
  auto ts_builder = static_cast<arrow::TimestampBuilder*>(builder);
  auto timestamp_type = static_pointer_cast<arrow::TimestampType>(datatype);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(ts_builder->Append(tc.KdbToArrow(kJ(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::TIME32>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(datatype);
  auto t32_builder = static_cast<arrow::Time32Builder*>(builder);
  auto time32_type = static_pointer_cast<arrow::Time32Type>(datatype);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(t32_builder->Append(tc.KdbToArrow(kI(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::TIME64>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(datatype);
  auto t64_builder = static_cast<arrow::Time64Builder*>(builder);
  auto time64_type = static_pointer_cast<arrow::Time64Type>(datatype);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(t64_builder->Append(tc.KdbToArrow(kJ(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::DECIMAL>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto dec_builder = static_cast<arrow::Decimal128Builder*>(builder);
  auto dec_type = static_pointer_cast<arrow::Decimal128Type>(datatype);
  for (auto i = 0; i < k_array->n; ++i) {
    if (type_overrides.decimal128_as_double) {
      // Construct the decimal from a double
      arrow::Decimal128 dec128;
      PARQUET_ASSIGN_OR_THROW(dec128, arrow::Decimal128::FromReal(kF(k_array)[i], dec_type->precision(), dec_type->scale()));
      PARQUET_THROW_NOT_OK(dec_builder->Append(dec128));
    } else {
      // Each decimal is a list of 16 bytes
      K k_dec = kK(k_array)[i];
      TYPE_CHECK_LENGTH(k_dec->n != 16, datatype->ToString(), 16, k_dec->n);
      TYPE_CHECK_ITEM(k_dec->t != KG, datatype->ToString(), KG, k_dec->t);

      arrow::Decimal128 dec128((const uint8_t*)kG(k_dec));
      PARQUET_THROW_NOT_OK(dec_builder->Append(dec128));
    }
  }
}

template<>
void PopulateBuilder<arrow::Type::DURATION>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  TemporalConversion tc(datatype);
  auto dur_builder = static_cast<arrow::DurationBuilder*>(builder);
  auto duration_type = static_pointer_cast<arrow::DurationType>(datatype);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(dur_builder->Append(tc.KdbToArrow(kJ(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::INTERVAL_MONTHS>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto month_builder = static_cast<arrow::MonthIntervalBuilder*>(builder);
  PARQUET_THROW_NOT_OK(month_builder->AppendValues((int32_t*)kI(k_array), k_array->n));
}

template<>
void PopulateBuilder<arrow::Type::INTERVAL_DAY_TIME>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  auto dt_builder = static_cast<arrow::DayTimeIntervalBuilder*>(builder);
  for (auto i = 0; i < k_array->n; ++i)
    PARQUET_THROW_NOT_OK(dt_builder->Append(KTimespan_DayTimeInterval(kJ(k_array)[i])));
}

template<>
void PopulateBuilder<arrow::Type::LIST>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  PopulateListBuilder<arrow::ListBuilder>(datatype, k_array, builder, type_overrides);
}

template<>
void PopulateBuilder<arrow::Type::LARGE_LIST>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  PopulateListBuilder<arrow::LargeListBuilder>(datatype, k_array, builder, type_overrides);
}

template<>
void PopulateBuilder<arrow::Type::FIXED_SIZE_LIST>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  PopulateListBuilder<arrow::FixedSizeListBuilder>(datatype, k_array, builder, type_overrides);
}

template<>
void PopulateBuilder<arrow::Type::MAP>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  // An arrow map array is a nested set of key/item paired child arrays.  This
  // is represented in kdb as a mixed list for the parent map array, with a
  // dictionary for each map value set.
  //
  // Get the key and item builders from the parent map builder
  auto map_builder = static_cast<arrow::MapBuilder*>(builder);
  auto key_builder = map_builder->key_builder();
  auto item_builder = map_builder->item_builder();

  for (auto i = 0; i < k_array->n; ++i) {
    // Ignore any mixed list items set to ::
    if (kK(k_array)[i]->t == 101)
      continue;

    // Delimit the start/end of each child map set
    map_builder->Append();

    // Populate the child builders for this map set from the dictionary key/value lists
    auto k_dict = kK(k_array)[i];
    TYPE_CHECK_ITEM(99 != k_dict->t, datatype->ToString(), 99, k_dict->t);
    PopulateBuilder(key_builder->type(), kK(k_dict)[0], key_builder, type_overrides);
    PopulateBuilder(item_builder->type(), kK(k_dict)[1], item_builder, type_overrides);
  }
}

template<>
void PopulateBuilder<arrow::Type::STRUCT>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  // An arrow struct array is a logical grouping of child arrays with each
  // child array corresponding to one of the fields in the struct.  A single
  // struct value is obtaining by slicing across all the child arrays at a
  // given index.  This is represented in kdb as a mixed list for the parent
  // struct array, containing child lists for each field in the struct.
  //
  // Check that the mixed list length is at least equal to the number of struct fields 
  auto struct_type = static_pointer_cast<arrow::StructType>(datatype);
  TYPE_CHECK_LENGTH(struct_type->num_fields() > k_array->n, datatype->ToString(), struct_type->num_fields(), k_array->n);

  // Get all the field builders from the parent struct builder    
  auto struct_builder = static_cast<arrow::StructBuilder*>(builder);
  vector<arrow::ArrayBuilder*> field_builders;
  for (auto i = 0; i < struct_builder->num_fields(); ++i)
    field_builders.push_back(struct_builder->field_builder(i));

  // Delimit each struct value in the parent builder
  for (auto index = 0; index < kK(k_array)[0]->n; ++index)
    struct_builder->Append();

  // Populate each of the field builders from its kdb list.  Only count up to
  // the number of struct fields.  Additional trailing data in the kdb mixed
  // list is ignored (to allow for ::)
  for (auto i = 0; i < struct_type->num_fields(); ++i)
    PopulateBuilder(field_builders[i]->type(), kK(k_array)[i], field_builders[i], type_overrides);

  // Check that all the populated field builders have the same length.
  for (auto it : field_builders)
    if (it->length() != struct_builder->length())
      throw TypeCheck("Mismatched struct list lengths");
}

template<>
void PopulateBuilder<arrow::Type::SPARSE_UNION>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  PopulateUnionBuilder<arrow::SparseUnionBuilder>(datatype, k_array, builder, type_overrides);
}

template<>
void PopulateBuilder<arrow::Type::DENSE_UNION>(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  PopulateUnionBuilder<arrow::DenseUnionBuilder>(datatype, k_array, builder, type_overrides);
}

using PopulateHandler = void ( * ) ( shared_ptr<arrow::DataType>, K, arrow::ArrayBuilder*, TypeMappingOverride& );

template<arrow::Type::type TypeId>
auto make_populate_handler()
{
  return make_pair( TypeId, &PopulateBuilder<TypeId> );
}

unordered_map<arrow::Type::type, PopulateHandler> PopulateHandlers {
    make_populate_handler<arrow::Type::NA>()
  , make_populate_handler<arrow::Type::BOOL>()
  , make_populate_handler<arrow::Type::UINT8>()
  , make_populate_handler<arrow::Type::INT8>()
  , make_populate_handler<arrow::Type::UINT16>()
  , make_populate_handler<arrow::Type::INT16>()
  , make_populate_handler<arrow::Type::UINT32>()
  , make_populate_handler<arrow::Type::INT32>()
  , make_populate_handler<arrow::Type::UINT64>()
  , make_populate_handler<arrow::Type::INT64>()
  , make_populate_handler<arrow::Type::HALF_FLOAT>()
  , make_populate_handler<arrow::Type::FLOAT>()
  , make_populate_handler<arrow::Type::DOUBLE>()
  , make_populate_handler<arrow::Type::STRING>()
  , make_populate_handler<arrow::Type::LARGE_STRING>()
  , make_populate_handler<arrow::Type::BINARY>()
  , make_populate_handler<arrow::Type::LARGE_BINARY>()
  , make_populate_handler<arrow::Type::FIXED_SIZE_BINARY>()
  , make_populate_handler<arrow::Type::DATE32>()
  , make_populate_handler<arrow::Type::DATE64>()
  , make_populate_handler<arrow::Type::TIMESTAMP>()
  , make_populate_handler<arrow::Type::TIME32>()
  , make_populate_handler<arrow::Type::TIME64>()
  , make_populate_handler<arrow::Type::DECIMAL>()
  , make_populate_handler<arrow::Type::DURATION>()
  , make_populate_handler<arrow::Type::INTERVAL_MONTHS>()
  , make_populate_handler<arrow::Type::INTERVAL_DAY_TIME>()
  , make_populate_handler<arrow::Type::LIST>()
  , make_populate_handler<arrow::Type::LARGE_LIST>()
  , make_populate_handler<arrow::Type::FIXED_SIZE_LIST>()
  , make_populate_handler<arrow::Type::MAP>()
  , make_populate_handler<arrow::Type::STRUCT>()
  , make_populate_handler<arrow::Type::SPARSE_UNION>()
  , make_populate_handler<arrow::Type::DENSE_UNION>()
};

} // namespace

namespace kx {
namespace arrowkdb {

// Populates data values from a kdb list into the specified array builder.
void PopulateBuilder(shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides)
{
  // Special cases for:
  // symbol - string or large_string
  // guid - fixed_size_binary(16)
  // char - uint8
  bool is_symbol = k_array->t == KS && (datatype->id() == arrow::Type::STRING || datatype->id() == arrow::Type::LARGE_STRING);
  bool is_guid = k_array->t == UU && datatype->id() == arrow::Type::FIXED_SIZE_BINARY && static_cast<arrow::FixedSizeBinaryBuilder*>(builder)->byte_width() == sizeof(U);
  bool is_char = k_array->t == KC && (datatype->id() == arrow::Type::UINT8 || datatype->id() == arrow::Type::INT8);

  // Type check the kdb structure
  if (!is_symbol && !is_guid && !is_char)
    TYPE_CHECK_ARRAY(GetKdbType(datatype, type_overrides) != k_array->t, datatype->ToString(), GetKdbType(datatype, type_overrides), k_array->t);

  auto type_id = datatype->id();
  if( PopulateHandlers.find( type_id ) == PopulateHandlers.end() )
  {
    TYPE_CHECK_UNSUPPORTED(datatype->ToString());
  }
  else
  {
    PopulateHandlers[type_id]( datatype, k_array, builder, type_overrides );
  }
}

// Construct a dictionary array from its values and indicies arrays.
//
// This is represented in kdb as a mixed list for the parent dictionary array
// containing the values and indicies sub-lists.
shared_ptr<arrow::Array> MakeDictionary(shared_ptr<arrow::DataType> datatype, K k_array, TypeMappingOverride& type_overrides)
{
  K values = kK(k_array)[0];
  K indicies = kK(k_array)[1];

  auto dictionary_type = static_pointer_cast<arrow::DictionaryType>(datatype);

  // Recursively construct the values and indicies arrays
  auto values_array = MakeArray(dictionary_type->value_type(), values, type_overrides);
  auto indicies_array = MakeArray(dictionary_type->index_type(), indicies, type_overrides);

  shared_ptr<arrow::Array> result;
  PARQUET_ASSIGN_OR_THROW(result, arrow::DictionaryArray::FromArrays(datatype, indicies_array, values_array));

  return result;
}

shared_ptr<arrow::Array> MakeArray(shared_ptr<arrow::DataType> datatype, K k_array, TypeMappingOverride& type_overrides)
{
  // DictionaryBuilder works in quite an unusual and non-standard way so just
  // construct the dictionary array directly
  if (datatype->id() == arrow::Type::DICTIONARY) 
    return MakeDictionary(datatype, k_array, type_overrides);

  // Construct a array builder for this datatype and populate it from the kdb
  // list
  auto builder = GetBuilder(datatype);
  PopulateBuilder(datatype, k_array, builder.get(), type_overrides);

  // Finalise the builder into the arrow array
  shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder->Finish(&array));
  return array;
}

} // namespace arrowkdb
} // namespace kx


K prettyPrintArray(K datatype_id, K array, K options)
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
  auto options = arrow::PrettyPrintOptions();
  string result;
  arrow::PrettyPrint(*arrow_array, options, &result);

  return kp((S)result.c_str());

  KDB_EXCEPTION_CATCH;
}
