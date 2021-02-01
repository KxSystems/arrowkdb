#include <memory>
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


namespace kx {
namespace arrowkdb {

// Constructs and returns the correct arrow array builder for the specified
// datatype.
//
// This handles all datatypes except Dictionary which is handled separately.
std::shared_ptr<arrow::ArrayBuilder> GetBuilder(std::shared_ptr<arrow::DataType> datatype)
{
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  switch (datatype->id()) {
  case arrow::Type::NA:
    return std::make_shared<arrow::NullBuilder>(pool);
  case arrow::Type::BOOL:
    return std::make_shared<arrow::BooleanBuilder>(pool);
  case arrow::Type::UINT8:
    return std::make_shared<arrow::UInt8Builder>(pool);
  case arrow::Type::INT8:
    return std::make_shared<arrow::Int8Builder>(pool);
  case arrow::Type::UINT16:
    return std::make_shared<arrow::UInt16Builder>(pool);
  case arrow::Type::INT16:
    return std::make_shared<arrow::Int16Builder>(pool);
  case arrow::Type::UINT32:
    return std::make_shared<arrow::UInt32Builder>(pool);
  case arrow::Type::INT32:
    return std::make_shared<arrow::Int32Builder>(pool);
  case arrow::Type::UINT64:
    return std::make_shared<arrow::UInt64Builder>(pool);
  case arrow::Type::INT64:
    return std::make_shared<arrow::Int64Builder>(pool);
  case arrow::Type::HALF_FLOAT:
    return std::make_shared<arrow::HalfFloatBuilder>(pool);
  case arrow::Type::FLOAT:
    return std::make_shared<arrow::FloatBuilder>(pool);
  case arrow::Type::DOUBLE:
    return std::make_shared<arrow::DoubleBuilder>(pool);
  case arrow::Type::STRING:
    return std::make_shared<arrow::StringBuilder>(pool);
  case arrow::Type::LARGE_STRING:
    return std::make_shared<arrow::LargeStringBuilder>(pool);
  case arrow::Type::BINARY:
    return std::make_shared<arrow::BinaryBuilder>(pool);
  case arrow::Type::LARGE_BINARY:
    return std::make_shared<arrow::LargeBinaryBuilder>(pool);
  case arrow::Type::FIXED_SIZE_BINARY:
    return std::make_shared<arrow::FixedSizeBinaryBuilder>(datatype, pool);
  case arrow::Type::DATE32:
    return std::make_shared<arrow::Date32Builder>(pool);
  case arrow::Type::DATE64:
    return std::make_shared<arrow::Date64Builder>(pool);
  case arrow::Type::TIMESTAMP:
    return std::make_shared<arrow::TimestampBuilder>(datatype, pool);
  case arrow::Type::TIME32:
    return std::make_shared<arrow::Time32Builder>(datatype, pool);
  case arrow::Type::TIME64:
    return std::make_shared<arrow::Time64Builder>(datatype, pool);
  case arrow::Type::DECIMAL:
    return std::make_shared<arrow::Decimal128Builder>(datatype, pool);
  case arrow::Type::DURATION:
    return std::make_shared<arrow::DurationBuilder>(datatype, pool);
  case arrow::Type::INTERVAL_MONTHS:
    return std::make_shared<arrow::MonthIntervalBuilder>(pool);
  case arrow::Type::INTERVAL_DAY_TIME:
    return std::make_shared<arrow::DayTimeIntervalBuilder>(pool);
  case arrow::Type::LIST:
  case arrow::Type::LARGE_LIST:
  case arrow::Type::FIXED_SIZE_LIST:
  {
    // The parent list datatype details the child datatype so construct the child
    // builder and use it to initialise the parent list builder
    auto list_type = std::static_pointer_cast<arrow::BaseListType>(datatype);
    auto value_builder = GetBuilder(list_type->value_type());

    // Construct the correct listbuilder
    if (datatype->id() == arrow::Type::LIST)
      return std::make_shared<arrow::ListBuilder>(pool, value_builder);
    else if (datatype->id() == arrow::Type::LARGE_LIST)
      return std::make_shared<arrow::LargeListBuilder>(pool, value_builder);
    else
      return std::make_shared<arrow::FixedSizeListBuilder>(pool, value_builder, datatype);
  }
  case arrow::Type::MAP:
  {
    // The parent map datatype details the key/item child datatypes so construct
    // builders for both and use these to initialise the parent map builder
    auto map_type = std::static_pointer_cast<arrow::MapType>(datatype);
    auto key_builder = GetBuilder(map_type->key_type());
    auto item_builder = GetBuilder(map_type->item_type());
    return std::make_shared<arrow::MapBuilder>(pool, key_builder, item_builder);
  }
  case arrow::Type::STRUCT:
  {
    auto struct_type = std::static_pointer_cast<arrow::StructType>(datatype);

    // Iterate through all the fields in the struct constructing and adding each
    // field's builder into a vector
    auto fields = struct_type->fields();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    for (auto field : fields)
      field_builders.push_back(GetBuilder(field->type()));

    // Construct the parent struct builder from this vector of all the child
    // builders
    return std::make_shared<arrow::StructBuilder>(datatype, pool, field_builders);
  }
  case arrow::Type::SPARSE_UNION:
  case arrow::Type::DENSE_UNION:
  {
    auto union_type = std::static_pointer_cast<arrow::UnionType>(datatype);

    // Iterate through all the fields in the union constructing and adding each
    // field's builder into a vector
    auto fields = union_type->fields();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    for (auto field : fields)
      field_builders.push_back(GetBuilder(field->type()));

    // Construct the parent union builder from this vector of all the child
    // builders
    if (datatype->id() == arrow::Type::SPARSE_UNION)
      return std::make_shared<arrow::SparseUnionBuilder>(pool, field_builders, datatype);
    else
      return std::make_shared<arrow::DenseUnionBuilder>(pool, field_builders, datatype);
  }
  default:
    TYPE_CHECK_UNSUPPORTED(datatype->ToString());
  }
}

// Populate a list/large_list/fixed_size_list builder
//
// An arrow list array is a nested set of child lists.  This is represented in
// kdb as a mixed list for the parent list array containing a set of sub-lists,
// one for each of the list value sets.
template <typename ListBuilderType>
void PopulateListBuilder(std::shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder)
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
      auto fixed_list_type = std::static_pointer_cast<arrow::FixedSizeListType>(datatype);
      TYPE_CHECK_LENGTH(fixed_list_type->list_size() != list_data->n, datatype->ToString(), fixed_list_type->list_size(), list_data->n);
    }

    // Populate the child builder for this list set
    PopulateBuilder(value_builder->type(), kK(k_array)[i], value_builder);
  }
}

// Populate a sparse_union/dense_union builder
//
// An arrow union array is similar to a struct array except that it has an
// additional type id array which identifies the live field in each union value
// set.
template <typename UnionBuilderType>
void PopulateUnionBuilder(std::shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder)
{
  // Check that the mixed list length is at least one greater (the additional 
  // first sub-list contains the union type_ids) than the number of union 
  // fields
  auto union_type = std::static_pointer_cast<arrow::UnionType>(datatype);
  const auto min_length = union_type->num_fields() + 1;
  TYPE_CHECK_LENGTH(min_length > k_array->n, datatype->ToString(), min_length, k_array->n);

  // The first list contains the list of type_ids which denotes the index of the
  // 'live' child builder for each union value
  K type_ids = kK(k_array)[0];
  if (type_ids->t != KH)
    throw TypeCheck("union type_id list not KH");

  // Get all the child builders from the parent union builder
  auto union_builder = static_cast<UnionBuilderType*>(builder);
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
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
    PopulateBuilder(child_builders[builder_num]->type(), kK(k_array)[i], child_builders[builder_num].get());
  }

  // Check that all the populated child builders have the same length
  for (auto it : child_builders)
    if (it->length() != union_builder->length())
      throw TypeCheck("Mismatched union list lengths");
}

// Populates data values from a kdb list into the specified array builder.
void PopulateBuilder(std::shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder)
{
  // Type check the kdb structure, allowing symbols to be used for the arrow string types
  if ((datatype->id() != arrow::Type::STRING && datatype->id() != arrow::Type::LARGE_STRING) || k_array->t != KS)
    TYPE_CHECK_ARRAY(kx::arrowkdb::GetKdbType(datatype) != k_array->t, datatype->ToString(), kx::arrowkdb::GetKdbType(datatype), k_array->t);

  switch (datatype->id()) {
  case arrow::Type::NA: 
  {
    auto null_builder = static_cast<arrow::NullBuilder*>(builder);
    PARQUET_THROW_NOT_OK(null_builder->AppendNulls(k_array->n));
    break;
  }
  case arrow::Type::BOOL:
  {
    auto bool_builder = static_cast<arrow::BooleanBuilder*>(builder);
    PARQUET_THROW_NOT_OK(bool_builder->AppendValues((uint8_t*)kG(k_array), k_array->n));
    break;
  }
  case arrow::Type::UINT8:
  {
    auto uint8_builder = static_cast<arrow::UInt8Builder*>(builder);
    PARQUET_THROW_NOT_OK(uint8_builder->AppendValues((uint8_t*)kG(k_array), k_array->n));
    break;
  }
  case arrow::Type::INT8:
  {
    auto int8_builder = static_cast<arrow::Int8Builder*>(builder);
    PARQUET_THROW_NOT_OK(int8_builder->AppendValues((int8_t*)kG(k_array), k_array->n));
    break;
  }
  case arrow::Type::UINT16:
  {
    auto uint16_builder = static_cast<arrow::UInt16Builder*>(builder);
    PARQUET_THROW_NOT_OK(uint16_builder->AppendValues((uint16_t*)kH(k_array), k_array->n));
    arrow::Status s;
    break;
  }
  case arrow::Type::INT16:
  {
    auto int16_builder = static_cast<arrow::Int16Builder*>(builder);
    PARQUET_THROW_NOT_OK(int16_builder->AppendValues((int16_t*)kH(k_array), k_array->n));
    break;
  }
  case arrow::Type::UINT32:
  {
    auto uint32_builder = static_cast<arrow::UInt32Builder*>(builder);
    PARQUET_THROW_NOT_OK(uint32_builder->AppendValues((uint32_t*)kI(k_array), k_array->n));
    break;
  }
  case arrow::Type::INT32:
  {
    auto int32_builder = static_cast<arrow::Int32Builder*>(builder);
    PARQUET_THROW_NOT_OK(int32_builder->AppendValues((int32_t*)kI(k_array), k_array->n));
    break;
  }
  case arrow::Type::UINT64:
  {
    auto uint64_builder = static_cast<arrow::UInt64Builder*>(builder);
    PARQUET_THROW_NOT_OK(uint64_builder->AppendValues((uint64_t*)kJ(k_array), k_array->n));
    break;
  }
  case arrow::Type::INT64:
  {
    auto int64_builder = static_cast<arrow::Int64Builder*>(builder);
    PARQUET_THROW_NOT_OK(int64_builder->AppendValues((int64_t*)kJ(k_array), k_array->n));
    break;
  }
  case arrow::Type::HALF_FLOAT:
  {
    arrow::HalfFloatType hft;
    auto hfl_builder = static_cast<arrow::HalfFloatBuilder*>(builder);
    PARQUET_THROW_NOT_OK(hfl_builder->AppendValues((uint16_t*)kH(k_array), k_array->n));
    break;
  }
  case arrow::Type::FLOAT:
  {
    auto fl_builder = static_cast<arrow::FloatBuilder*>(builder);
    PARQUET_THROW_NOT_OK(fl_builder->AppendValues(kE(k_array), k_array->n));
    break;
  }
  case arrow::Type::DOUBLE:
  {
    auto dbl_builder = static_cast<arrow::DoubleBuilder*>(builder);
    PARQUET_THROW_NOT_OK(dbl_builder->AppendValues(kF(k_array), k_array->n));
    break;
  }
  case arrow::Type::STRING:
  {
    auto str_builder = static_cast<arrow::StringBuilder*>(builder);
    if (k_array->t == KS) {
      // Populate from symbol list
      for (auto i = 0; i < k_array->n; ++i)
        PARQUET_THROW_NOT_OK(str_builder->Append(kS(k_array)[i]));
    } else {
      // Populate from mixed list of char lists
      for (auto i = 0; i < k_array->n; ++i) {
        K str_data = kK(k_array)[i];
        TYPE_CHECK_ITEM(str_data->t != KC, datatype->ToString(), KC, str_data->t);
        PARQUET_THROW_NOT_OK(str_builder->Append(kG(str_data), str_data->n));
      }
    }
    break;
  }
  case arrow::Type::LARGE_STRING:
  {
    auto str_builder = static_cast<arrow::LargeStringBuilder*>(builder);
    if (k_array->t == KS) {
      // Populate from symbol list
      for (auto i = 0; i < k_array->n; ++i)
        PARQUET_THROW_NOT_OK(str_builder->Append(kS(k_array)[i]));
    } else {
      // Populate from mixed list of char lists
      for (auto i = 0; i < k_array->n; ++i) {
        K str_data = kK(k_array)[i];
        TYPE_CHECK_ITEM(str_data->t != KC, datatype->ToString(), KC, str_data->t);
        PARQUET_THROW_NOT_OK(str_builder->Append(kG(str_data), str_data->n));
      }
    }
    break;
  }
  case arrow::Type::BINARY:
  {
    auto bin_builder = static_cast<arrow::BinaryBuilder*>(builder);
    for (auto i = 0; i < k_array->n; ++i) {
      K bin_data = kK(k_array)[i];
      TYPE_CHECK_ITEM(bin_data->t != KG, datatype->ToString(), KG, bin_data->t);
      PARQUET_THROW_NOT_OK(bin_builder->Append(kG(bin_data), bin_data->n));
    }
    break;
  }
  case arrow::Type::LARGE_BINARY:
  {
    auto bin_builder = static_cast<arrow::LargeBinaryBuilder*>(builder);
    for (auto i = 0; i < k_array->n; ++i) {
      K bin_data = kK(k_array)[i];
      TYPE_CHECK_ITEM(bin_data->t != KG, datatype->ToString(), KG, bin_data->t);
      PARQUET_THROW_NOT_OK(bin_builder->Append(kG(bin_data), bin_data->n));
    }
    break;
  }
  case arrow::Type::FIXED_SIZE_BINARY:
  {
    auto fixed_bin_builder = static_cast<arrow::FixedSizeBinaryBuilder*>(builder);
    for (auto i = 0; i < k_array->n; ++i) {
      K bin_data = kK(k_array)[i];
      TYPE_CHECK_ITEM(bin_data->t != KG, datatype->ToString(), KG, bin_data->t);
      TYPE_CHECK_LENGTH(fixed_bin_builder->byte_width() != bin_data->n, builder->type()->ToString(), fixed_bin_builder->byte_width(), bin_data->n);
      PARQUET_THROW_NOT_OK(fixed_bin_builder->Append(kG(bin_data)));
    }
    break;
  }
  case arrow::Type::DATE32:
  {
    auto d32_builder = static_cast<arrow::Date32Builder*>(builder);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(d32_builder->Append(KDate_Date32(kI(k_array)[i])));
    break;
  }
  case arrow::Type::DATE64:
  {
    auto d64_builder = static_cast<arrow::Date64Builder*>(builder);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(d64_builder->Append(KTimestamp_Date64(kJ(k_array)[i])));
    break;
  }
  case arrow::Type::TIMESTAMP:
  {
    auto ts_builder = static_cast<arrow::TimestampBuilder*>(builder);
    auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(datatype);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(ts_builder->Append(KTimestamp_Timestamp(timestamp_type, kJ(k_array)[i])));
    break;
  }
  case arrow::Type::TIME32:
  {
    auto t32_builder = static_cast<arrow::Time32Builder*>(builder);
    auto time32_type = std::static_pointer_cast<arrow::Time32Type>(datatype);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(t32_builder->Append(KTime_Time32(time32_type, kI(k_array)[i])));
    break;
  }
  case arrow::Type::TIME64:
  {
    auto t64_builder = static_cast<arrow::Time64Builder*>(builder);
    auto time64_type = std::static_pointer_cast<arrow::Time64Type>(datatype);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(t64_builder->Append(KTimespan_Time64(time64_type, kJ(k_array)[i])));
    break;
  }
  case arrow::Type::DECIMAL:
  {
    auto dec_builder = static_cast<arrow::Decimal128Builder*>(builder);
    for (auto i = 0; i < k_array->n; ++i) {
      // Each decimal is a list of 16 bytes
      K k_dec = kK(k_array)[i];
      TYPE_CHECK_LENGTH(k_dec->n != 16, datatype->ToString(), 16, k_dec->n);
      TYPE_CHECK_ITEM(k_dec->t != KG, datatype->ToString(), KG, k_dec->t);

      arrow::BasicDecimal128 dec128((const uint8_t*)kG(k_dec));
      PARQUET_THROW_NOT_OK(dec_builder->Append(dec128));
    }
    break;
  }
  case arrow::Type::DURATION:
  {
    auto dur_builder = static_cast<arrow::DurationBuilder*>(builder);
    auto duration_type = std::static_pointer_cast<arrow::DurationType>(datatype);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(dur_builder->Append(KTimespan_Duration(duration_type, kJ(k_array)[i])));
    break;
  }
  case arrow::Type::INTERVAL_MONTHS:
  {
    auto month_builder = static_cast<arrow::MonthIntervalBuilder*>(builder);
    PARQUET_THROW_NOT_OK(month_builder->AppendValues((int32_t*)kI(k_array), k_array->n));
    break;
  }
  case arrow::Type::INTERVAL_DAY_TIME:
  {
    auto dt_builder = static_cast<arrow::DayTimeIntervalBuilder*>(builder);
    for (auto i = 0; i < k_array->n; ++i)
      PARQUET_THROW_NOT_OK(dt_builder->Append(KTimespan_DayTimeInterval(kJ(k_array)[i])));
    break;
  }
  case arrow::Type::LIST:
    PopulateListBuilder<arrow::ListBuilder>(datatype, k_array, builder);
    break;
  case arrow::Type::LARGE_LIST:
    PopulateListBuilder<arrow::LargeListBuilder>(datatype, k_array, builder);
    break;
  case arrow::Type::FIXED_SIZE_LIST:
    PopulateListBuilder<arrow::FixedSizeListBuilder>(datatype, k_array, builder);
    break;
  case arrow::Type::MAP:
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
      PopulateBuilder(key_builder->type(), kK(k_dict)[0], key_builder);
      PopulateBuilder(item_builder->type(), kK(k_dict)[1], item_builder);
    }
    break;
  }

  case arrow::Type::STRUCT:
  {
    // An arrow struct array is a logical grouping of child arrays with each
    // child array corresponding to one of the fields in the struct.  A single
    // struct value is obtaining by slicing across all the child arrays at a
    // given index.  This is represented in kdb as a mixed list for the parent
    // struct array, containing child lists for each field in the struct.
    //
    // Check that the mixed list length is at least equal to the number of struct fields 
    auto struct_type = std::static_pointer_cast<arrow::StructType>(datatype);
    TYPE_CHECK_LENGTH(struct_type->num_fields() > k_array->n, datatype->ToString(), struct_type->num_fields(), k_array->n);

    // Get all the field builders from the parent struct builder    
    auto struct_builder = static_cast<arrow::StructBuilder*>(builder);
    std::vector<arrow::ArrayBuilder*> field_builders;
    for (auto i = 0; i < struct_builder->num_fields(); ++i)
      field_builders.push_back(struct_builder->field_builder(i));

    // Delimit each struct value in the parent builder
    for (auto index = 0; index < kK(k_array)[0]->n; ++index)
      struct_builder->Append();

    // Populate each of the field builders from its kdb list.  Only count up to
    // the number of struct fields.  Additional trailing data in the kdb mixed
    // list is ignored (to allow for ::)
    for (auto i = 0; i < struct_type->num_fields(); ++i)
      PopulateBuilder(field_builders[i]->type(), kK(k_array)[i], field_builders[i]);

    // Check that all the populated field builders have the same length.
    for (auto it : field_builders)
      if (it->length() != struct_builder->length())
        throw TypeCheck("Mismatched struct list lengths");

    break;
  }
  case arrow::Type::SPARSE_UNION:
    PopulateUnionBuilder<arrow::SparseUnionBuilder>(datatype, k_array, builder);
    break;
  case arrow::Type::DENSE_UNION:
    PopulateUnionBuilder<arrow::DenseUnionBuilder>(datatype, k_array, builder);
    break;
  default:
    TYPE_CHECK_UNSUPPORTED(datatype->ToString());
  }
}

// Construct a dictionary array from its values and indicies arrays.
//
// This is represented in kdb as a mixed list for the parent dictionary array
// containing the values and indicies sub-lists.
std::shared_ptr<arrow::Array> MakeDictionary(std::shared_ptr<arrow::DataType> datatype, K k_array)
{
  K values = kK(k_array)[0];
  K indicies = kK(k_array)[1];

  auto dictionary_type = std::static_pointer_cast<arrow::DictionaryType>(datatype);

  // Recursively construct the values and indicies arrays
  auto values_array = MakeArray(dictionary_type->value_type(), values);
  auto indicies_array = MakeArray(dictionary_type->index_type(), indicies);

  std::shared_ptr<arrow::Array> result;
  PARQUET_ASSIGN_OR_THROW(result, arrow::DictionaryArray::FromArrays(datatype, indicies_array, values_array));

  return result;
}

std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::DataType> datatype, K k_array)
{
  // DictionaryBuilder works in quite an unusual and non-standard way so just
  // construct the dictionary array directly
  if (datatype->id() == arrow::Type::DICTIONARY) 
    return MakeDictionary(datatype, k_array);

  // Construct a array builder for this datatype and populate it from the kdb
  // list
  auto builder = GetBuilder(datatype);
  PopulateBuilder(datatype, k_array, builder.get());

  // Finalise the builder into the arrow array
  std::shared_ptr<arrow::Array> array;
  PARQUET_THROW_NOT_OK(builder->Finish(&array));
  return array;
}

} // namespace arrowkdb
} // namespace kx


K prettyPrintArray(K datatype_id, K array)
{
  KDB_EXCEPTION_TRY;

  if (datatype_id->t != -KI)
    return krr((S)"datatype_id not -6h");

  auto datatype = kx::arrowkdb::GetDatatypeStore()->Find(datatype_id->i);
  if (!datatype)
    return krr((S)"datatype not found");

  auto arrow_array = kx::arrowkdb::MakeArray(datatype, array);
  auto options = arrow::PrettyPrintOptions();
  std::string result;
  arrow::PrettyPrint(*arrow_array, options, &result);

  return kp((S)result.c_str());

  KDB_EXCEPTION_CATCH;
}
