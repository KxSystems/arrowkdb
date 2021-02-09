#include <iostream>

#include "FieldStore.h"
#include "DatatypeStore.h"
#include "HelperFunctions.h"


namespace kx {
namespace arrowkdb {

template<>
GenericStore<std::shared_ptr<arrow::Field>>* GenericStore<std::shared_ptr<arrow::Field>>::instance = nullptr;

GenericStore<std::shared_ptr<arrow::Field>>* GetFieldStore()
{
  return GenericStore<std::shared_ptr<arrow::Field>>::Instance();
}

} // namespace arrowkdb
} // namespace kx


K listFields(K unused)
{
  auto field_ids = kx::arrowkdb::GetFieldStore()->List();

  K result = ktn(KI, field_ids.size());
  size_t index = 0;
  for (auto it : field_ids)
    kI(result)[index++] = it;

  return result;
}

K printField(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -6h");

  auto field = kx::arrowkdb::GetFieldStore()->Find(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return kp((S)field->ToString().c_str());
}

K removeField(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -6h");

  auto field = kx::arrowkdb::GetFieldStore()->Remove(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return (K)0;
}

K equalFields(K first_field_id, K second_field_id)
{
  if (first_field_id->t != -KI)
    return krr((S)"first_field_id not -6h");
  if (second_field_id->t != -KI)
    return krr((S)"second_field_id not -6h");

  auto first_field = kx::arrowkdb::GetFieldStore()->Find(first_field_id->i);
  if (!first_field)
    return krr((S)"first field not found");
  auto second_field = kx::arrowkdb::GetFieldStore()->Find(second_field_id->i);
  if (!second_field)
    return krr((S)"second field not found");

  return kb(first_field->Equals(second_field));
}

K fieldName(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -6h");

  auto field = kx::arrowkdb::GetFieldStore()->Find(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return ks((S)field->name().c_str());
}

K fieldDatatype(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -6h");

  auto field = kx::arrowkdb::GetFieldStore()->Find(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return ki(kx::arrowkdb::GetDatatypeStore()->ReverseFind(field->type()));
}

K field(K field_name, K datatype_id)
{
  if (!kx::arrowkdb::IsKdbString(field_name))
    return krr((S)"field_name not -11|10h");
  if (datatype_id->t != -KI)
    return krr((S)"datatype_id not -6h");

  auto datatype = kx::arrowkdb::GetDatatypeStore()->Find(datatype_id->i);
  if (!datatype)
    return krr((S)"datatype not found");

  // Converting between kdb nulls are arrow nulls would incur a massive
  // performance hit (up to 10x worse with trival datatypes that could otherwise
  // be memcpy'ed).  Also, not all kdb types have a null value, e.g. KB, KG, KS,
  // 0 of KC, 0 of KG, etc.  So don't allow fields to be created as nullable
  // (other than NA type which is all nulls).
  bool nullable = datatype->id() == arrow::Type::NA;
  return ki(kx::arrowkdb::GetFieldStore()->Add(arrow::field(kx::arrowkdb::GetKdbString(field_name), datatype, nullable)));
}