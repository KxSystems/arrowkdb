#include <iostream>

#include "FieldStore.h"
#include "DatatypeStore.h"


template<>
GenericStore<std::shared_ptr<arrow::Field>>* GenericStore<std::shared_ptr<arrow::Field>>::instance = nullptr;
GenericStore<std::shared_ptr<arrow::Field>>* GetFieldStore()
{
  return GenericStore<std::shared_ptr<arrow::Field>>::Instance();
}

K listFields(K unused)
{
  auto field_ids = GetFieldStore()->List();

  K result = ktn(KI, field_ids.size());
  size_t index = 0;
  for (auto it : field_ids)
    kI(result)[index++] = it;

  return result;
}

K printField(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -KI");

  auto field = GetFieldStore()->Find(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return kp((S)field->ToString().c_str());
}

K removeField(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -KI");

  auto field = GetFieldStore()->Remove(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return (K)0;
}

K equalFields(K first_field_id, K second_field_id)
{
  if (first_field_id->t != -KI)
    return krr((S)"first_field_id not -KI");
  if (second_field_id->t != -KI)
    return krr((S)"second_field_id not -KI");

  auto first_field = GetFieldStore()->Find(first_field_id->i);
  if (!first_field)
    return krr((S)"first field not found");
  auto second_field = GetFieldStore()->Find(second_field_id->i);
  if (!second_field)
    return krr((S)"second field not found");

  return kb(first_field->Equals(second_field));
}

K fieldName(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -KI");

  auto field = GetFieldStore()->Find(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return ks((S)field->name().c_str());
}

K fieldDatatype(K field_id)
{
  if (field_id->t != -KI)
    return krr((S)"field_id not -KI");

  auto field = GetFieldStore()->Find(field_id->i);
  if (!field)
    return krr((S)"field not found");

  return ki(GetDatatypeStore()->ReverseFind(field->type()));
}

K field(K field_name, K datatype_id)
{
  if (field_name->t != -KS)
    return krr((S)"field_name not -KS");
  if (datatype_id->t != -KI)
    return krr((S)"datatype_id not -KI");

  auto datatype = GetDatatypeStore()->Find(datatype_id->i);
  if (!datatype)
    return krr((S)"datatype not found");

  // @@@
  // We could prevent fields being defined as nullable.  But then what do you do
  // with nullable fields in externlly loaded schemas: nothing, warning, error?
  // See also SchemaContainsNullable()
  bool nullable = true;
  return ki(GetFieldStore()->Add(arrow::field(field_name->s, datatype, nullable)));
}