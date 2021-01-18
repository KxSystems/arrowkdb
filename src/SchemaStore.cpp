#include <iostream>

#include "SchemaStore.h"
#include "FieldStore.h"
#include "HelperFunctions.h"


template<>
GenericStore<std::shared_ptr<arrow::Schema>>* GenericStore<std::shared_ptr<arrow::Schema>>::instance = nullptr;
GenericStore<std::shared_ptr<arrow::Schema>>* GetSchemaStore()
{
  return GenericStore<std::shared_ptr<arrow::Schema>>::Instance();
}

K listSchemas(K unused)
{
  auto schema_ids = GetSchemaStore()->List();

  K result = ktn(KI, schema_ids.size());
  size_t index = 0;
  for (auto it : schema_ids)
    kI(result)[index++] = it;

  return result;
}

K printSchema(K schema_id)
{
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"schema not found");

  return kp((S)schema->ToString().c_str());
}

K removeSchema(K schema_id)
{
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = GetSchemaStore()->Remove(schema_id->i);
  if (!schema)
    return krr((S)"schema not found");

  return (K)0;
}

K equalSchemas(K first_schema_id, K second_schema_id)
{
  if (first_schema_id->t != -KI)
    return krr((S)"first_schema_id not -6h");
  if (second_schema_id->t != -KI)
    return krr((S)"second_schema_id not -6h");

  auto first_schema = GetSchemaStore()->Find(first_schema_id->i);
  if (!first_schema)
    return krr((S)"first schema not found");
  auto second_schema = GetSchemaStore()->Find(second_schema_id->i);
  if (!second_schema)
    return krr((S)"second schema not found");

  return kb(first_schema->Equals(second_schema));
}

K schemaFields(K schema_id)
{
  if (schema_id->t != -KI)
    return krr((S)"schema_id not -6h");

  auto schema = GetSchemaStore()->Find(schema_id->i);
  if (!schema)
    return krr((S)"schema not found");

  auto fields = schema->fields();
  K k_fields = ktn(KI, fields.size());
  for (auto i = 0; i < fields.size(); ++i)
    kI(k_fields)[i] = GetFieldStore()->ReverseFind(fields[i]);

  return k_fields;
}

K schema(K field_ids)
{
  if (field_ids->t != KI)
    return krr((S)"field_ids not 6h");

  arrow::FieldVector fields;
  for (auto i = 0; i < field_ids->n; ++i) {
    auto field = GetFieldStore()->Find(kI(field_ids)[i]);
    if (!field)
      return krr((S)"field_id not found");
    fields.push_back(field);
  }

  return ki(GetSchemaStore()->Add(arrow::schema(fields)));
}

K deriveSchema(K table)
{
  if (table->t != 99 && table->t != 98)
    return krr((S)"table not 98|99h");
  
  K dict;
  if (table->t == 98)
    dict = table->k;
  else
    dict = table;

  // Get the list of field names from either a symbol list or mixed list of
  // strings
  K k_field_names = kK(dict)[0];
  std::vector<std::string> field_names;
  if (k_field_names->t != KS && k_field_names->t != 0)
    return krr((S)"table key list not 11h or 0 of 10h");
  if (k_field_names->t == KS)
    // k_field_names is symbol list
    for (auto i = 0; i < k_field_names->n; ++i)
      field_names.push_back(kS(k_field_names)[i]);
  else
    // k_field_names is a mixed list
    for (auto i = 0; i < k_field_names->n; ++i) {
      K k_field = kK(k_field_names)[i];
      if (k_field->t == KC)
        field_names.push_back(std::string((S)kG(k_field), k_field->n));
      else if (k_field->t == -KS)
        field_names.push_back(k_field->s);
      else
        return krr((S)"table key value not -11|10h");
    }

  KDB_EXCEPTION_TRY;

  // Determine the arrow datatype for each data set
  K k_array_data = kK(dict)[1];
  assert(k_array_data->n == field_names.size());
  arrow::FieldVector fields;
  for (auto i = 0; i < field_names.size(); ++i) {
    auto datatype = GetArrowType(kK(k_array_data)[i]);
    // Construct each arrow field
    fields.push_back(arrow::field(field_names[i], datatype));
  }

  // Create the schema with these fields
  return ki(GetSchemaStore()->Add(arrow::schema(fields)));

  KDB_EXCEPTION_CATCH;
}
