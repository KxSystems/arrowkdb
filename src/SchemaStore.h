#ifndef __SCHEMA_STORE_H__
#define __SCHEMA_STORE_H__

#include <map>
#include <memory>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"
#include "GenericStore.h"


/**
 * @brief Returns the SchemaStore singleton which uses the GenericStore
 * template, specialised on std::shared_ptr<arrow::Schema>
 *
 * @return Pointer to the SchemaStore singleton
*/
GenericStore<std::shared_ptr<arrow::Schema>>* GetSchemaStore();

extern "C"
{
  ///////////////////////
  // SCHEMA MANAGEMENT //
  ///////////////////////

  /**
   * @brief Returns the list of identifiers for all arrow schemas currently held
   * in the SchemaStore.
   *
   * @param   unused 
   * @return  KI list of schema identifiers
  */
  EXP K listSchemas(K unused);

  /**
   * @brief Displays user readable information on the specified schema identifier
   *
   * @param schema_id Schema identifier to inspect
   * @return          Formatted string detailing the schema, error if not found
  */
  EXP K printSchema(K schema_id);

  /**
   * @brief Removes an arrow schema from the SchemaStore.  Any memory held
   * by the schema object will be released.
   *
   * @param schema_id Schema identifier to remove
   * @return          Error if not found
  */
  EXP K removeSchema(K schema_id);

  /**
   * @brief Checks whether two existing arrow schema are logically equal
   *
   * @param first_schema_id   Identifier of first schema
   * @param second_schema_id  Identifier of second schema
   * @return                  Bool result
  */
  EXP K equalSchemas(K first_schema_id, K second_schema_id);


  /////////////////////////
  // SCHEMA CONSTRUCTORS //
  /////////////////////////
  
  /**
   * @brief Constructs an arrow schema from its constituent list of field
   * identifiers
   *
   * @param field_ids List of field identifiers to be included in the schema
   * @return          Schema identifier
  */
  EXP K schema(K field_ids);

  /**
   * @brief Derives and constructs an arrow schema based on a kdb table or 
   * dictionary.
   *
   * Each column in the table is mapped to a field in the schema.  The column
   * name is used as the field name and the column's kdb type is mapped to an
   * arrow datatype as follows:
   *
   * Kdb type | Arrow datatype
   *  KB      |   boolean
   *  UU      |   fixed_size_binary(16)
   *  KG      |   int8
   *  KH      |   int16
   *  KI      |   int32
   *  KJ      |   int64
   *  KE      |   float32
   *  KF      |   float64
   *  KC      |   uint8
   *  KS      |   utf8
   *  KP      |   timestamp(nano)
   *  KD      |   date32
   *  KN      |   time64(nano)
   *  KT      |   time32(milli)
   *  99      |   map
   *  0 of KC |   utf8
   *  0 of KB |   binary
   *
   * Some kdb temporal types need to be cast to a different type that has a
   * cleaner mapping to an arrow datatype:
   *
   * Kdb type | Cast to
   *  KM      |   KD
   *  KU      |   KT
   *  KV      |   KT
   *  KZ      |   KP
   * 
   * Note that the derivation only works for a simple kdb table containing
   * trivial datatypes.  Only mixed lists of char arrays or byte arrays are
   * supported, mapped to arrow utf8 and binary datatypes respectively.  Other
   * mixed list structures (e.g. those used by the nested arrow datatypes)
   * cannot be interpreted - if required these should be created manually using
   * the datatype/field/schema constructors.
   *
   * @param table Kdb table or dictionary from which to derive the arrow schema
   * @return      Schema identifier
  */
  EXP K deriveSchema(K table);


  ///////////////////////
  // SCHEMA INSPECTION //
  ///////////////////////

  /**
   * @brief Returns the list of field identifiers used by the specified schema
   *
   * @param schema_id The schema identifier
   * @return          List of field identifiers used by that schema
  */
  EXP K schemaFields(K schema_id);
}

#endif // __SCHEMA_STORE_H__
