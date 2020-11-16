#ifndef __FIELD_STORE_H__
#define __FIELD_STORE_H__

#include <map>
#include <memory>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"
#include "GenericStore.h"


/**
 * @brief Returns the FieldStore singleton which uses the GenericStore
 * template, specialised on std::shared_ptr<arrow::Field>
 *
 * @return Pointer to the FieldStore singleton
*/
GenericStore<std::shared_ptr<arrow::Field>>* GetFieldStore();

extern "C"
{
  //////////////////////
  // FIELD MANAGEMENT //
  //////////////////////

  /**
   * @brief Returns the list of identifiers for all arrow fields currently held
   * in the FieldStore
   *
   * @param   unused 
   * @return  KI list of field identifiers 
  */
  EXP K listFields(K unused);

  /**
   * @brief Displays user readable information on the specified field identifier
   *
   * @param field_id  Field identifier to inspect
   * @return          Formatted string detailing the field, error if not found
  */
  EXP K printField(K field_id);

  /**
   * @brief Removes an arrow field from the FieldStore.  Any memory held
   * by the field object will be released.
   *
   * @param field_id  Field identifier to be removed
   * @return          Error if not found
  */
  EXP K removeField(K field_id);

  /**
   * @brief Checks whether two existing arrow fields are logically equal
   *
   * @param first_field_id  Identifier of first field
   * @param second_field_id Identifier of second field
   * @return                Bool result
  */
  EXP K equalFields(K first_field_id, K second_field_id);


  ///////////////////////
  // FIELD CONSTRUCTOR //
  ///////////////////////

  /**
   * @brief Constructs an arrow field from its name and datatype
   *
   * @param field_name  Field name symbol
   * @param datatype_id The datatype identifier for the field
   * @return            Field identifier
  */
  EXP K field(K field_name, K datatype_id);


  //////////////////////
  // FIELD INSPECTION //
  //////////////////////

  /**
   * @brief Returns the field name of the specified field identifier
   *
   * @param field_id  The field identifier
   * @return          Field name symbol
  */
  EXP K fieldName(K field_id);

  /**
   * @brief Return the datatype identifier of the specified field identifier
   *
   * @param field_id  The field identifier
   * @return          The datatype identifier
  */
  EXP K fieldDatatype(K field_id);
}

#endif // __FIELD_STORE_H__
