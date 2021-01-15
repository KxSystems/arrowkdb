#ifndef __ARRAY_WRITER_H__
#define __ARRAY_WRITER_H__

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"


/**
 * @brief Populates data values from a kdb list into the specified array builder
 * 
 * @param datatype  Datatype of the arrow array
 * @param k_array   Kdb list data to be populated
 * @param builder   Arrow array builder for this datatype
*/
void PopulateBuilder(std::shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder);

/**
 * @brief Copies and converts a kdb list to an arrow array
 *
 * @param datatype  The datatype to use when creating the arrow array
 * @param k_array   The kdb list from which to source the data
 * @return          The arrow array
*/
std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::DataType> datatype, K k_array);

extern "C"
{
  /**
   * @brief Debugging function which converts a kdb list to an arrow array,
   * pretty prints the array into a buffer which is then returned back to kdb. 
   *
   * @param datatype_id The arrow datatype identifier to use for the intemediate
   * arrow array
   * @param array       The kdb list to be written to the intermediate arrow
   * array
   * @return            kdb char list containing the pretty printed buffer
  */
  EXP K prettyPrintArray(K datatype_id, K array);
}

#endif // __ARRAY_WRITER_H__
