#ifndef __ARRAY_WRITER_H__
#define __ARRAY_WRITER_H__

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"
#include "HelperFunctions.h"


namespace kx {
namespace arrowkdb {

/**
 * @brief Populates data values from a kdb list into the specified array builder
 * 
 * @param datatype  Datatype of the arrow array
 * @param k_array   Kdb list data to be populated
 * @param builder   Arrow array builder for this datatype
*/
void PopulateBuilder(std::shared_ptr<arrow::DataType> datatype, K k_array, arrow::ArrayBuilder* builder, TypeMappingOverride& type_overrides);

/**
 * @brief Copies and converts a kdb list to an arrow array
 *
 * @param datatype  The datatype to use when creating the arrow array
 * @param k_array   The kdb list from which to source the data
 * @return          The arrow array
*/
std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::DataType> datatype, K k_array, TypeMappingOverride& type_overrides);

} // namespace arrowkdb
} // namespace kx


extern "C"
{
  /**
   * @brief Debugging function which converts a kdb list to an arrow array,
   * pretty prints the array into a buffer which is then returned back to kdb. 
   *
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param datatype_id The arrow datatype identifier to use for the intemediate
   * arrow array
   * @param array       The kdb list to be written to the intermediate arrow
   * array
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return            kdb char list containing the pretty printed buffer
  */
  EXP K prettyPrintArray(K datatype_id, K array, K options);
}

#endif // __ARRAY_WRITER_H__
