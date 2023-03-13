#ifndef __ARRAY_READER_H__
#define __ARRAY_READER_H__

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"
#include "HelperFunctions.h"


namespace kx {
namespace arrowkdb {

/**
 * @brief Appends data from an arrow array into an existing kdb list starting at
 * the specified index.
 *
 * @param array_data  The arrow array from which to source the data.  The entire
 * array will be appended.
 * @param k_array     The kdb list that the data should be inserted into.  This
 * list needs to have been created with the correct length by the calling
 * function.
 * @param index       The index into the kdb list at which the appending should
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * begin.  Index will be updated to account for the new offset by adding the
 * length of the array array.
*/
void AppendArray(std::shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides);
void AppendArrayNullBitmap(std::shared_ptr<arrow::Array> array_data, K k_array, size_t& index, TypeMappingOverride& type_overrides);

/**
 * @brief Copies and converts an arrow array to a kdb list
 *
 * @param array The arrow array to be converted
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * @return      A kdb list represented the arrow array
*/
K ReadArray(std::shared_ptr<arrow::Array> array, TypeMappingOverride& type_overrides);
K ReadArrayNullBitmap(std::shared_ptr<arrow::Array> array, TypeMappingOverride& type_overrides);

/**
 * @brief An arrow chunked array is a set of sub-arrays which are logically but not
 * physically contiguous.  To convert to kdb requires creating the kdb list with
 * the total chunked array length first, then appending the data from each chunk
 * into the list.
 *
 * @param chunked_array The chunked array to be converted
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * @return              A kdb list representing the chunked array
*/
K ReadChunkedArray(std::shared_ptr<arrow::ChunkedArray> chunked_array, TypeMappingOverride& type_overrides);

/**
 * @brief Extracts nulls bitmap of an arrow array into a boolean kdb list
 *
 * @param chunked_array The chunked array to be converted
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * @return              A kdb list representing the nulls bitmap
*/
K ReadChunkedArrayNullBitmap( std::shared_ptr<arrow::ChunkedArray> chunked_array, TypeMappingOverride& type_overrides );

/**
 * @brief Creates a kdb list of the correct type and specified length according
 * to the arrow datatype.  For the arrow struct/union datatypes this includes
 * creating the necessary chield field lists.
 *
 * @param datatype  The arrow datatype to be stored in the kdb list
 * @param length    The required length of the kdb list
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * @return          Newly created kdb list
*/
K InitKdbForArray(std::shared_ptr<arrow::DataType> datatype, size_t length, TypeMappingOverride& type_overrides, GetKdbTypeCommon get_kdb_type);

/**
 * @brief Appends null bitmap data from an arrow array into an existing kdb boolean
 * list starting at the specified index.
 *
 * @param array_data  The arrow array from which to source the data.  The entire
 * array will be appended.
 * @param k_bitmap     The kdb boolean list that the data should be inserted into.
 * This list needs to have been created with the correct length by the calling
 * function.
 * @param index       The index into the kdb list at which the appending should
 * begin.  Index will be updated to account for the new offset by adding the
 * length of the array array.
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * In null bitmap is used for overriding key types of unions
*/
void InitKdbNullBitmap( std::shared_ptr<arrow::Array> array_data, K* k_bitmap, size_t& index, TypeMappingOverride& type_overrides );

/**
 * @brief Appends null bitmap data from an arrow array into an existing kdb boolean
 * list starting at the specified index.
 *
 * @param array_data  The arrow array from which to source the data.  The entire
 * array will be appended.
 * @param k_bitmap     The kdb boolean list that the data should be inserted into.
 * This list needs to have been created with the correct length by the calling
 * function.
 * @param index       The index into the kdb list at which the appending should
 * begin.  Index will be updated to account for the new offset by adding the
 * length of the array array.
 * @param type_overrides  Overrides for type mappings configured by KdbOptions
 * In null bitmap is used for overriding key types of unions
*/
void InitKdbNullBitmap( std::shared_ptr<arrow::Array> array_data, K* k_bitmap, size_t& index, TypeMappingOverride& type_overrides );

} // namespace arrowkdb
} // namespace kx


extern "C"
{
  /**
   * @brief Debugging function which converts a kdb list to an arrow array then
   * converts it back again.
   *
   * Developer use only - Only useful for manual testing, do not expose in
   * release version of arrowkdb.q since it has no practical use
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
   * @return            The kdb list created from the intermediate arrow array
  */
  EXP K writeReadArray(K datatype_id, K array, K options);
}

#endif // __ARRAY_READER_H__
