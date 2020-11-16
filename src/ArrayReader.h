#ifndef __ARRAY_READER_H__
#define __ARRAY_READER_H__

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"


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
 * begin.  Index will be updated to account for the new offset by adding the
 * length of the array array. 
*/
void AppendArray(std::shared_ptr<arrow::Array> array_data, K k_array, size_t& index);

/**
 * @brief Copies and converts an arrow array to a kdb list
 *
 * @param array The arrow array to be converted
 * @return      A kdb list represented the arrow array
*/
K ReadArray(std::shared_ptr<arrow::Array> array);

/**
 * @brief An arrow chunked array is a set of sub-arrays which are logically but not
 * physically contiguous.  To convert to kdb requires creating the kdb list with
 * the total chunked array length first, then appending the data from each chunk
 * into the list.
 *
 * @param chunked_array The chunked array to be converted
 * @return              A kdb list representing the chunked array
*/
K ReadChunkedArray(std::shared_ptr<arrow::ChunkedArray> chunked_array);

/**
 * @brief Creates a kdb list of the correct type and specified length according
 * to the arrow datatype.  For the arrow struct/union datatypes this includes
 * creating the necessary chield field lists.
 *
 * @param datatype  The arrow datatype to be stored in the kdb list
 * @param length    The required length of the kdb list
 * @return          Newly created kdb list
*/
K InitKdbForArray(std::shared_ptr<arrow::DataType> datatype, size_t length);


extern "C"
{
  /**
   * @brief Debugging function which converts a kdb list to an arrow array then
   * converts it back again.
   *
   * @param datatype_id The arrow datatype identifier to use for the intemediate
   * arrow array
   * @param array       The kdb list to be written to the intermediate arrow
   * array
   * @return            The kdb list created from the intermediate arrow array
  */
  EXP K writeReadArray(K datatype_id, K array);
}

#endif // __ARRAY_READER_H__
