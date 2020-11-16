#ifndef __TABLE_DATA_H__
#define __TABLE_DATA_H__

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"


extern "C"
{
  /**
   * @brief Debugging function which converts a kdb mixed list of arrow array
   * data objects to an arrow table, pretty prints the table into a buffer which
   * is then returned back to kdb.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * @param schema_id   The schema identifier to use for the intermediate arrow
   * table
   * @param array_data  Mixed list of arrow array data to be written to the
   * intermediate arrow table
   * @return            kdb char list containing the pretty printed buffer
  */
  EXP K prettyPrintTable(K schema_id, K array_data);

  /**
   * @brief Debugging function which converts a kdb mixed list of arrow array
   * data objects to an arrow table then converts it back again.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * @param schema_id   The schema identifier to use for the intermediate arrow
   * table
   * @param array_data  Mixed list of arrow array data to be written to the
   * intermediate arrow table
   * @return            The kdb mixed list created from the intermediate arrow
   * table
  */
  EXP K writeReadTable(K schema_id, K array_data);

  /**
   * @brief Sets the chunk size to use when writing parquet files.  This
   * controls the approximate size of encoded data pages within a column chunk.
   * Defaults to 1MB.
   *
   * @param chunk_size  Chunk size required
   * @return            NULL
  */
  EXP K setParquetChunkSize(K chunk_size);
  
  /**
   * @brief Gets the configured chunk size used when writing parquet files.
   *
   * @return Current chunk size
  */
  EXP K getParquetChunkSize(K unused);

  /**
   * @brief Sets whether the parquet file reader should operate in
   * multi-threaded mode.  This can improve performance by processing multiple
   * columns in parallel.  By default this is set to FALSE.
   *
   * @param mt_flag Bool flag specifing whether to use multiple threads
   * @return        NULL
  */
  EXP K setParquetMultithreadedRead(K mt_flag);

  /**
   * @brief Returns the current parquet reader threading mode
   *
   * @return  TRUE if running with mulitple threads, FALSE otherwise
  */
  EXP K getParquetMultithreadedRead(K unused);

  /**
   * @brief Creates a parquet file with the specified arrow schema and populates
   * it from a mixed list of arrow array objects.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * Note that in general parquet only supports a subset of the the arrow
   * datatypes with more limited functionality.  For example the only supported
   * nested datatypes are top level lists and structs (without further nesting).
   * Similarly temporal datatypes with TimeUnit parameters only support MILLI or
   * MICRO granularity.  In such cases the parquet/arrow file writer will return
   * an error.
   *
   * @param parquet_file  String name of the parquet file to write
   * @param schema_id     The schema identifier
   * @param array_data    Mixed list of arrow array data to be written to the
   * file
   * @return              NULL on success, error otherwise
  */
  EXP K writeParquet(K parquet_file, K schema_id, K array_data);

  /**
   * @brief Reads the arrow schema from the specified parquet file
   *
   * @param parquet_file  String name of the parquet file to read
   * @return              Schema identifier
  */
  EXP K readParquetSchema(K parquet_file);

  /**
   * @brief Reads the arrow array data from the specified parquet file
   *
   * @param parquet_file  String name of the parquet file to read
   * @return              Mixed list of arrow array objects
  */
  EXP K readParquetData(K parquet_file);

  /**
   * @brief Creates an arrow file with the specified arrow schema and populates
   * it from a mixed list of arrow array objects.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * @param arrow_file  String name of the arrow file to write
   * @param schema_id   The schema identifier
   * @param array_data  Mixed list of arrow array data to be written to the file
   * @return            NULL on success, error otherwise
  */
  EXP K writeArrow(K arrow_file, K schema_id, K array_data);

  /**
   * @brief Reads the arrow schema from the specified arrow file
   *
   * @param arrow_file  String name of the arrow file to read
   * @return            Schema identifier
  */
  EXP K readArrowSchema(K arrow_file);

  /**
   * @brief Reads the arrow array data from the specified arrow file
   *
   * @param arrow_file  String name of the arrow file to read
   * @return            Mixed list of arrow array objects
  */
  EXP K readArrowData(K arrow_file);

  /**
   * @brief Serializes to an arrow stream using the specified arrow schema and
   * populated with a mixed list of arrow array objects.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * @param schema_id   The schema identifier
   * @param array_data  Mixed list of arrow array data to be serialized
   * @return            KG list containing the serialized stream data
  */
  EXP K serializeArrow(K schema_id, K array_data);

  /**
   * @brief Parses the arrow schema from the specified arrow stream
   *
   * @param char_array  KG list containing the serialized stream data
   * @return            Schema identifier
  */
  EXP K parseArrowSchema(K char_array);

  /**
   * @brief Parses the arrow array data from the specified arrow stream
   *
   * @param char_array  KG list containing the serialized stream data
   * @return            Mixed list of arrow array objects
  */
  EXP K parseArrowData(K char_array);
}

#endif // __TABLE_DATA_H__
