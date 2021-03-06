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
   * Supported options: 
   *  parquet_chunk_size
   *    Controls the approximate size of encoded data pages within a column 
   *    chunk (long, default: 1MB)
   *
   * @param parquet_file  String name of the parquet file to write
   * @param schema_id     The schema identifier
   * @param array_data    Mixed list of arrow array data to be written to the
   * file
   * @param options       Dictionary of symbol options to long values
   * @return              NULL on success, error otherwise
  */
  EXP K writeParquet(K parquet_file, K schema_id, K array_data, K options);

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
   * Supported options:
   *  parquet_multithreaded_read
   *    Flag indicating whether the parquet reader should run in multithreaded
   *    mode.   This can improve performance by processing multiple columns in
   *    parallel (long, default: 0)
   *  use_mmap
   *    Flag indicating whether the parquet file should be memory mapped in.  
   *    This can improve performance on systems which support mmap (long, 
   *    default: 0)
   *
   * @param parquet_file  String name of the parquet file to read
   * @param options       Dictionary of symbol options to long values
   * @return              Mixed list of arrow array objects
  */
  EXP K readParquetData(K parquet_file, K options);

  /**
   * @brief Reads a single column from a parquet file
   * 
   * @param parquet_file  String name of the parquet file to read
   * @param column_index  The index of the column to be read
   * @return              Arrow array object
  */
  EXP K readParquetColumn(K parquet_file, K column_index);

  /**
   * @brief Creates an arrow IPC record batch file with the specified arrow
   * schema and populates it from a mixed list of arrow array objects.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * @param arrow_file  String name of the arrow file to write
   * @param schema_id   The schema identifier
   * @param array_data  Mixed list of arrow array data to be written to the 
   * file
   * @return            NULL on success, error otherwise
  */
  EXP K writeArrow(K arrow_file, K schema_id, K array_data);

  /**
   * @brief Reads the arrow schema from the specified arrow IPC record batch
   * file
   *
   * @param arrow_file  String name of the arrow file to read
   * @return            Schema identifier
  */
  EXP K readArrowSchema(K arrow_file);

  /**
   * @brief Reads the arrow array data from the specified arrow IPC record 
   * batch file
   *
   * Supported options:
   *  use_mmap
   *    Flag indicating whether the arrow file should be memory mapped in.  This 
   *    can improve performance on systems which support mmap (long, default: 0)
   * 
   * @param arrow_file  String name of the arrow file to read
   * @param options     Dictionary of symbol options to long values
   * @return            Mixed list of arrow array objects
  */
  EXP K readArrowData(K arrow_file, K options);

  /**
   * @brief Serializes to an arrow IPC record batch stream using the specified
   * arrow schema and populated with a mixed list of arrow array objects.
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
   * @brief Parses the arrow schema from the specified arrow IPC record batch
   * stream
   *
   * @param char_array  KG list containing the serialized stream data
   * @return            Schema identifier
  */
  EXP K parseArrowSchema(K char_array);

  /**
   * @brief Parses the arrow array data from the specified arrow IPC record
   * batch stream
   *
   * @param char_array  KG list containing the serialized stream data
   * @return            Mixed list of arrow array objects
  */
  EXP K parseArrowData(K char_array);

}

#endif // __TABLE_DATA_H__
