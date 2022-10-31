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
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param schema_id   The schema identifier to use for the intermediate arrow
   * table
   * @param array_data  Mixed list of arrow array data to be written to the
   * intermediate arrow table
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return            kdb char list containing the pretty printed buffer
  */
  EXP K prettyPrintTable(K schema_id, K array_data, K options);

  /**
   * @brief Debugging function which converts a kdb mixed list of arrow array
   * data objects to an arrow table then converts it back again.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
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
   * @param schema_id   The schema identifier to use for the intermediate arrow
   * table
   * @param array_data  Mixed list of arrow array data to be written to the
   * intermediate arrow table
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return            The kdb mixed list created from the intermediate arrow
   * table
  */
  EXP K writeReadTable(K schema_id, K array_data, K options);

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
   *
   * PARQUET_CHUNK_SIZE (long) - Controls the approximate size of encoded data
   *  pages within a column chunk.  Default 1MB
   *
   * PARQUET_VERSION (string) - Selects the Parquet format version, either
   * `V1.0` or `V2.0`.  `V2.0` is more fully featured but may be incompatible
   * with older Parquet implementations.  Default `V1.0`
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param parquet_file  String name of the parquet file to write
   * @param schema_id     The schema identifier
   * @param array_data    Mixed list of arrow array data to be written to the
   * file
   * @options             Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
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
   *
   * PARQUET_MULTITHREADED_READ (long) - Flag indicating whether the parquet
   * reader should run in multithreaded mode.   This can improve performance by
   * processing multiple columns in parallel.  Default 0
   *
   * USE_MMAP (long) - Flag indicating whether the parquet file should be memory
   * mapped in.  This can improve performance on systems which support mmap.
   * Default 0
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param parquet_file  String name of the parquet file to read
   * @options             Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return              Mixed list of arrow array objects
  */
  EXP K readParquetData(K parquet_file, K options);

  /**
   * @brief Reads a single column from a parquet file
   *
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param parquet_file  String name of the parquet file to read
   * @param column_index  The index of the column to be read
   * @options             Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return              Arrow array object
  */
  EXP K readParquetColumn(K parquet_file, K column_index, K options);

  /**
   * @brief Reads the number of row groups used by a parquet file
   * 
   * @param parquet_file  String name of the parquet file to read
   * @return              Number of row groups as a -6h
  */
  EXP K readParquetNumRowGroups(K parquet_file);

  /**
   * @brief Reads a set of row groups from a parquet file
   *
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param parquet_file  String name of the parquet file to read
   * @param row_groups    Integer list (6h) of row groups indicies to read, or
   * generic null (::) to read all row groups
   * @param columns       Integer list (6h) of column indicies to read, or
   * generic null (::) to read all columns
   * @options             Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return              Mixed list of arrow array objects
  */
  EXP K readParquetRowGroups(K parquet_file, K row_groups, K columns, K options);

  /**
   * @brief Creates an arrow IPC record batch file with the specified arrow
   * schema and populates it from a mixed list of arrow array objects.
   *
   * The mixed list of arrow array data should be ordered in schema field
   * number.  Each kdb object representing one of the arrays must be structured
   * according to the field's datatype.  This required array data structure is
   * detailed for each of the datatype constructor functions.
   *
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param arrow_file  String name of the arrow file to write
   * @param schema_id   The schema identifier
   * @param array_data  Mixed list of arrow array data to be written to the 
   * file
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return            NULL on success, error otherwise
  */
  EXP K writeArrow(K arrow_file, K schema_id, K array_data, K options);

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
   *
   * USE_MMAP (long) - Flag indicating whether the parquet file should be memory
   * mapped in.  This can improve performance on systems which support mmap.
   * Default 0
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   * 
   * @param arrow_file  String name of the arrow file to read
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
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
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param schema_id   The schema identifier
   * @param array_data  Mixed list of arrow array data to be serialized
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return            KG list containing the serialized stream data
  */
  EXP K serializeArrow(K schema_id, K array_data, K options);

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
   * Supported options:
   *
   * DECIMAL128_AS_DOUBLE (long) - Flag indicating whether to override the
   * default type mapping for the arrow decimal128 datatype and instead
   * represent it as a double (9h).  Default 0.
   *
   * @param char_array  KG list containing the serialized stream data
   * @options           Dictionary of options or generic null (::) to use
   * defaults.  Dictionary key must be a 11h list. Values list can be 7h, 11h or
   * mixed list of -7|-11|4h.
   * @return            Mixed list of arrow array objects
  */
  EXP K parseArrowData(K char_array, K options);

}

#endif // __TABLE_DATA_H__
