#ifndef __DATATYPE_STORE_H__
#define __DATATYPE_STORE_H__

#include <map>
#include <memory>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"
#include "GenericStore.h"


/**
 * @brief Returns the DatatypeStore singleton which uses the GenericStore
 * template, specialised on std::shared_ptr<arrow::DataType>
 *
 * @return Pointer to the DatatypeStore singleton
*/
GenericStore<std::shared_ptr<arrow::DataType>>* GetDatatypeStore();

/**
 * @brief Converts a string to its corresponding arrow TimeUnit
 *
 * @param tu_str  Time unit string: SECOND/MILLI/MICRO/NANO
 * @return        Arrow TimeUnit
*/
arrow::TimeUnit::type ToTimeUnit(const std::string& tu_str);

/**
 * @brief Converts an arrow TimeUnit to its corresponding string
 *
 * @param tu  Arrow TimeUnit
 * @return    Time unit string: SECOND/MILLI/MICRO/NANO
*/
const std::string FromTimeUnit(arrow::TimeUnit::type tu);

extern "C"
{
  /////////////////////////
  // DATATYPE MANAGEMENT //
  /////////////////////////

  /**
   * @brief Returns the list of identifiers for all arrow datatypes currently
   * held in the DatatypeStore
   *
   * @param   unused 
   * @return  KI list of datatype identifiers
  */
  EXP K listDatatypes(K unused);

  /**
   * @brief Displays user readable information on the specified datatype
   * identifier
   *
   * @param datatype_id Datatype identifier to inspect
   * @return            Formatted string detailing the datatype, error if not
   * found
  */
  EXP K printDatatype(K datatype_id);

  /**
   * @brief Removes an arrow datatype from the DatatypeStore.  Any memory held
   * by the datatype object will be released.
   *
   * @param datatype_id Datatype identifier to be removed
   * @return            Error if not found
  */
  EXP K removeDatatype(K datatype_id);

  /**
   * @brief Checks whether two existing arrow datatypes are logically equal
   *
   * @param first_datatype_id   Identifier of first datatype
   * @param second_datatype_id  Identifier of second datatype 
   * @return                    Bool result
  */
  EXP K equalDatatypes(K first_datatype_id, K second_datatype_id);

  EXP K datatypeName(K datatype_id);


  ///////////////////////////
  // DATATYPE CONSTRUCTORS //
  ///////////////////////////

  /**
   * @brief A NULL type having no physical storage.  NullType arrow array
   * represented in kdb as mixed list of empty lists.
  */
  EXP K null(K unused);

  /**
   * @brief Boolean as 1 bit, LSB bit-packed ordering.  BooleanType arrow array
   * represented in kdb as KB list.
  */
  EXP K boolean(K unused);

  /**
   * @brief Unsigned 8-bit little-endian integer.  UInt8Type arrow array
   * represented in kdb as KG list.
  */
  EXP K uint8(K unused);
  /**
   * @brief Signed 8-bit little-endian integer.  Int8Type arrow array
   * represented in kdb as KG list.
  */
  EXP K int8(K unused);
  /**
   * @brief Unsigned 16-bit little-endian integer.  UInt16Type arrow array
   * represented in kdb as KH list.
  */
  EXP K uint16(K unused);
  /**
   * @brief Signed 16-bit little-endian integer.  Int16Type arrow array
   * represented in kdb as KH list.
  */
  EXP K int16(K unused);
  /**
   * @brief Unsigned 32-bit little-endian integer.  UInt32Type arrow array
   * represented in kdb as KI list.
  */
  EXP K uint32(K unused);
  /**
   * @brief Signed 32-bit little-endian integer.  Int32Type arrow array
   * represented in kdb as KI list.
  */
  EXP K int32(K unused);
  /**
   * @brief Unsigned 64-bit little-endian integer.  UInt64Type arrow array
   * represented in kdb as KJ list.
  */
  EXP K uint64(K unused);
  /**
   * @brief Unsigned 64-bit little-endian integer.  Int64Type arrow array
   * represented in kdb as KJ list.
  */
  EXP K int64(K unused);

  /**
   * @brief 2-byte floating point value (populated from uint16_t).  Float16Type
   * arrow array represented in kdb as KH list.
  */
  EXP K float16(K unused);
  /**
   * @brief 4-byte floating point value.  Float32Type arrow array represented in
   * kdb as KE list.
  */
  EXP K float32(K unused);
  /**
   * @brief 8-byte floating point value.  Float64Type arrow array represented in
   * kdb as KF list.
  */
  EXP K float64(K unused);

  /**
   * @brief UTF8 variable-length string as List<Char>.  StringType arrow array
   * represented in kdb as a mixed list of KC lists.
  */
  EXP K utf8(K unused);
  EXP K large_utf8(K unused);
  /**
   * @brief Variable-length bytes (no guarantee of UTF8-ness).  BinaryType arrow
   * array represented in kdb as a mixed list of KG lists.
  */
  EXP K binary(K unused);
  EXP K large_binary(K unused);
  /**
   * @brief Fixed-size binary. Each value occupies the same number of bytes.
   * FixedSizeBinaryType arrow array represented in kdb as a mixed list of KG
   * lists.
   *
   * @param byte_width  Fixed size byte width
  */
  EXP K fixed_size_binary(K byte_width);

  /**
   * @brief int32_t days since the UNIX epoch.  Date32Type arrow array
   * represented in kdb as KD list (with automatic epoch offsetting).
  */
  EXP K date32(K unused);
  /**
   * @brief int64_t milliseconds since the UNIX epoch.  Date64Type arrow array
   * represented in kdb as KP list (with automatic epoch offsetting and ms
   * scaling).
  */
  EXP K date64(K unused);
  /**
   * @brief Exact timestamp encoded with int64_t (as number of seconds,
   * milliseconds, microseconds or nanoseconds since UNIX epoch).  TimestampType
   * arrow array represented in kdb as KP list (with automatic epoch offsetting
   * and TimeUnit scaling).

   * @param time_unit Time unit string: SECOND/MILLI/MICRO/NANO
  */
  EXP K timestamp(K time_unit);
  /**
   * @brief Time as signed 32-bit integer, representing either seconds or
   * milliseconds since midnight.  Time32Type arrow array represented in kdb as
   * KT list (with automatic TimeUnit scaling).
   *
   * @param time_unit Time unit string: SECOND/MILLI
  */
  EXP K time32(K time_unit);
  /**
   * @brief Time as signed 64-bit integer, representing either microseconds or
   * nanoseconds since midnight.  Time64Type arrow array represented in kdb as
   * KN list (with automatic TimeUnit scaling).
   *
   * @param time_unit Time unit string: MICRO/NANO
  */
  EXP K time64(K time_unit);
  /**
   * @brief Interval described as a number of months, similar to YEAR_MONTH in
   * SQL.  MonthIntervalType arrow array represented in kdb as KM list
  */
  EXP K month_interval(K unused);
  /**
   * @brief Interval described as number of days and milliseconds, similar to
   * DAY_TIME in SQL.  DayTimeIntervalType arrow array represented in kdb as KN
   * list.
  */
  EXP K day_time_interval(K unused);
  /**
   * @brief Measure of elapsed time in either seconds, milliseconds,
   * microseconds or nanoseconds.  DurationType arrow array represented in kdb
   * as KN list (with automatic TimeUnit scaling).
   *
   * @param time_unit Time unit string: SECOND/MILLI/MICRO/NANO
  */
  EXP K duration(K time_unit);

  /**
   * @brief Precision- and scale-based signed 128-bit integer in two's
   * complement.  Decimal128Type arrow array represented in kdb as a mixed list
   * of KG lists (each of length 16).  
   *
   * @param precision Precision width
   * @param scale     Scaling factor
  */
  EXP K decimal128(K precision, K scale);

  /**
   * @brief A list datatype specified in terms of its child datatype.
   *
   * An arrow list array is a nested set of child lists.  This is represented in
   * kdb as a mixed list for the parent list array containing a set of sub-lists
   * (of type determined by the child datatype), one for each of the list value
   * sets.
   *
   * For example, an arrow array with the datatype 'list<item: int64>' could be
   * populated from kdb with: ((1 2 3j); (4 5j); enlist(6j))
   *
   * @param datatype_id Child datatype to use for the list
  */
  EXP K list(K child_datatype_id);
  EXP K large_list(K child_datatype_id);

  /**
   * @brief A fixed_size_list datatype specified in terms of its child datatype
   * and the fixed size of each of the child lists.
   *
   * The kdb representation is the same as for variable length lists, except
   * each of the sub-lists must be of length equal to the list_size.
   *
   * @param datatype_id Child datatype to use for the list
   * @param list_size   Fixed size of each of the child lists
  */
  EXP K fixed_size_list(K child_datatype_id, K list_size);

  /**
   * @brief A map datatype specified in terms of its key and item child
   * datatypes.
   *
   * An arrow map array is a nested set of key/item paired child arrays.  This
   * is represented in kdb as a mixed list for the parent map array, with a
   * dictionary for each map value set.
   *
   * For example, an arrow array with the datatype 'map<string, int64>' could be
   * populated from kdb with: ((`a`b!1 2j);(enlist `c)!(enlist 3j))
   *
   * @param key_datatype_id   Child datatype to use for the map key
   * @param item_datatype_id  Child datatype to use for the map item
  */
  EXP K map(K key_datatype_id, K item_datatype_id);

  /**
   * @brief A struct datatype specified in terms of a list of its constituent
   * child field identifiers.
   *
   * An arrow struct array is a logical grouping of child arrays with each child
   * array corresponding to one of the fields in the struct.  A single struct
   * value is obtaining by slicing across all the child arrays at a given index.
   * This is represented in kdb as a mixed list for the parent struct array,
   * containing child lists for each field in the struct.
   *
   * For example, an arrow array with the datatype:
   *
   * 'struct<field1: int64, field2: string>'
   *
   * could be populated from kdb with: ((1 2 3j);("aa";"bb";"cc")).  By slicing
   * across the lists the first logical struct value is (1j, "aa"), the second
   * (2j, "bb"), etc.
   *
   * Note the trailing underscore in the function's name since struct is a C++
   * reserved keyword.
   *
   * @param field_ids List of the struct's child field identifiers
  */
  EXP K struct_(K field_ids);

  /**
   * @brief A union datatype specified in terms of a list of its constituent
   * child field identifiers.
   *
   * An arrow union array is similar to a struct array except that it has an
   * additional type_id array which identifies the live field in each union
   * value set.  This is represented in kdb as a mixed list for the parent union
   * array, containing a KH list for the type_id array followed by the child
   * lists for each field in the union.
   *
   * For example, an arrow array with the datatype:
   *
   * 'union[sparse]<field1: int64=0, field2: string=1>'
   *
   * could be populated from kdb with: ((0 1 0h);(1 2 3j);("aa";"bb";"cc")).
   *
   * By slicing across the lists the first logical union value is (1j, "aa") with
   * 1j being the live value, the second (2j, "bb") with "bb" being the live
   * value, etc.
   *
   * @param field_ids List of the union's child field identifiers
  */
  EXP K sparse_union(K field_ids);
  EXP K dense_union(K field_ids);

  /**
   * @brief Maps a kdb list to a suitable arrow datatype as follows:
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
   *  KU      |   KT
   *  KV      |   KT
   *  KZ      |   KP
   *
   * Note that the derivation only works for a simple kdb lists containing
   * trivial datatypes.  Only mixed lists of char arrays or byte arrays are
   * supported, mapped to arrow utf8 and binary datatypes respectively.  Other
   * mixed list structures (e.g. those used by the nested arrow datatypes)
   * cannot be interpreted - if required these should be created manually using
   * the datatype constructors.
   *
   * @param k_array Kdb list to be mapped
  */
  EXP K deriveDatatype(K k_array);


  /////////////////////////
  // DATATYPE INSPECTION //
  /////////////////////////
  
  /**
   * @brief Returns the byte_width of a fixed_size_binary datatype.
   *
   * @param datatype_id Identifier of the datatype
   * @return            Byte width
  */
  EXP K getByteWidth(K datatype_id);

  /**
   * @brief Returns the list_size of a fixed_size_list datatype.
   *
   * @param datatype_id Identifier of the datatype
   * @return            List size
  */
  EXP K getListSize(K datatype_id);

  /**
   * @brief Returns the TimeUnit of a time32/time64/timestamp/duration datatype.
   *
   * @param datatype_id Identifier of the datatype
   * @return            Time unit string: SECOND/MILLI/MICRO/NANO
  */
  EXP K getTimeUnit(K datatype_id);

  /**
   * @brief Returns the precision and scale of a decimal datatype.
   *
   * @param datatype_id Identifier of the datatype
   * @return            Mixed list with precision and scale
  */
  EXP K getPrecisionScale(K datatype_id);

  /**
   * @brief Returns the child datatype identifier of a parent list datatype
   *
   * @param datatype_id Identifier of the parent list datatype
   * @return            Child datatype identifier
  */
  EXP K getListDatatype(K datatype_id);

  /**
   * @brief Returns the key and item child datatype identifiers of a parent map
   * datatype
   *
   * @param datatype_id Identifier of the parent map datatype
   * @return            Mixed list with key and item datatype identifiers
  */
  EXP K getMapDatatypes(K datatype_id);

  /**
   * @brief Returns the list of child field identifiers of a struct or union
   * parent datatype
   *
   * @param datatype_id Identifier of the parent struct or union datatype
   * @return            List of child field identifiers
  */
  EXP K getChildFields(K datatype_id);
}

#endif // __DATATYPE_STORE_H__
