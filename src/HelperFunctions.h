#ifndef __HELPER_FUNCTIONS_H__
#define __HELPER_FUNCTIONS_H__

#include <cstdint>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include <k.h>


namespace kx {
namespace arrowkdb {

//////////////////////////////
// TEMPORAL TYPE CONVERSION //
//////////////////////////////

// Arrow date32 <-> kdb date (KD)
// Requires: epoch offsetting
int32_t Date32_KDate(int32_t date32);
int32_t KDate_Date32(int32_t k_date);

// Arrow date64 <-> kdb timestamp (KP)
// Requires: epoch offsetting and scaling
int64_t Date64_KTimestamp(int64_t date64);
int64_t KTimestamp_Date64(int64_t k_timestamp);

// Arrow timestamp <-> kdb timestamp (KP)
// Requires: epoch offsetting and scaling
int64_t Timestamp_KTimestamp(std::shared_ptr<arrow::TimestampType> datatype, int64_t timestamp);
int64_t KTimestamp_Timestamp(std::shared_ptr<arrow::TimestampType> datatype, int64_t k_timestamp);

// Arrow time32 <-> kdb time (KT)
// Requires: scaling
int32_t Time32_KTime(std::shared_ptr<arrow::Time32Type> datatype, int32_t time32);
int32_t KTime_Time32(std::shared_ptr<arrow::Time32Type> datatype, int32_t k_time);

// Arrow time64 <-> kdb timespan (KN)
// Requires: scaling
int64_t Time64_KTimespan(std::shared_ptr<arrow::Time64Type> datatype, int64_t time64);
int64_t KTimespan_Time64(std::shared_ptr<arrow::Time64Type> datatype, int64_t k_timespan);

// Arrow duration <-> kdb timespan (KN)
// Requires: scaling
int64_t Duration_KTimespan(std::shared_ptr<arrow::DurationType> datatype, int64_t timespan);
int64_t KTimespan_Duration(std::shared_ptr<arrow::DurationType> datatype, int64_t k_timespan);

// Arrow day_time_interval <-> kdb timespan (KN)
// Requires: splitting day/time and scaling
int64_t DayTimeInterval_KTimespan(arrow::DayTimeIntervalType::c_type dt_interval);
arrow::DayTimeIntervalType::c_type KTimespan_DayTimeInterval(int64_t k_timespan);


/////////////////
// KDB STRINGS //
/////////////////
bool IsKdbString(K str);
const std::string GetKdbString(K str);


//////////////////
// TYPE MAPPING //
//////////////////
typedef signed char KdbType;

/**
 * @brief Maps an arrow datatype to a kdb list type.  Used to:
 * 1. Create kdb list of the correct type when reading from an arrow array to
 *    kdb object
 * 2. Type checking when writing to an arrow array from a kdb object
 *
 * @param datatype  Required arrow datatype
 * @return          KdbType (k0->t)
*/
KdbType GetKdbType(std::shared_ptr<arrow::DataType> datatype);

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
 *  KM      |   interval_months
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
 * @return        Arrow datatype
*/
std::shared_ptr<arrow::DataType> GetArrowType(K k_array);

} // namespace arrowkdb
} // namespace kx


////////////////////////
// EXCEPTION HANDLING //
////////////////////////

#define KDB_EXCEPTION_TRY \
  static char error_msg[1024]; \
  *error_msg = '\0'; \
  try {

#define KDB_EXCEPTION_CATCH \
  } catch (std::exception& e) {  \
    strncpy(error_msg, e.what(), sizeof(error_msg));  \
    error_msg[sizeof(error_msg) - 1] = '\0';  \
    return krr(error_msg);  \
  }


#endif // __HELPER_FUNCTIONS_H__
