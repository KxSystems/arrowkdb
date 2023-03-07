#ifndef __HELPER_FUNCTIONS_H__
#define __HELPER_FUNCTIONS_H__

#include <limits>
#include <cmath>
#include <cstdint>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "TypeCheck.h"
#include "KdbOptions.h"

#include <k.h>


namespace kx {
namespace arrowkdb {

//////////////////////////////
// TEMPORAL TYPE CONVERSION //
//////////////////////////////

// Helper class which can convert any int32 or int64 arrow temporal type
// (including those with a parameterised TimeUnit) to an appropriate kdb type.
class TemporalConversion
{
private:
  // Epoch / scaling constants
  const static int32_t kdb_date_epoch_days = 10957;
  const static int64_t kdb_timestamp_epoch_nano = 946684800000000000LL;
  const static int64_t ns_us_scale = 1000LL;
  const static int64_t ns_ms_scale = ns_us_scale * 1000LL;
  const static int64_t ns_sec_scale = ns_ms_scale * 1000LL;
  const static int64_t day_as_ns = 86400000000000LL;

  int64_t offset = 0;
  int64_t scalar = 1;

public:
  // The constructor sets up the correct epoch offsetting and scaling factor
  // based the arrow datatype
  TemporalConversion(std::shared_ptr<arrow::DataType> datatype);

  // Converts from an arrow temporal (either int32 or int64) to its kdb value,
  // applying the epoch offseting and scaling factor
  template <typename T>
  inline T ArrowToKdb(T value)
  {
    return value * (T)scalar - (T)offset;
  }

  // Converts from a kdb temporal (either int32 or int64) to its arrow value,
  // applying the epoch offseting and scaling factor
  template <typename T>
  inline T KdbToArrow(T value)
  {
    return (value + (T)offset) / (T)scalar;
  }
};

// Arrow day_time_interval <-> kdb timespan (KN)
// Requires: splitting day/time and scaling
int64_t DayTimeInterval_KTimespan(arrow::DayTimeIntervalType::c_type dt_interval);
arrow::DayTimeIntervalType::c_type KTimespan_DayTimeInterval(int64_t k_timespan);


/////////////////
// KDB STRINGS //
/////////////////

bool IsKdbString(K str);
const std::string GetKdbString(K str);


////////////////////
// FLOATS COMPARE //
////////////////////

//! Compares floating point numbers, because of unreliable direct compare
//! @param lhs - left-hand side value
//! @param rhs - right-hand side value
//! @return true if values are nearby
template<typename T>
inline bool is_equal( T lhs, T rhs )
{
    static const T epsilon = 2 * std::numeric_limits<T>::epsilon();

    return ::fabs( lhs -= rhs ) <= epsilon;
}


//////////////////
// TYPE MAPPING //
//////////////////

typedef signed char KdbType;

 struct TypeMappingOverride
{
  int64_t decimal128_as_double = 0;
  Options::NullMapping null_mapping;
  int64_t chunk_offset = 0;
  int64_t chunk_length = 0;

  TypeMappingOverride(void) {};
  TypeMappingOverride(const KdbOptions& options);

  int64_t NumChunks( long long array_length ) { return !chunk_length ? 1
    : array_length / chunk_length + ( array_length % chunk_length ? 1 : 0 );
  }
  std::pair<int64_t, int64_t> GetChunk( long long array_length ){
      int64_t offset = chunk_length ? chunk_offset : 0;
      int64_t length = std::min( array_length - offset, chunk_length ? chunk_length : array_length );

      return std::make_pair( offset, length );
  }
};

/**
 * @brief Maps an arrow datatype to a kdb list type.  Used to:
 * 1. Create kdb list of the correct type when reading from an arrow array to
 *    kdb object
 * 2. Type checking when writing to an arrow array from a kdb object
 *
 * @param datatype  Required arrow datatype
 * @return          KdbType (k0->t)
*/
KdbType GetKdbType(std::shared_ptr<arrow::DataType> datatype, TypeMappingOverride& type_overrides);

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


///////////////////////
// FUNCTION HANDLERS //
///////////////////////

typedef std::function<KdbType(std::shared_ptr<arrow::DataType> datatype, TypeMappingOverride& type_overrides)> GetKdbTypeCommon;

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
