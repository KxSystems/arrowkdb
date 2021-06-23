#include <iostream>

#include "HelperFunctions.h"
#include "TypeCheck.h"


namespace kx {
namespace arrowkdb {


TemporalConversion::TemporalConversion(std::shared_ptr<arrow::DataType> datatype)
{
  // Work out the correct epoch offsetting and scaling factors required for this
  // arrow datatype
  switch (datatype->id()) {
  case arrow::Type::DATE32:
    // Arrow date32 <-> kdb date (KD)
    // Date32 is int32_t days since the UNIX epoch
    // Kdb date is days since 2000.01.01
    // Requires: epoch offsetting
    offset = kdb_date_epoch_days;
    scalar = 1;
    break;
  case arrow::Type::DATE64:
    // Arrow date64 <-> kdb timestamp (KP)
    // Date64 is int64_t milliseconds since the UNIX epoch 
    // Kdb timestamp is nano since 2000.01.01 00:00:00.0
    // Requires: epoch offsetting and scaling
    offset = kdb_timestamp_epoch_nano;
    scalar = ns_ms_scale;
    break;
  case arrow::Type::TIMESTAMP:
    // Arrow timestamp <-> kdb timestamp (KP)
    // Timestamp is int64 (as number of seconds, milliseconds, microseconds 
    //   or nanoseconds since UNIX epoch)
    // Kdb timestamp is nano since 2000.01.01 00:00:00.0
    // Requires: epoch offsetting and scaling
    offset = kdb_timestamp_epoch_nano;
    switch (std::static_pointer_cast<arrow::TimestampType>(datatype)->unit()) {
    case arrow::TimeUnit::SECOND:
      scalar = ns_sec_scale;
      break;
    case arrow::TimeUnit::MILLI:
      scalar = ns_ms_scale;
      break;
    case arrow::TimeUnit::MICRO:
      scalar = ns_us_scale;
      break;
    case arrow::TimeUnit::NANO:
      scalar = 1;
      break;
    default:
      throw TypeCheck("Invalid TimeUnit");
    }
    break;
  case arrow::Type::TIME32:
    // Arrow time32 <-> kdb time (KT)
    // Time32 is int32 representing either seconds or milliseconds since 
    //   midnight
    // Kdb time is milliseconds from midnight
    // Requires: scaling
    offset = 0;
    switch (std::static_pointer_cast<arrow::Time32Type>(datatype)->unit()) {
    case arrow::TimeUnit::SECOND:
      scalar = 1000;
      break;
    case arrow::TimeUnit::MILLI:
      scalar = 1;
      break;
    default:
      throw TypeCheck("Invalid TimeUnit");
    }
    break;
  case arrow::Type::TIME64:
    // Arrow time64 <-> kdb timespan (KN)
    // Time64 is int64 representing either microseconds or nanoseconds 
    //   since midnight
    // Kdb timespan is nanoseconds from midnight
    // Requires: scaling
    offset = 0;
    switch (std::static_pointer_cast<arrow::Time64Type>(datatype)->unit()) {
    case arrow::TimeUnit::MICRO:
      scalar = 1000;
      break;
    case arrow::TimeUnit::NANO:
      scalar = 1;
      break;
    default:
      throw TypeCheck("Invalid TimeUnit");
    }
    break;
  case arrow::Type::DURATION:
    // Arrow duration <-> kdb timespan (KN)
    // Duration is an int64 measure of elapsed time in either seconds, 
    //   milliseconds, microseconds or nanoseconds
    // Kdb timestamp is nano since 2000.01.01 00:00:00.0
    // Requires: scaling
    offset = 0;
    switch (std::static_pointer_cast<arrow::DurationType>(datatype)->unit()) {
    case arrow::TimeUnit::SECOND:
      scalar = ns_sec_scale;
      break;
    case arrow::TimeUnit::MILLI:
      scalar = ns_ms_scale;
      break;
    case arrow::TimeUnit::MICRO:
      scalar = ns_us_scale;
      break;
    case arrow::TimeUnit::NANO:
      scalar = 1;
      break;
    default:
      throw TypeCheck("Invalid TimeUnit");
    }
    break;
  default:
    TYPE_CHECK_UNSUPPORTED(datatype->ToString());
  }
}

// Epoch / scaling constants
const static int64_t ns_us_scale = 1000LL;
const static int64_t ns_ms_scale = ns_us_scale * 1000LL;
const static int64_t day_as_ns = 86400000000000LL;

int64_t DayTimeInterval_KTimespan(arrow::DayTimeIntervalType::c_type dt_interval)
{
  return dt_interval.days * day_as_ns + dt_interval.milliseconds * ns_ms_scale;
}

arrow::DayTimeIntervalType::c_type KTimespan_DayTimeInterval(int64_t k_timespan)
{
  int32_t days = (int32_t)(k_timespan / day_as_ns);
  int32_t msec = (int32_t)((k_timespan % day_as_ns) / ns_ms_scale);
  return { days, msec };
}

bool IsKdbString(K str)
{
  return str != NULL && (str->t == -KS || str->t == KC);
}

const std::string GetKdbString(K str)
{
  return str->t == -KS ? str->s : std::string((S)kG(str), str->n);
}

TypeMappingOverride::TypeMappingOverride(const KdbOptions& options)
{
  options.GetIntOption(Options::DECIMAL128_AS_DOUBLE, decimal128_as_double);
}

KdbType GetKdbType(std::shared_ptr<arrow::DataType> datatype, TypeMappingOverride& type_overrides)
{
  switch (datatype->id()) {
  case arrow::Type::NA:
    return 0;
  case arrow::Type::BOOL:
    return KB;
  case arrow::Type::UINT8:
  case arrow::Type::INT8:
    return KG;
  case arrow::Type::UINT16:
  case arrow::Type::INT16:
    return KH;
  case arrow::Type::UINT32:
  case arrow::Type::INT32:
    return KI;
  case arrow::Type::UINT64:
  case arrow::Type::INT64:
    return KJ;
  case arrow::Type::HALF_FLOAT: 
    return KH; // uses uint16_t
  case arrow::Type::FLOAT:
    return KE;
  case arrow::Type::DOUBLE:
    return KF;
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return 0; // mixed list of KC lists
  case arrow::Type::BINARY:
  case arrow::Type::LARGE_BINARY:
  case arrow::Type::FIXED_SIZE_BINARY:
    return 0; // mixed list of KG lists
  case arrow::Type::DATE32:
    return KD;
  case arrow::Type::DATE64:
    return KP;
  case arrow::Type::TIMESTAMP:
    return KP;
  case arrow::Type::TIME32:
    return KT;
  case arrow::Type::TIME64:
    return KN;
  case arrow::Type::DECIMAL:
    if (type_overrides.decimal128_as_double)
      return KF; // map decimal128 to double
    else
      return 0; // mixed list of KG lists of length 16
  case arrow::Type::DURATION:
    return KN;
  case arrow::Type::INTERVAL_MONTHS:
    return KM;
  case arrow::Type::INTERVAL_DAY_TIME:
    return KN;
  // Nested datatypes, see constructors in DatatypeStore.h for mixed list structure
  case arrow::Type::LIST:
  case arrow::Type::LARGE_LIST:
  case arrow::Type::FIXED_SIZE_LIST:
    return 0; 
  case arrow::Type::MAP:
    return 0;
  case arrow::Type::STRUCT:
    return 0;
  case arrow::Type::SPARSE_UNION:
  case arrow::Type::DENSE_UNION:
    return 0;
  default:
    TYPE_CHECK_UNSUPPORTED(datatype->ToString());
  }
  return 0;
}

std::shared_ptr<arrow::DataType> GetArrowType(K k_array)
{
  switch (k_array->t) {
  case KB:
    return arrow::boolean();
  case UU:
    // Guids are allowed on the array writing path
    return arrow::fixed_size_binary(sizeof(U));
  case KG:
    return arrow::int8();
  case KH:
    return arrow::int16();
  case KI:
    return arrow::int32();
  case KJ:
    return arrow::int64();
  case KE:
    return arrow::float32();
  case KF:
    return arrow::float64();
  case KC:
    // Chars are allowed on the array writing path
    return arrow::int8();
  case KS:
    // Symbols are allowed on the array writing path
    return arrow::utf8();
  case KP:
    return arrow::timestamp(arrow::TimeUnit::NANO);
  case KM:
    return arrow::month_interval();
  case KD:
    return arrow::date32();
  case KN:
    return arrow::time64(arrow::TimeUnit::NANO);
  case KT:
    return arrow::time32(arrow::TimeUnit::MILLI);

  // There is fixed mapping from each arrow temporal datatype to the best fit
  // kdb type.  Supporting the below would clutter the type checking and
  // requiring double scaling (from kdb to a fixed time/date representation then
  // from that to the correct TimeUnit specified for that arrow datatype).
  // Casting these in q is simpler and more robust.
  case KU:
    throw TypeCheck("17h unsuppported, cast in q to 19h");
  case KV:
    throw TypeCheck("18h unsupported, cast in q to 19h");
  case KZ:
    throw TypeCheck("15h unsupported, cast to in q to 12h");

  // We can't differentiate a mixed lixed - should the array datatype be a null,
  // string, variable binary, fixed size binary, decimal, list, map, union,
  // struct, etc.?
  case 0:
  {
    if (k_array->n < 1)
      throw TypeCheck("Cannot infer arrow datatype from empty mixed list");

    KdbType k_type = kK(k_array)[0]->t;
    for (auto i = 1; i < k_array->n; ++i)
      if (k_type != kK(k_array)[i]->t)
        throw TypeCheck("Cannot infer arrow datatype from mixed list with different sub-types");

    switch (k_type) {
    case KC:
      return arrow::utf8();
    case KG:
      return arrow::binary();
    default:
      throw TypeCheck(("Cannot infer arrow datatype from mixed list of " + std::to_string(k_type) + "h").c_str());
    }
  }
  default:
    throw TypeCheck("Unsupported kdb datatype");
  }

  return std::shared_ptr<arrow::DataType>();
}

} // namespace arrowkdb
} // namespace kx
