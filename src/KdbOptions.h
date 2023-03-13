#ifndef __KDB_OPTIONS__
#define __KDB_OPTIONS__

#include <string>
#include <map>
#include <unordered_map>
#include <stdexcept>
#include <cctype>
#include <set>
#include <algorithm>

#include "k.h"
#include <arrow/type_fwd.h>

namespace kx {
namespace arrowkdb {

// Supported options
namespace Options
{
  // Int options
  const std::string ARROW_CHUNK_ROWS = "ARROW_CHUNK_ROWS";
  const std::string PARQUET_CHUNK_SIZE = "PARQUET_CHUNK_SIZE";
  const std::string PARQUET_MULTITHREADED_READ = "PARQUET_MULTITHREADED_READ";
  const std::string USE_MMAP = "USE_MMAP";
  const std::string DECIMAL128_AS_DOUBLE = "DECIMAL128_AS_DOUBLE";
  const std::string WITH_NULL_BITMAP = "WITH_NULL_BITMAP";

  // String options
  const std::string PARQUET_VERSION = "PARQUET_VERSION";

  // Dict options
  const std::string NULL_MAPPING = "NULL_MAPPING";

  // Null mapping options
  const std::string NM_BOOLEAN = "bool";
  const std::string NM_UINT_8 = "uint8";
  const std::string NM_INT_8 = "int8";
  const std::string NM_UINT_16 = "uint16";
  const std::string NM_INT_16 = "int16";
  const std::string NM_UINT_32 = "uint32";
  const std::string NM_INT_32 = "int32";
  const std::string NM_UINT_64 = "uint64";
  const std::string NM_INT_64 = "int64";
  const std::string NM_FLOAT_16 = "float16";
  const std::string NM_FLOAT_32 = "float32";
  const std::string NM_FLOAT_64 = "float64";
  const std::string NM_STRING = "utf8";
  const std::string NM_LARGE_STRING = "large_utf8";
  const std::string NM_BINARY = "binary";
  const std::string NM_LARGE_BINARY = "large_binary";
  const std::string NM_FIXED_BINARY = "fixed_binary";
  const std::string NM_DATE_32 = "date32";
  const std::string NM_DATE_64 = "date64";
  const std::string NM_TIMESTAMP = "timestamp";
  const std::string NM_TIME_32 = "time32";
  const std::string NM_TIME_64 = "time64";
  const std::string NM_DECIMAL = "decimal";
  const std::string NM_DURATION = "duration";
  const std::string NM_MONTH_INTERVAL = "month_interval";
  const std::string NM_DAY_TIME_INTERVAL = "day_time_interval";

  const static std::set<std::string> int_options = {
    ARROW_CHUNK_ROWS,
    PARQUET_CHUNK_SIZE,
    PARQUET_MULTITHREADED_READ,
    USE_MMAP,
    DECIMAL128_AS_DOUBLE,
    WITH_NULL_BITMAP
  };
  const static std::set<std::string> string_options = {
    PARQUET_VERSION,
  };
  const static std::set<std::string> dict_options = {
    NULL_MAPPING,
  };

  struct NullMapping
  {
      bool have_boolean;
      bool have_uint8;
      bool have_int8;
      bool have_uint16;
      bool have_int16;
      bool have_uint32;
      bool have_int32;
      bool have_uint64;
      bool have_int64;
      bool have_float16;
      bool have_float32;
      bool have_float64;
      bool have_string;
      bool have_large_string;
      bool have_binary;
      bool have_large_binary;
      bool have_fixed_binary;
      bool have_date32;
      bool have_date64;
      bool have_timestamp;
      bool have_time32;
      bool have_time64;
      bool have_decimal;
      bool have_duration;
      bool have_month_interval;
      bool have_day_time_interval;

      using Binary = std::basic_string<unsigned char>;

      bool boolean_null;

      uint8_t uint8_null;
      int8_t int8_null;

      uint16_t uint16_null;
      int16_t int16_null;

      uint32_t uint32_null;
      int32_t int32_null;

      uint64_t uint64_null;
      int64_t int64_null;

      uint16_t float16_null;
      float float32_null;
      double float64_null;

      std::string string_null;
      std::string large_string_null;
      Binary binary_null;
      Binary large_binary_null;
      Binary fixed_binary_null;

      int32_t date32_null;
      int64_t date64_null;
      int64_t timestamp_null;
      int32_t time32_null;
      int64_t time64_null;
      double decimal_null;
      int64_t duration_null;
      int32_t month_interval_null;
      int64_t day_time_interval_null;
  };

} // namespace Options

// Helper class for reading dictionary of options
//
// Dictionary key:    KS
// Dictionary value:  KS or
//                    KJ or
//                    XD or
//                    0 of -KS|-KJ|XD|KC
class KdbOptions
{
private:
  Options::NullMapping null_mapping_options;
  std::map<std::string, std::string> string_options;
  std::map<std::string, int64_t> int_options;

  const std::set<std::string>& supported_string_options;
  const std::set<std::string>& supported_int_options;
  const std::set<std::string>& supported_dict_options;
  std::set<std::string> supported_null_mapping_options;

  using NullMappingHandler = void ( KdbOptions::* )( const std::string&, K );
  using NullMappingHandlers = std::unordered_map<arrow::Type::type, NullMappingHandler>;
  const std::unordered_map<arrow::Type::type, std::string> null_mapping_types;

  static const NullMappingHandlers null_mapping_handlers;
private:
  const std::string ToUpper(std::string str) const;

  const std::string ToLower( std::string str ) const;

  void PopulateIntOptions(K keys, K values);

  void PopulateStringOptions(K keys, K values);

  void PopulateNullMappingOptions( long long index, K dict );

  void PopulateDictOptions( K keys, K values );

  void PopulateMixedOptions(K keys, K values);

public:
  class InvalidOption : public std::invalid_argument
  {
  public:
    InvalidOption(const std::string message) : std::invalid_argument(message.c_str())
    {};
  };

  KdbOptions(
          K options
        , const std::set<std::string>& supported_string_options_
        , const std::set<std::string>& supported_int_options_
        , const std::set<std::string>& supported_dict_options_ = Options::dict_options );

  template<arrow::Type::type TypeId>
  inline void HandleNullMapping( const std::string& key, K value );

  arrow::Type::type GetNullMappingType( const std::string& option );

  void GetNullMappingOptions( Options::NullMapping& null_mapping ) const{
    null_mapping = null_mapping_options;
  }

  bool GetStringOption(const std::string key, std::string& result) const;

  bool GetIntOption(const std::string key, int64_t& result) const;
};

inline void null_mapping_error( const std::string& key, K value )
{
    std::string message = std::string( "Unsupported KDB data type for NULL_MAPPING option '")
        .append( key )
        .append( "', type=" )
        .append( std::to_string( value->t ) )
        .append( "h" );

    throw KdbOptions::InvalidOption( message );
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::BOOL>( const std::string& key, K value )
{
  if( value->t == -KB || value->t == -KG ){
    null_mapping_options.boolean_null = value->g;
    null_mapping_options.have_boolean = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::UINT8>( const std::string& key, K value )
{
  if( -KG == value->t ){
    null_mapping_options.uint8_null = static_cast<uint8_t>( value->g );
    null_mapping_options.have_uint8 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::INT8>( const std::string& key, K value )
{
  if( -KG == value->t ){
    null_mapping_options.int8_null = value->g;
    null_mapping_options.have_int8 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::UINT16>( const std::string& key, K value )
{
  if( -KH == value->t ){
    null_mapping_options.uint16_null = static_cast<uint16_t>( value->h );
    null_mapping_options.have_uint16 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::INT16>( const std::string& key, K value )
{
  if( -KH == value->t ){
    null_mapping_options.int16_null = value->h;
    null_mapping_options.have_int16 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::UINT32>( const std::string& key, K value )
{
  if( -KI == value->t ){
    null_mapping_options.uint32_null = static_cast<uint32_t>( value->i );
    null_mapping_options.have_uint32 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::INT32>( const std::string& key, K value )
{
  if( -KI == value->t ){
    null_mapping_options.int32_null = value->i;
    null_mapping_options.have_int32 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::UINT64>( const std::string& key, K value )
{
  if( -KJ == value->t ){
    null_mapping_options.uint64_null = static_cast<uint64_t>( value->j );
    null_mapping_options.have_uint64 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::INT64>( const std::string& key, K value )
{
  if( -KJ == value->t ){
    null_mapping_options.int64_null = value->j;
    null_mapping_options.have_int64 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::HALF_FLOAT>( const std::string& key, K value )
{
  if( -KH == value->t ){
    null_mapping_options.float16_null = static_cast<uint16_t>( value->h );
    null_mapping_options.have_float16 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::FLOAT>( const std::string& key, K value )
{
  if( -KE == value->t ){
    null_mapping_options.float32_null = value->e;
    null_mapping_options.have_float32 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::DOUBLE>( const std::string& key, K value )
{
  if( -KF == value->t ){
    null_mapping_options.float64_null = value->f;
    null_mapping_options.have_float64 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::STRING>( const std::string& key, K value )
{
  if( KC == value->t ){
    null_mapping_options.string_null.assign( ( char* )kC( value ), value->n );
    null_mapping_options.have_string = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::LARGE_STRING>( const std::string& key, K value )
{
  if( KC == value->t ){
    null_mapping_options.large_string_null.assign( ( char* )kC( value ), value->n );
    null_mapping_options.have_large_string = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::BINARY>( const std::string& key, K value )
{
  if( value->t == KG || value->t == KC ){
      null_mapping_options.binary_null.assign( kG( value ), value->n );
      null_mapping_options.have_binary = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::LARGE_BINARY>( const std::string& key, K value )
{
  if( value->t == KG || value->t == KC ){
    null_mapping_options.large_binary_null.assign( kG( value ), value->n );
    null_mapping_options.have_large_binary = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::FIXED_SIZE_BINARY>( const std::string& key, K value )
{
  switch( value->t ){
  case -UU:
    null_mapping_options.fixed_binary_null.assign( &kU( value )->g[0], sizeof( U ) );
    null_mapping_options.have_fixed_binary = true;
    break;
  case KG:
  case KC:
    null_mapping_options.fixed_binary_null.assign( kG( value ), value->n );
    null_mapping_options.have_fixed_binary = true;
    break;
  default:
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::DATE32>( const std::string& key, K value )
{
  if( value->t == -KD ){
    null_mapping_options.date32_null = value->i;
    null_mapping_options.have_date32 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::DATE64>( const std::string& key, K value )
{
  if( value->t == -KP ){
    null_mapping_options.date64_null = value->j;
    null_mapping_options.have_date64 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::TIMESTAMP>( const std::string& key, K value )
{
  if( value->t == -KP ){
    null_mapping_options.timestamp_null = value->j;
    null_mapping_options.have_timestamp = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::TIME32>( const std::string& key, K value )
{
  if( value->t == -KT ){
    null_mapping_options.time32_null = value->i;
    null_mapping_options.have_time32 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::TIME64>( const std::string& key, K value )
{
  if( value->t == -KN ){
    null_mapping_options.time64_null = value->j;
    null_mapping_options.have_time64 = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::DECIMAL>( const std::string& key, K value )
{
  if( -KF == value->t ){
    null_mapping_options.decimal_null = value->f;
    null_mapping_options.have_decimal = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::DURATION>( const std::string& key, K value )
{
  if( value->t == -KN ){
    null_mapping_options.duration_null = value->j;
    null_mapping_options.have_duration = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::INTERVAL_MONTHS>( const std::string& key, K value )
{
  if( value->t == -KM ){
    null_mapping_options.month_interval_null = value->i;
    null_mapping_options.have_month_interval = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

template<>
inline void KdbOptions::HandleNullMapping<arrow::Type::INTERVAL_DAY_TIME>( const std::string& key, K value )
{
  if( value->t == -KN ){
    null_mapping_options.day_time_interval_null = value->j;
    null_mapping_options.have_day_time_interval = true;
  }
  else{
    null_mapping_error( key, value );
  }
}

} // namespace arrowkdb
} // namespace kx


#endif // __KDB_OPTIONS__
