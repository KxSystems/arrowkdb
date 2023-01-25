#ifndef __KDB_OPTIONS__
#define __KDB_OPTIONS__

#include <string>
#include <map>
#include <stdexcept>
#include <cctype>
#include <set>
#include <algorithm>

#include "k.h"
#include <arrow/type_fwd.h>

namespace kx {
namespace arrowkdb {

template<typename E>
constexpr auto toUType( E enumerator ) noexcept
{
    return static_cast<std::underlying_type_t<E>>( enumerator );
}

template< typename E >
struct ETraits
{
    using Names = std::map<E, std::string>;

    static std::string name( E enumerator )
    {
        auto it = names.find( enumerator );
        if( it != names.end() )
        {
            return it->second;
        }

        return "UNKNOWN";
    }

    static std::string name( int index ) { return name( static_cast<E>( index ) ); }

    static const Names names;
};

// Supported options
namespace Options
{
  // Int options
  const std::string PARQUET_CHUNK_SIZE = "PARQUET_CHUNK_SIZE";
  const std::string PARQUET_MULTITHREADED_READ = "PARQUET_MULTITHREADED_READ";
  const std::string USE_MMAP = "USE_MMAP";
  const std::string DECIMAL128_AS_DOUBLE = "DECIMAL128_AS_DOUBLE";

  // String options
  const std::string PARQUET_VERSION = "PARQUET_VERSION";

  // Dict options
  const std::string NULL_MAPPING = "NULL_MAPPING";

  // Null mapping options
  const std::string NM_NA = "na";
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
  const std::string NM_STRING = "string";
  const std::string NM_LARGE_STRING = "large_string";
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
    PARQUET_CHUNK_SIZE,
    PARQUET_MULTITHREADED_READ,
    USE_MMAP,
    DECIMAL128_AS_DOUBLE,
  };
  const static std::set<std::string> string_options = {
    PARQUET_VERSION,
  };
  const static std::set<std::string> dict_options = {
    NULL_MAPPING,
  };
  const static std::set<std::string> null_mapping_options = {
      NM_NA
    , NM_BOOLEAN
    , NM_UINT_8
    , NM_INT_8
    , NM_UINT_16
    , NM_INT_16
    , NM_UINT_32
    , NM_INT_32
    , NM_UINT_64
    , NM_INT_64
    , NM_FLOAT_16
    , NM_FLOAT_32
    , NM_FLOAT_64
    , NM_STRING
    , NM_LARGE_STRING
    , NM_BINARY
    , NM_LARGE_BINARY
    , NM_FIXED_BINARY
    , NM_DATE_32
    , NM_DATE_64
    , NM_TIMESTAMP
    , NM_TIME_32
    , NM_TIME_64
    , NM_DECIMAL
    , NM_DURATION
    , NM_MONTH_INTERVAL
    , NM_DAY_TIME_INTERVAL
  };

  struct NullMapping
  {
      bool have_na;
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

      void* na_null = nullptr;
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

      template<arrow::Type::type TypeId = arrow::Type::NA>
      auto GetOption() const { return std::make_pair( true, na_null );}
  };

  template<> inline auto NullMapping::GetOption<arrow::Type::BOOL>() const{ return std::make_pair( have_boolean, boolean_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::UINT8>() const{ return std::make_pair( have_uint8, uint8_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::INT8>() const{ return std::make_pair( have_int8, int8_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::UINT16>() const{ return std::make_pair( have_uint16, uint16_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::INT16>() const{ return std::make_pair( have_int16, int16_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::UINT32>() const{ return std::make_pair( have_uint32, uint32_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::INT32>() const{ return std::make_pair( have_int32, int32_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::UINT64>() const{ return std::make_pair( have_uint64, uint64_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::INT64>() const{ return std::make_pair( have_int64, int64_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::HALF_FLOAT>() const{ return std::make_pair( have_float16, float16_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::FLOAT>() const{ return std::make_pair( have_float32, float32_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::DOUBLE>() const{ return std::make_pair( have_float64, float64_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::STRING>() const{ return std::make_pair( have_string, string_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::LARGE_STRING>() const{ return std::make_pair( have_large_string, large_string_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::BINARY>() const{ return std::make_pair( have_binary, binary_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::LARGE_BINARY>() const{ return std::make_pair( have_large_binary, large_binary_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::FIXED_SIZE_BINARY>() const{ return std::make_pair( have_fixed_binary, fixed_binary_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::DATE32>() const{ return std::make_pair( have_date32, date32_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::DATE64>() const{ return std::make_pair( have_date64, date64_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::TIMESTAMP>() const{ return std::make_pair( have_timestamp, timestamp_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::TIME32>() const{ return std::make_pair( have_time32, time32_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::TIME64>() const{ return std::make_pair( have_time64, time64_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::DECIMAL>() const{ return std::make_pair( have_decimal, decimal_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::DURATION>() const{ return std::make_pair( have_duration, duration_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::INTERVAL_MONTHS>() const{ return std::make_pair( have_month_interval, month_interval_null ); }
  template<> inline auto NullMapping::GetOption<arrow::Type::INTERVAL_DAY_TIME>() const{ return std::make_pair( have_day_time_interval, day_time_interval_null ); }
} // namespace Options

template<>
inline const ETraits<arrow::Type::type>::Names ETraits<arrow::Type::type>::names{
    { arrow::Type::NA, Options::NM_NA }
  , { arrow::Type::BOOL, Options::NM_BOOLEAN }
  , { arrow::Type::UINT8, Options::NM_UINT_8 }
  , { arrow::Type::INT8, Options::NM_INT_8 }
  , { arrow::Type::UINT16, Options::NM_UINT_16 }
  , { arrow::Type::INT16, Options::NM_INT_16 }
  , { arrow::Type::UINT32, Options::NM_UINT_32 }
  , { arrow::Type::INT32, Options::NM_INT_32 }
  , { arrow::Type::UINT64, Options::NM_UINT_64 }
  , { arrow::Type::INT64, Options::NM_INT_64 }
  , { arrow::Type::HALF_FLOAT, Options::NM_FLOAT_16 }
  , { arrow::Type::FLOAT, Options::NM_FLOAT_32 }
  , { arrow::Type::DOUBLE, Options::NM_FLOAT_64 }
  , { arrow::Type::STRING, Options::NM_STRING }
  , { arrow::Type::LARGE_STRING, Options::NM_LARGE_STRING }
  , { arrow::Type::BINARY, Options::NM_BINARY }
  , { arrow::Type::LARGE_BINARY, Options::NM_LARGE_BINARY }
  , { arrow::Type::FIXED_SIZE_BINARY, Options::NM_FIXED_BINARY }
  , { arrow::Type::DATE32, Options::NM_DATE_32 }
  , { arrow::Type::DATE64, Options::NM_DATE_64 }
  , { arrow::Type::TIMESTAMP, Options::NM_TIMESTAMP }
  , { arrow::Type::DATE32, Options::NM_DATE_32 }
  , { arrow::Type::DATE64, Options::NM_DATE_64 }
  , { arrow::Type::DECIMAL, Options::NM_DECIMAL }
  , { arrow::Type::DURATION, Options::NM_DURATION }
  , { arrow::Type::INTERVAL_MONTHS, Options::NM_MONTH_INTERVAL }
  , { arrow::Type::INTERVAL_DAY_TIME, Options::NM_DAY_TIME_INTERVAL }
};

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
  const std::set<std::string>& supported_null_mapping_options;

private:
  const std::string ToUpper(std::string str) const
  {
    std::string upper;
    for (auto i : str)
      upper.push_back((unsigned char)std::toupper(i));
    return upper;
  }

  const std::string ToLower( std::string str ) const
  {
    std::transform( str.begin(), str.end(), str.begin(), ::tolower );

    return str;
  }

  void PopulateIntOptions(K keys, K values)
  {
    for (auto i = 0ll; i < values->n; ++i) {
      const std::string key = ToUpper(kS(keys)[i]);
      if (supported_int_options.find(key) == supported_int_options.end())
        throw InvalidOption(("Unsupported int option '" + key + "'").c_str());
      int_options[key] = kJ(values)[i];
    }
  }

  void PopulateStringOptions(K keys, K values)
  {
    for (auto i = 0ll; i < values->n; ++i) {
      const std::string key = ToUpper(kS(keys)[i]);
      if (supported_string_options.find(key) == supported_string_options.end())
        throw InvalidOption(("Unsupported string option '" + key + "'").c_str());
      string_options[key] = ToUpper(kS(values)[i]);
    }
  }

  void PopulateNullMappingOptions( long long index, K dict )
  {
    using NM = arrow::Type::type;

    K keys = kK( kK( dict )[index] )[0];
    K values = kK( kK( dict )[index] )[1];
    if( KS != keys->t ){
        throw InvalidOption( "Unsupported KDB data type for NULL_MAPPING keys (expected=11h), type=" + std::to_string( keys->t ) + "h" );
    }
    if( 0 != values->t ){
        throw InvalidOption( "Unsupported KDB data type for NULL_MAPPING values (extected=0h), type=" + std::to_string( keys->t ) + "h" );
    }
    for( auto i = 0ll; i < values->n; ++i ){
      const std::string key = ToLower( kS( keys )[i] );
      if( supported_null_mapping_options.find( key ) == supported_null_mapping_options.end() ){
        throw InvalidOption(("Unsupported NULL_MAPPING option '" + key + "'").c_str());
      }
      K value = kK( values )[i];
      if( ETraits<NM>::name( NM::BOOL ) == key && -KB == value->t ){
        null_mapping_options.boolean_null = value->g;
        null_mapping_options.have_boolean = true;
      }
      else if( ETraits<NM>::name( NM::BOOL ) == key && -KG == value->t ){
        null_mapping_options.boolean_null = value->g;
        null_mapping_options.have_boolean = true;
      }
      else if( ETraits<NM>::name( NM::UINT8 ) == key && -KG == value->t ){
        null_mapping_options.uint8_null = value->g;
        null_mapping_options.have_uint8 = true;
      }
      else if( ETraits<NM>::name( NM::INT8 ) == key && -KG == value->t ){
        null_mapping_options.int8_null = value->g;
        null_mapping_options.have_int8 = true;
      }
      else if( ETraits<NM>::name( NM::UINT16 ) == key && -KH == value->t ){
        null_mapping_options.uint16_null = value->h;
        null_mapping_options.have_uint16 = true;
      }
      else if( ETraits<NM>::name( NM::INT16 ) == key && -KH == value->t ){
        null_mapping_options.int16_null = value->h;
        null_mapping_options.have_int16 = true;
      }
      else if( ETraits<NM>::name( NM::UINT32 ) == key && -KI == value->t ){
        null_mapping_options.uint32_null = value->i;
        null_mapping_options.have_uint32 = true;
      }
      else if( ETraits<NM>::name( NM::INT32 ) == key && -KI == value->t ){
        null_mapping_options.int32_null = value->i;
        null_mapping_options.have_int32 = true;
      }
      else if( ETraits<NM>::name( NM::UINT64 ) == key && -KJ == value->t ){
        null_mapping_options.uint64_null = value->j;
        null_mapping_options.have_uint64 = true;
      }
      else if( ETraits<NM>::name( NM::INT64 ) == key && -KJ == value->t ){
        null_mapping_options.int64_null = value->j;
        null_mapping_options.have_int64 = true;
      }
      else if( ETraits<NM>::name( NM::HALF_FLOAT ) == key && -KH == value->t ){
        null_mapping_options.float16_null = value->h;
        null_mapping_options.have_float16 = true;
      }
      else if( ETraits<NM>::name( NM::FLOAT ) == key && -KE == value->t ){
        null_mapping_options.float32_null = value->e;
        null_mapping_options.have_float32 = true;
      }
      else if( ETraits<NM>::name( NM::DOUBLE ) == key && -KF == value->t ){
        null_mapping_options.float64_null = value->f;
        null_mapping_options.have_float64 = true;
      }
      else if( ETraits<NM>::name( NM::STRING ) == key && KC == value->t ){
        null_mapping_options.string_null.assign( (char*)kC( value ), value->n );
        null_mapping_options.have_string = true;
      }
      else if( ETraits<NM>::name( NM::LARGE_STRING ) == key && KC == value->t ){
        null_mapping_options.large_string_null.assign( (char*)kC( value ), value->n );
        null_mapping_options.have_large_string = true;
      }
      else if( ETraits<NM>::name( NM::BINARY ) == key && KC == value->t ){
        null_mapping_options.binary_null.assign( kC( value ), value->n );
        null_mapping_options.have_binary = true;
      }
      else if( ETraits<NM>::name( NM::LARGE_BINARY ) == key && KC == value->t ){
        null_mapping_options.large_binary_null.assign( kC( value ), value->n );
        null_mapping_options.have_large_binary = true;
      }
      else if( ETraits<NM>::name( NM::FIXED_SIZE_BINARY ) == key && KC == value->t ){
        null_mapping_options.fixed_binary_null.assign( kC( value ), value->n );
        null_mapping_options.have_fixed_binary = true;
      }
      else if( ETraits<NM>::name( NM::DATE32 ) == key && -KI == value->t ){
        null_mapping_options.date32_null = value->i;
        null_mapping_options.have_date32 = true;
      }
      else if( ETraits<NM>::name( NM::DATE64 ) == key && -KJ == value->t ){
        null_mapping_options.date64_null = value->j;
        null_mapping_options.have_date64 = true;
      }
      else if( ETraits<NM>::name( NM::TIMESTAMP ) == key && -KJ == value->t ){
        null_mapping_options.timestamp_null = value->j;
        null_mapping_options.have_timestamp = true;
      }
      else if( ETraits<NM>::name( NM::TIME32 ) == key && -KI == value->t ){
        null_mapping_options.time32_null = value->i;
        null_mapping_options.have_time32 = true;
      }
      else if( ETraits<NM>::name( NM::TIME64 ) == key && -KJ == value->t ){
        null_mapping_options.time64_null = value->j;
        null_mapping_options.have_time64 = true;
      }
      else if( ETraits<NM>::name( NM::DECIMAL ) == key && -KF == value->t ){
        null_mapping_options.decimal_null = value->f;
        null_mapping_options.have_decimal = true;
      }
      else if( ETraits<NM>::name( NM::DURATION ) == key && -KJ == value->t ){
        null_mapping_options.duration_null = value->j;
        null_mapping_options.have_duration = true;
      }
      else if( ETraits<NM>::name( NM::INTERVAL_MONTHS ) == key && -KI == value->t ){
        null_mapping_options.month_interval_null = value->i;
        null_mapping_options.have_month_interval = true;
      }
      else if( ETraits<NM>::name( NM::INTERVAL_DAY_TIME ) == key && -KJ == value->t ){
        null_mapping_options.day_time_interval_null = value->j;
        null_mapping_options.have_day_time_interval = true;
      }
      else if( 101 == value->t ){
        // Ignore generic null, which may be used here to ensure mixed list of options
      }
      else{
        throw InvalidOption(("Unsupported KDB data type for NULL_MAPPING option '" + key + "', type=" + std::to_string( value->t ) + "h" ).c_str());
      }
    }
  }

  void PopulateDictOptions( K keys, K values )
  {
    for( auto i = 0ll; i < values->n; ++i ) {
      const std::string key = ToUpper( kS( keys )[i] );
      if( supported_dict_options.find( key ) == supported_dict_options.end() ){
        throw InvalidOption(("Unsupported dict option '" + key + "'").c_str());
      }
      if( Options::NULL_MAPPING == key )
      {
          PopulateNullMappingOptions( i, values );
      }
    }
  }

  void PopulateMixedOptions(K keys, K values)
  {
    for (auto i = 0ll; i < values->n; ++i) {
      const std::string key = ToUpper(kS(keys)[i]);
      K value = kK(values)[i];
      switch (value->t) {
      case -KJ:
        if (supported_int_options.find(key) == supported_int_options.end())
          throw InvalidOption(("Unsupported int option '" + key + "'").c_str());
        int_options[key] = value->j;
        break;
      case -KS:
        if (supported_string_options.find(key) == supported_string_options.end())
          throw InvalidOption(("Unsupported string option '" + key + "'").c_str());
        string_options[key] = ToUpper(value->s);
        break;
      case KC:
      {
        if (supported_string_options.find(key) == supported_string_options.end())
          throw InvalidOption(("Unsupported string option '" + key + "'").c_str());
        string_options[key] = ToUpper(std::string((char*)kG(value), value->n));
        break;
      }
      case XD:
      {
        if( supported_dict_options.find( key ) == supported_dict_options.end() ){
          throw InvalidOption(("Unsupported dict option '" + key + "'").c_str());
        }
        if( Options::NULL_MAPPING == key )
        {
            PopulateNullMappingOptions( i, values );
        }
        break;
      }
      case 101:
        // Ignore ::
        break;
      default:
        throw InvalidOption(("option '" + key + "' value not -7|-11|10h").c_str());
      }
    }
  }

public:
  class InvalidOption : public std::invalid_argument
  {
  public:
    InvalidOption(const std::string message) : std::invalid_argument(message.c_str())
    {};
  };

  KdbOptions(
          K options
        , const std::set<std::string> supported_string_options_
        , const std::set<std::string> supported_int_options_
        , const std::set<std::string>& supported_dict_options_ = Options::dict_options
        , const std::set<std::string>& supported_null_mapping_options_ = Options::null_mapping_options )
    : null_mapping_options {0}
    , supported_string_options(supported_string_options_)
    , supported_int_options(supported_int_options_)
    , supported_dict_options( supported_dict_options_ )
    , supported_null_mapping_options( supported_null_mapping_options_ )
  {
    if (options != NULL && options->t != 101) {
      if (options->t != 99)
        throw InvalidOption("options not -99h");
      K keys = kK(options)[0];
      if (keys->t != KS)
        throw InvalidOption("options keys not 11h");
      K values = kK(options)[1];
      switch (values->t) {
      case KJ:
        PopulateIntOptions(keys, values);
        break;
      case KS:
        PopulateStringOptions(keys, values);
        break;
      case XD:
        PopulateDictOptions(keys, values);
        break;
      case 0:
        PopulateMixedOptions(keys, values);
        break;
      default:
        throw InvalidOption("options values not 7|11|0h");
      }
    }
  }

  template<arrow::Type::type TypeId = arrow::Type::NA>
  auto GetNullMappingOption() const { return null_mapping_options.GetOption<TypeId>(); }

  void GetNullMappingOptions( Options::NullMapping& null_mapping ) const
  {
      null_mapping = null_mapping_options;
  }

  bool GetStringOption(const std::string key, std::string& result) const
  {
    const auto it = string_options.find(key);
    if (it == string_options.end())
      return false;
    else {
      result = it->second;
      return true;
    }
  }

  bool GetIntOption(const std::string key, int64_t& result) const
  {
    const auto it = int_options.find(key);
    if (it == int_options.end())
      return false;
    else {
      result = it->second;
      return true;
    }
  }
};

} // namespace arrowkdb
} // namespace kx


#endif // __KDB_OPTIONS__
