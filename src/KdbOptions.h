#ifndef __KDB_OPTIONS__
#define __KDB_OPTIONS__

#include <string>
#include <map>
#include <stdexcept>
#include <cctype>
#include <set>

#include "k.h"


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
    using Names = std::map< E, std::string >;

    static std::string name( E enumerator )
    {
        auto it = names.find( enumerator );
        if( it != names.end() )
        {
            return it->second;
        }

        return "unknown";
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
  const std::string NM_INT_16 = "INT16";
  const std::string NM_INT_32 = "INT32";
  const std::string NM_STRING = "STRING";
  const std::string NM_LARGE_STRING = "LARGE_STRING";

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
    NM_INT_16,
    NM_INT_32,
    NM_STRING,
    NM_LARGE_STRING
  };

  struct NullMapping
  {
      enum class Type: int{
            INT_16
          , INT_32
          , STRING
          , LARGE_STRING
      };

      bool have_int16;
      int16_t int16_null;
      bool have_int32;
      int32_t int32_null;
      bool have_string;
      std::string string_null;
      bool have_large_string;
      std::string large_string_null;
  };
}

template<>
inline const ETraits< Options::NullMapping::Type >::Names ETraits< Options::NullMapping::Type >::names {
    { Options::NullMapping::Type::INT_16, Options::NM_INT_16 }
  , { Options::NullMapping::Type::INT_32, Options::NM_INT_32 }
  , { Options::NullMapping::Type::STRING, Options::NM_STRING }
  , { Options::NullMapping::Type::LARGE_STRING, Options::NM_LARGE_STRING }
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
    using NM = Options::NullMapping::Type;

    K keys = kK( kK( dict )[index] )[0];
    K values = kK( kK( dict )[index] )[1];
    if( KS != keys->t ){
        throw InvalidOption( "Unsupported KDB data type for NULL_MAPPING keys (expected=11h), type=" + std::to_string( keys->t ) );
    }
    if( 0 != values->t ){
        throw InvalidOption( "Unsupported KDB data type for NULL_MAPPING values (extected=0), type=" + std::to_string( keys->t ) );
    }
    for( auto i = 0ll; i < values->n; ++i ){
      const std::string key = ToUpper( kS( keys )[i] );
      if( supported_null_mapping_options.find( key ) == supported_null_mapping_options.end() ){
        throw InvalidOption(("Unsupported NULL_MAPPING option '" + key + "'").c_str());
      }
      K value = kK( values )[i];
      if( ETraits<NM>::name( NM::INT_16 ) == key && -KH == value->t ){
        null_mapping_options.int16_null = value->h;
        null_mapping_options.have_int16 = true;
      }
      else if( ETraits<NM>::name( NM::INT_32 ) == key && -KI == value->t ){
        null_mapping_options.int32_null = value->i;
        null_mapping_options.have_int32 = true;
      }
      else if( ETraits<NM>::name( NM::STRING ) == key && KC == value->t ){
        null_mapping_options.string_null.assign( (char*)kC( value ), value->n );
        null_mapping_options.have_string = true;
      }
      else if( ETraits<NM>::name( NM::LARGE_STRING ) == key && KC == value->t ){
        null_mapping_options.large_string_null.assign( (char*)kC( value ), value->n );
        null_mapping_options.have_large_string = true;
      }
      else if( 101 == value->t ){
        // Ignore generic null, which may be used here to ensure mixed list of options
      }
      else{
        throw InvalidOption(("Unsupported KDB data type for NULL_MAPPING option '" + key + "', type=" + std::to_string( value->t ) ).c_str());
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

  template<Options::NullMapping::Type TypeId>
  auto GetNullMappingOption( bool& result );

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

template<>
inline auto KdbOptions::GetNullMappingOption<Options::NullMapping::Type::INT_16>( bool& result )
{
    result = null_mapping_options.have_int16;

    return null_mapping_options.int16_null;
}

template<>
inline auto KdbOptions::GetNullMappingOption<Options::NullMapping::Type::INT_32>( bool& result )
{
    result = null_mapping_options.have_int32;

    return null_mapping_options.int32_null;
}

template<>
inline auto KdbOptions::GetNullMappingOption<Options::NullMapping::Type::STRING>( bool& result )
{
    result = null_mapping_options.have_string;

    return null_mapping_options.string_null;
}

template<>
inline auto KdbOptions::GetNullMappingOption<Options::NullMapping::Type::LARGE_STRING>( bool& result )
{
    result = null_mapping_options.have_large_string;

    return null_mapping_options.large_string_null;
}

} // namespace arrowkdb
} // namespace kx


#endif // __KDB_OPTIONS__
