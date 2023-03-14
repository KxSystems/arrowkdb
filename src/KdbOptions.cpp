#include <iterator>

#include "KdbOptions.h"

namespace{

template<arrow::Type::type TypeId>
auto make_handler()
{
  return std::make_pair( TypeId, &kx::arrowkdb::KdbOptions::HandleNullMapping<TypeId> );
}

} // namespace

namespace kx {

namespace arrowkdb {

const KdbOptions::NullMappingHandlers KdbOptions::null_mapping_handlers = {
      make_handler<arrow::Type::BOOL>()
    , make_handler<arrow::Type::UINT8>()
    , make_handler<arrow::Type::INT8>()
    , make_handler<arrow::Type::UINT16>()
    , make_handler<arrow::Type::INT16>()
    , make_handler<arrow::Type::UINT32>()
    , make_handler<arrow::Type::INT32>()
    , make_handler<arrow::Type::UINT64>()
    , make_handler<arrow::Type::INT64>()
    , make_handler<arrow::Type::HALF_FLOAT>()
    , make_handler<arrow::Type::FLOAT>()
    , make_handler<arrow::Type::DOUBLE>()
    , make_handler<arrow::Type::STRING>()
    , make_handler<arrow::Type::LARGE_STRING>()
    , make_handler<arrow::Type::BINARY>()
    , make_handler<arrow::Type::LARGE_BINARY>()
    , make_handler<arrow::Type::FIXED_SIZE_BINARY>()
    , make_handler<arrow::Type::DATE32>()
    , make_handler<arrow::Type::DATE64>()
    , make_handler<arrow::Type::TIMESTAMP>()
    , make_handler<arrow::Type::TIME32>()
    , make_handler<arrow::Type::TIME64>()
    , make_handler<arrow::Type::DECIMAL>()
    , make_handler<arrow::Type::DURATION>()
    , make_handler<arrow::Type::INTERVAL_MONTHS>()
    , make_handler<arrow::Type::INTERVAL_DAY_TIME>()
};

KdbOptions::KdbOptions(
        K options
      , const std::set<std::string>& supported_string_options_
      , const std::set<std::string>& supported_int_options_
      , const std::set<std::string>& supported_dict_options_ )
  : null_mapping_options {0}
  , supported_string_options(supported_string_options_)
  , supported_int_options(supported_int_options_)
  , supported_dict_options( supported_dict_options_ )
  , null_mapping_types {
      { arrow::Type::BOOL, arrowkdb::Options::NM_BOOLEAN }
    , { arrow::Type::UINT8, arrowkdb::Options::NM_UINT_8 }
    , { arrow::Type::INT8, arrowkdb::Options::NM_INT_8 }
    , { arrow::Type::UINT16, arrowkdb::Options::NM_UINT_16 }
    , { arrow::Type::INT16, arrowkdb::Options::NM_INT_16 }
    , { arrow::Type::UINT32, arrowkdb::Options::NM_UINT_32 }
    , { arrow::Type::INT32, arrowkdb::Options::NM_INT_32 }
    , { arrow::Type::UINT64, arrowkdb::Options::NM_UINT_64 }
    , { arrow::Type::INT64, arrowkdb::Options::NM_INT_64 }
    , { arrow::Type::HALF_FLOAT, arrowkdb::Options::NM_FLOAT_16 }
    , { arrow::Type::FLOAT, arrowkdb::Options::NM_FLOAT_32 }
    , { arrow::Type::DOUBLE, arrowkdb::Options::NM_FLOAT_64 }
    , { arrow::Type::STRING, arrowkdb::Options::NM_STRING }
    , { arrow::Type::LARGE_STRING, arrowkdb::Options::NM_LARGE_STRING }
    , { arrow::Type::BINARY, arrowkdb::Options::NM_BINARY }
    , { arrow::Type::LARGE_BINARY, arrowkdb::Options::NM_LARGE_BINARY }
    , { arrow::Type::FIXED_SIZE_BINARY, arrowkdb::Options::NM_FIXED_BINARY }
    , { arrow::Type::DATE32, arrowkdb::Options::NM_DATE_32 }
    , { arrow::Type::DATE64, arrowkdb::Options::NM_DATE_64 }
    , { arrow::Type::TIMESTAMP, arrowkdb::Options::NM_TIMESTAMP }
    , { arrow::Type::TIME32, arrowkdb::Options::NM_TIME_32 }
    , { arrow::Type::TIME64, arrowkdb::Options::NM_TIME_64 }
    , { arrow::Type::DECIMAL, arrowkdb::Options::NM_DECIMAL }
    , { arrow::Type::DURATION, arrowkdb::Options::NM_DURATION }
    , { arrow::Type::INTERVAL_MONTHS, arrowkdb::Options::NM_MONTH_INTERVAL }
    , { arrow::Type::INTERVAL_DAY_TIME, arrowkdb::Options::NM_DAY_TIME_INTERVAL } }
{
  std::transform(
        null_mapping_types.begin()
      , null_mapping_types.end()
      , std::inserter( supported_null_mapping_options, end( supported_null_mapping_options ) )
      , []( const auto& value ){
    return value.second;
  } );
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

const std::string KdbOptions::ToUpper(std::string str) const
{
  std::string upper;
  for (auto i : str)
    upper.push_back((unsigned char)std::toupper(i));
  return upper;
}

const std::string KdbOptions::ToLower( std::string str ) const
{
  std::transform( str.begin(), str.end(), str.begin(), ::tolower );

  return str;
}

void KdbOptions::PopulateIntOptions(K keys, K values)
{
  for (auto i = 0ll; i < values->n; ++i) {
    const std::string key = ToUpper(kS(keys)[i]);
    if (supported_int_options.find(key) == supported_int_options.end())
      throw InvalidOption(("Unsupported int option '" + key + "'").c_str());
    int_options[key] = kJ(values)[i];
  }
}

void KdbOptions::PopulateStringOptions(K keys, K values)
{
  for (auto i = 0ll; i < values->n; ++i) {
    const std::string key = ToUpper(kS(keys)[i]);
    if (supported_string_options.find(key) == supported_string_options.end())
      throw InvalidOption(("Unsupported string option '" + key + "'").c_str());
    string_options[key] = ToUpper(kS(values)[i]);
  }
}

void KdbOptions::PopulateNullMappingOptions( long long index, K dict )
{
  K keys = kK( kK( dict )[index] )[0];
  K values = kK( kK( dict )[index] )[1];
  if( KS != keys->t ){
      throw InvalidOption( "Unsupported KDB data type for NULL_MAPPING keys (expected=11h), type=" + std::to_string( keys->t ) + "h" );
  }
  if( 0 != values->t ){
      throw InvalidOption( "Unsupported KDB data type for NULL_MAPPING values (extected=0h), type=" + std::to_string( values->t ) + "h" );
  }
  for( auto i = 0ll; i < values->n; ++i ){
    const std::string key = ToLower( kS( keys )[i] );
    if( supported_null_mapping_options.find( key ) == supported_null_mapping_options.end() ){
      throw InvalidOption( "Unsupported NULL_MAPPING option '" + key + "'" );
    }
    K value = kK( values )[i];
    auto option = GetNullMappingType( key );
    auto it = null_mapping_handlers.find( option );
    if( it != null_mapping_handlers.end() ){
        ( this->*it->second )( key, value );
    }
    else if( 101 == value->t ){
      // Ignore generic null, which may be used here to ensure mixed list of options
    }
    else{
      throw InvalidOption( "Unhandled NULL_MAPPING option '" + key + "', type=" + std::to_string( keys->t ) + "h" );
    }
  }
}

void KdbOptions::PopulateDictOptions( K keys, K values )
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

void KdbOptions::PopulateMixedOptions(K keys, K values)
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

arrow::Type::type KdbOptions::GetNullMappingType( const std::string& option )
{
  auto it = std::find_if( null_mapping_types.begin(), null_mapping_types.end(), [&option]( const auto& value ){
    return option == value.second;
  } );
  if( it != null_mapping_types.end() ){
    return it->first;
  }

  return arrow::Type::NA;
}

bool KdbOptions::GetStringOption(const std::string key, std::string& result) const
{
  const auto it = string_options.find(key);
  if (it == string_options.end())
    return false;
  else {
    result = it->second;
    return true;
  }
}

bool KdbOptions::GetIntOption(const std::string key, int64_t& result) const
{
  const auto it = int_options.find(key);
  if (it == int_options.end())
    return false;
  else {
    result = it->second;
    return true;
  }
}

} // namespace arrowkdb

} // kx
