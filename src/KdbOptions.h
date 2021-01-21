#ifndef __KDB_OPTIONS__
#define __KDB_OPTIONS__

#include <string>
#include <map>
#include <stdexcept>
#include <cctype>

#include "k.h"


// Helper class for reading function argument containing dictionary of options
//
// Dictionary key:    KS
// Dictionary value:  KS or
//                    KJ or
//                    0 of -KS|-KJ
class KdbOptions
{
private:
  std::map<std::string, std::string> string_options;
  std::map<std::string,  int64_t> int_options;

private:
  std::string ToUpper(std::string str)
  {
    std::string upper;
    for (auto i : str)
      upper.push_back((unsigned char)std::toupper(i));
    return upper;
  }

  void PopulateIntOptions(K keys, K values)
  {
    for (auto i = 0; i < values->n; ++i)
      int_options[ToUpper(kS(keys)[i])] = kJ(values)[i];
  }

  void PopulateStringOptions(K keys, K values)
  {
    for (auto i = 0; i < values->n; ++i)
      string_options[ToUpper(kS(keys)[i])] = ToUpper(kS(values)[i]);
  }

  void PopulateMixedOptions(K keys, K values)
  {
    for (auto i = 0; i < values->n; ++i) {
      std::string key = ToUpper(kS(keys)[i]);
      K value = kK(values)[i];
      switch (value->t) {
      case -KJ:
        int_options[key] = value->j;
        break;
      case -KS:
        string_options[key] = ToUpper(value->s);
        break;
      case 101:
        // Ignore ::
        break;
      default:
        throw InvalidOption(("option '" + key + "' value not -7|-11h").c_str());
      }
    }
  }

public:
  class InvalidOption : public std::invalid_argument
  {
  public:
    InvalidOption(std::string message) : std::invalid_argument(message.c_str())
    {};
  };

  KdbOptions(K options)
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
      case 0:
        PopulateMixedOptions(keys, values);
        break;
      default:
        throw InvalidOption("options values not 7|11|0h");
      }
    }
  }

  bool GetStringOption(std::string key, std::string& result)
  {
    auto it = string_options.find(ToUpper(key));
    if (it == string_options.end())
      return false;
    else {
      result = it->second;
      return true;
    }
  }

  bool GetIntOption(std::string key, int64_t& result)
  {
    auto it = int_options.find(ToUpper(key));
    if (it == int_options.end())
      return false;
    else {
      result = it->second;
      return true;
    }
  }
};

#endif // __KDB_OPTIONS__

