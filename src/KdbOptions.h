#ifndef __KDB_OPTIONS__
#define __KDB_OPTIONS__

#include <string>
#include <map>
#include <stdexcept>

#include "k.h"

class KdbOptions
{
private:
  std::map<std::string, std::string> string_options;
  std::map<std::string,  int64_t> int_options;

private:
  void PopulateIntOptions(K keys, K values)
  {
    for (auto i = 0; i < values->n; ++i)
      int_options[kS(keys)[i]] = kJ(values)[i];
  }

  void PopulateStringOptions(K keys, K values)
  {
    for (auto i = 0; i < values->n; ++i)
      string_options[kS(keys)[i]] = kS(values)[i];
  }

  void PopulateMixedOptions(K keys, K values)
  {
    for (auto i = 0; i < values->n; ++i) {
      std::string key = kS(keys)[i];
      K value = kK(values)[i];
      switch (value->t) {
      case -KJ:
        int_options[key] = value->j;
        break;
      case -KS:
        string_options[key] = value->s;
        break;
      case 101:
        // Ignore ::
        break;
      default:
        throw std::exception(("option '" + key + "' value not -7|-11h").c_str());
      }
    }
  }
public:
  KdbOptions(K options)
  {
    if (options != NULL && options->t != 101) {
      if (options->t != 99)
        throw std::exception("options not -99h");
      K keys = kK(options)[0];
      if (keys->t != KS)
        throw std::exception("options keys not 11h");
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
        throw std::exception("options values not 7|11|0h");
      }
    }
  }

  bool GetStringOption(std::string key, std::string& result)
  {
    auto it = string_options.find(key);
    if (it == string_options.end())
      return false;
    else {
      result = it->second;
      return true;
    }
  }

  bool GetIntOption(std::string key, int64_t& result)
  {
    auto it = int_options.find(key);
    if (it == int_options.end())
      return false;
    else {
      result = it->second;
      return true;
    }
  }
};

#endif // __KDB_OPTIONS__

