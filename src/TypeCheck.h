#ifndef __TYPE_CHECK_H__
#define __TYPE_CHECK_H__

#include <stdexcept>
#include <string>
#include <cstdint>


// Set of macros to assist with performing the type checks such the arguments
// required to generate the exception message are not evaluated unless the
// condition is met
#define TYPE_CHECK_ARRAY(condition, datatype_name, expected, received) \
  if (condition) throw TypeCheckArray(datatype_name, expected, received);
#define TYPE_CHECK_ITEM(condition, datatype_name, expected, received) \
  if (condition) throw TypeCheckItem(datatype_name, expected, received);
#define TYPE_CHECK_UNSUPPORTED(datatype_name) \
  throw TypeCheckUnsupported(datatype_name);
#define TYPE_CHECK_LENGTH(condition, datatype_name, expected, received) \
  if (condition) throw TypeCheckLength(datatype_name, expected, received);

// Hierachy of TypeCheck exceptions with each derived type being using for a
// specific check when converting from a kdb object to an arow arrow.
class TypeCheck : public std::invalid_argument
{
public:
  TypeCheck(std::string message) : std::invalid_argument(message.c_str()) {};
};

class TypeCheckArray : public TypeCheck
{
public:
  TypeCheckArray(const std::string& datatype_name, int expected, int received) :
    TypeCheck("Invalid array, datatype: '" + datatype_name + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

class TypeCheckItem : public TypeCheck
{
public:
  TypeCheckItem(const std::string& datatype_name, int expected, int received) :
    TypeCheck("Invalid item, datatype: '" + datatype_name + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

class TypeCheckUnsupported : public TypeCheck
{
public:
  TypeCheckUnsupported(const std::string& datatype_name) :
    TypeCheck("Unsupported datatype: '" + datatype_name + "'")
  {};
};

class TypeCheckLength : public TypeCheck
{
public:
  TypeCheckLength(const std::string& datatype_name, int64_t expected, int64_t received) :
    TypeCheck("Invalid length, datatype: '" + datatype_name + "', expected: " + std::to_string(expected) + ", received: " + std::to_string(received))
  {};
};

#endif // __TYPE_CHECK_H__
