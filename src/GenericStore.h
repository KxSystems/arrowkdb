#ifndef __GENERIC_STORE_H__
#define __GENERIC_STORE_H__

#include <map>
#include <memory>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "ArrowKdb.h"


/**
 * @brief Templated singleton which maintains a mapping from long identifiers to
 * their corresponding arrow objects.
 *
 * This is specialized by the DatatypeStore, FieldStore and SchemaStore which
 * all require the same functionality on different arrow shared pointer types.
*/
template <typename T>
class GenericStore
{
private:
  static class GenericStore* instance; // singleton instance

  long counter; // incremented before an object is added

  // Forward and reverse lookup maps between the identifiers and their objects
  std::map<long, T> forward_lookup;
  std::map<T, long> reverse_lookup;

private:
  GenericStore() : counter(0) {};

  /**
   * @brief Iterates through the lookup maps checking if an arrow object is
   * already present in the store which is equal to the one specified. 
   *
   * @param value Arrow object to check for equality
   * @return      0 if an equal arrow object is not found, the existing
   * identifier otherwise
  */
  long FindEqual(T value)
  {
    for (auto i : forward_lookup)
      if (value->Equals(i.second))
        return i.first;

    return 0;
  }

public:
  /**
   * @brief Returns the singlton instance, constructing it not already existing
   * @return GenericStore instance
  */
  static GenericStore* Instance()
  {
    if (instance == nullptr)
      instance = new GenericStore<T>();

    return instance;
  }

  /**
   * @brief Adds an arrow object to the lookup maps.  If an existing equal
   * object is already present it will return the identifier for that instead.
   * This avoid polluting the store with multiple equal objects.
   *
   * @param value Arrow object to add
   * @return      Identifier for that object
  */
  long Add(T value)
  {
    if (auto equal = FindEqual(value))
      return equal;

    // Add forward lookup: long > value
    long value_id = ++counter;
    forward_lookup[value_id] = value;

    // Add reverse lookup: value > long
    reverse_lookup[value] = value_id;

    return value_id;
  }

  /**
   * @brief Removes an arrow object from the lookup maps
   *
   * @param value_id  The identifier of the arrow object to be removed
   * @return          True on success, false otherwise
  */
  bool Remove(long value_id)
  {
    auto lookup = forward_lookup.find(value_id);
    if (lookup == forward_lookup.end())
      return false;

    // Get reference to the object and remove the reverse lookup first
    auto value = lookup->second;
    reverse_lookup.erase(value);

    // Remove the forward lookup
    forward_lookup.erase(value_id);

    return true;
  }

  /**
   * @brief Returns the arrow object found by searching the forward lookup map
   * for the specified identifier
   *
   * @param value_id The identifier of the arrow object to search for
   * @return         If found the arrow object, NULL otherwise
  */
  T Find(long value_id)
  {
    auto lookup = forward_lookup.find(value_id);
    if (lookup == forward_lookup.end())
      return T();

    return lookup->second;
  }

  /**
   * @brief Return the object identifier by searching the reverse lookup map
   * for the specified arrow object
   *
   * @param value The arrow object to seach for
   * @return      If found the object identifier, 0 otherwise
  */
  long ReverseFind(T value)
  {
    // Reverse lookup is only used internally by the interface so insert the
    // object if it's not already present.  This avoids having to add this logic
    // into all the calling functions.
    auto lookup = reverse_lookup.find(value);
    if (lookup == reverse_lookup.end())
      return Add(value);
    else
      return lookup->second;
  }

  /**
   * @brief Returns all object identifiers currently held in the store
   *
   * @return Vector of object identifiers
  */
  const std::vector<long> List(void)
  {
    std::vector<long> result;
    for (auto it : forward_lookup)
      result.push_back(it.first);
    return result;
  }
};


#endif // __GENERIC_STORE_H__
