#ifndef __ARROW_KDB_H__
#define __ARROW_KDB_H__

#include <k.h>

#ifdef _WIN32
#define EXP __declspec(dllexport)
#else
#define EXP
#endif // _WIN32


extern "C"
{
  /**
   * @brief Utility function for wrapping a kdb object in a one element mixed
   * list.
   *
   * Similar to how enlist is used to create one element simple lists, this
   * function can be used to create one element mixed lists.  This isn't
   * possible to do directly in q since it will automatically remove the
   * surrounding mixed list.
   *
   * This is necessary in certain edge cases (e.g. tables with a single column,
   * one element nested datatype arrays, etc.) to adhere to the arrow structured
   * data mappings used by the interface.  Not adhering to these mapping would
   * otherwise cause type checking errors.
   *
   * @param value The kdb object to be wrapped in a one element mixed list.
   * @return      Mixed list containing the object.
  */
  EXP K mixed(K value);

  EXP K getMemoryPoolStats(K unused);
}

#endif // __ARROW_KDB_H__
