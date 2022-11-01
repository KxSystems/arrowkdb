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
   * @brief Returns build info regarding the in use arrow library
   *
   * @param unused
   * @return Dictionary detailing various Arrow build info including: Arrow
   * version, shared object version, git description and compiler used.
  */
  EXP K buildInfo(K unused);

  /**
   * @brief Initialise the library
   * @param unused 
   * @return null
  */
  EXP K init(K unused);
}

#endif // __ARROW_KDB_H__
