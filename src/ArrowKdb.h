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
   * @brief Displays the statistics of the arrow memory pool: bytes allocated,
   * max memory and backend allocator name
   * 
   * @param unused 
   * @return NULL
  */
  EXP K getMemoryPoolStats(K unused);
}

#endif // __ARROW_KDB_H__
