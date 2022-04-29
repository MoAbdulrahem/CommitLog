#pragma once

// clang-format off
#if defined _WIN32 || defined __CYGWIN__
  #ifdef BUILDING_DIBIBASE
    #define DIBIBASE_PUBLIC __declspec(dllexport)
  #else
    #define DIBIBASE_PUBLIC __declspec(dllimport)
  #endif
#else
  #ifdef BUILDING_DIBIBASE
    #define DIBIBASE_PUBLIC __attribute__((visibility("default")))
  #else
    #define DIBIBASE_PUBLIC
  #endif
#endif
// clang-format on

#define DIBIBASE_PACKED __attribute__((packed))
