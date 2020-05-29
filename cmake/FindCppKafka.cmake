if(NOT CppKafka_FOUND)
  find_library(CppKafka_LIBRARIES
    NAMES libcppkafka.so libcppkafka.dylib
    HINTS /usr/local/lib)

  find_path(CppKafka_INCLUDE_DIRS cppkafka/cppkafka.h /usr/local/include)
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(CppKafka
    DEFAULT_MSG
    CppKafka_LIBRARIES
    CppKafka_INCLUDE_DIRS)
  message("Found cppkafka include dirs: ${CppKafka_INCLUDE_DIRS}")
  message("Found cppkafka library: ${CppKafka_LIBRARIES}")
  message("CppKafka_FOUND: ${CppKafka_FOUND}")
  mark_as_advanced(
    CppKafka_LIBRARIES
    CppKafka_INCLUDE_DIRS
    )

endif()
