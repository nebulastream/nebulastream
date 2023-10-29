# Try to find the cppkafka library and headers
# This module sets the following variables:
# CPPKAFKA_FOUND - True if cppkafka was found
# CPPKAFKA_INCLUDE_DIR - Directories where the cppkafka headers are located
# CPPKAFKA_LIBRARIES - Libraries to link against for cppkafka

find_path(CPPKAFKA_INCLUDE_DIR NAMES cppkafka/cppkafka.h
        HINTS
        ${NES_DEPENDENCIES_BINARY_ROOT}/include
        PATH_SUFFIXES cppkafka)

find_library(CPPKAFKA_LIBRARY NAMES cppkafka
        HINTS
        ${NES_DEPENDENCIES_BINARY_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cppkafka DEFAULT_MSG
        CPPKAFKA_LIBRARY
        CPPKAFKA_INCLUDE_DIR)

set(CPPKAFKA_LIBRARIES ${CPPKAFKA_LIBRARY})
set(CPPKAFKA_INCLUDE_DIRS ${CPPKAFKA_INCLUDE_DIR})

if(CPPKAFKA_FOUND AND NOT TARGET cppkafka::cppkafka)
    add_library(cppkafka::cppkafka STATIC IMPORTED)
    set_target_properties(cppkafka::cppkafka PROPERTIES
            IMPORTED_LOCATION "${CPPKAFKA_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${CPPKAFKA_INCLUDE_DIRS}")
endif()
