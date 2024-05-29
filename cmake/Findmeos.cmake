find_path(meos_INCLUDE_DIR
        NAMES meos.h
        PATHS /usr/local/include
        )

find_library(meos NAMES meos
        PATHS /usr/local/lib
        )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(meos
        FOUND_VAR meos_FOUND
        REQUIRED_VARS
        meos
        meos_INCLUDE_DIR 
        )

if(meos_FOUND AND NOT TARGET meos::meos)
    add_library(meos::meos UNKNOWN IMPORTED)
    set_target_properties(meos::meos PROPERTIES
            IMPORTED_LOCATION "${meos}"
            INTERFACE_INCLUDE_DIRECTORIES "${meos_INCLUDE_DIR}"
            )
    message(STATUS "meos include dir: ${meos_INCLUDE_DIR}")
    message(STATUS "meos library found")
endif()