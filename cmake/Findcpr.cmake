find_path(cpr_INCLUDE_DIR
        NAMES cpr.h
        PATH_SUFFIXES cpr
        )
find_library(cpr_LIBRARY
        NAMES cpr
        )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cpr
        FOUND_VAR cpr_FOUND
        REQUIRED_VARS
        cpr_LIBRARY
        cpr_INCLUDE_DIR
        )
if(cpr_FOUND AND NOT TARGET cpr::cpr)
    add_library(cpr::cpr UNKNOWN IMPORTED)
    set_target_properties(cpr::cpr PROPERTIES
            IMPORTED_LOCATION "${cpr_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${cpr_INCLUDE_DIR};${cpr_INCLUDE_DIR}/.."
            )
    message(STATUS "cpr include dir: ${cpr_INCLUDE_DIR}")
endif()
