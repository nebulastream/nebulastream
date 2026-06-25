
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_path(meos_INCLUDE_DIR
        NAMES meos.h
        HINTS $ENV{MEOS_ROOT} ${MEOS_ROOT}
        PATH_SUFFIXES include
        PATHS /usr/local/include)

find_library(meos NAMES meos
        HINTS $ENV{MEOS_ROOT} ${MEOS_ROOT}
        PATH_SUFFIXES lib lib64
        PATHS /usr/local/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(meos
        FOUND_VAR meos_FOUND
        REQUIRED_VARS
        meos
        meos_INCLUDE_DIR 
        )

if(meos_FOUND AND NOT TARGET meos::meos)
    add_library(meos::meos UNKNOWN IMPORTED)
    
    # Find MEOS dependencies
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GEOS REQUIRED geos)
    pkg_check_modules(PROJ REQUIRED proj)
    find_library(GSL_LIB gsl)
    find_library(JSON_C_LIB json-c)
    
    set_target_properties(meos::meos PROPERTIES
            IMPORTED_LOCATION "${meos}"
            INTERFACE_INCLUDE_DIRECTORIES "${meos_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "${GEOS_LIBRARIES};${PROJ_LIBRARIES};${GSL_LIB};${JSON_C_LIB}"
            )
    message(STATUS "meos include dir: ${meos_INCLUDE_DIR}")
    message(STATUS "meos library found")
    message(STATUS "MEOS dependencies: GEOS=${GEOS_LIBRARIES}, PROJ=${PROJ_LIBRARIES}, GSL=${GSL_LIB}, JSON-C=${JSON_C_LIB}")
endif()
