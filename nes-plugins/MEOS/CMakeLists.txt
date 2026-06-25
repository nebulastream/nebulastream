# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create MEOS plugin library
add_library(nes-meos STATIC MEOSWrapper.cpp)

# Set include directories
target_include_directories(nes-meos
        PUBLIC include
        PUBLIC .
)

# Link with MEOS library
# Check if meos variables are available from parent scope
if(NOT DEFINED meos OR NOT DEFINED meos_INCLUDE_DIR)
    # If not found in parent scope, find it here
    find_package(meos REQUIRED)
endif()

if(meos AND meos_INCLUDE_DIR)
    target_link_libraries(nes-meos PUBLIC ${meos})
    target_include_directories(nes-meos PUBLIC ${meos_INCLUDE_DIR})
    # Also link MEOS dependencies
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GEOS REQUIRED geos)
    pkg_check_modules(PROJ REQUIRED proj)
    find_library(GSL_LIB gsl)
    find_library(JSON_C_LIB json-c)
    target_link_libraries(nes-meos PUBLIC ${GEOS_LIBRARIES} ${PROJ_LIBRARIES} ${GSL_LIB} ${JSON_C_LIB})
else()
    message(FATAL_ERROR "MEOS library not found. meos=${meos}, meos_INCLUDE_DIR=${meos_INCLUDE_DIR}")
endif()

# Link with NebulaStream dependencies  
target_link_libraries(nes-meos PUBLIC nes-common nes-data-types)

# Add to build_all_plugins target
add_dependencies(build_all_plugins nes-meos)