# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)
include(GoogleTest)
#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

add_library(nes-test-util
        "Util/src/BaseIntegrationTest.cpp"
        "Util/src/BaseUnitTest.cpp")

find_package(PkgConfig)
pkg_check_modules(uuid REQUIRED IMPORTED_TARGET GLOBAL uuid)

message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-test-util PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/Util/include")
target_link_libraries(nes-test-util PUBLIC ${GTEST_LIBRARIES} nes-common PkgConfig::uuid)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-test-util REUSE_FROM nes-common)
endif ()

add_subdirectory(UnitTests)
