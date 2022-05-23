
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

if (CMAKE_CROSSCOMPILING)
    set(CMAKE_ARGS_CROSS -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_TOOLCHAIN_FILE=/opt/toolchain/toolchain-aarch64-llvm.cmake
            -DBoost_NO_SYSTEM_PATHS=TRUE
            -DBoost_INCLUDE_DIR=/opt/sysroots/aarch64-linux-gnu/usr/include/
            -DBoost_LIBRARY_DIR=/opt/sysroots/aarch64-linux-gnu/usr/lib/
            )
else ()
    set(CMAKE_ARGS_CROSS "")
endif ()

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test_data/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/test_data")

#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

add_library(nes-execution-engine-tests INTERFACE)
target_include_directories(nes-execution-engine-tests INTERFACE ${GTEST_INCLUDE_DIRS} "../include")
target_include_directories(nes-execution-engine-tests INTERFACE "${CMAKE_BINARY_DIR}")
message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-execution-engine-tests INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/util/")
target_link_libraries(nes-execution-engine-tests INTERFACE ${GTEST_LIBRARIES} nes)


#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------

### MLIR Tests ###
if(NES_USE_MLIR)
    get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
    get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)
    target_link_libraries(nes-execution-engine-tests
            INTERFACE
            ${dialect_libs}
            ${conversion_libs}
            MLIRReconcileUnrealizedCasts
            MLIRMemRefToLLVM
            MLIRExecutionEngine
            )
    add_executable(mlir-nesir-loop-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_Loop.cpp")
    target_link_libraries(mlir-nesir-loop-test
            nes-execution-engine-tests)
    add_test(NAME mlir-nesir-loop-test COMMAND mlir-nesir-loop-test)
    add_executable(mlir-nesir-if-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_IF.cpp")
    target_link_libraries(mlir-nesir-if-test
            nes-execution-engine-tests)
    add_test(NAME mlir-nesir-if-test COMMAND mlir-nesir-if-test)
    add_executable(mlir-nesir-types-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_Types.cpp")
    target_link_libraries(mlir-nesir-types-test
            nes-execution-engine-tests)
    add_test(NAME mlir-nesir-types-test COMMAND mlir-nesir-types-test)
endif (NES_USE_MLIR)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

