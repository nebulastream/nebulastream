
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(ExternalProject)

### MLIR Tests ###
get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)
target_link_libraries(nes-execution-engine
        PUBLIC
        nes
        ${dialect_libs}
        ${conversion_libs}
        ${GTEST_LIBRARIES}
        MLIRReconcileUnrealizedCasts
        MLIRMemRefToLLVM
        MLIRExecutionEngine
        )
add_executable(mlir-nesir-loop-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_Loop.cpp")
target_link_libraries(mlir-nesir-loop-test
        nes-execution-engine)
add_test(NAME mlir-nesir-loop-test COMMAND mlir-nesir-loop-test)

add_executable(mlir-nesir-if-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_IF.cpp")
target_link_libraries(mlir-nesir-if-test
        nes-execution-engine)
add_test(NAME mlir-nesir-if-test COMMAND mlir-nesir-if-test)

#add_executable(mlir-nesir-types-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_Types.cpp")
#target_link_libraries(mlir-nesir-types-test
#        nes-execution-engine)
#add_test(NAME mlir-nesir-types-test COMMAND mlir-nesir-types-test)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

