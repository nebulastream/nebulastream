
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

add_executable(mlir-nesir-print-test "UnitTests/MLIR_Experimental/NESIRPrintTest.cpp")
target_link_libraries(mlir-nesir-print-test
        nes-execution-engine)
add_test(NAME mlir-nesir-print-test COMMAND mlir-nesir-print-test)

#add_executable(mlir-nesir-types-test "UnitTests/MLIR_Experimental/MLIRNESIRTest_Types.cpp")
#target_link_libraries(mlir-nesir-types-test
#        nes-execution-engine)
#add_test(NAME mlir-nesir-types-test COMMAND mlir-nesir-types-test)

add_executable(execution-engine-trace-test "UnitTests/Experimental/TraceTest.cpp")
target_link_libraries(execution-engine-trace-test
        nes-execution-engine)
add_test(NAME execution-engine-trace-test COMMAND execution-engine-trace-test)

add_executable(execution-engine-symbolic-execution-test "UnitTests/Experimental/SymbolicExecutionTest.cpp")
target_link_libraries(execution-engine-symbolic-execution-test
        nes-execution-engine)
add_test(NAME execution-engine-symbolic-execution-test COMMAND execution-engine-symbolic-execution-test)


add_executable(execution-engine-value-test "UnitTests/Experimental/ValueTest.cpp")
target_link_libraries(execution-engine-value-test
        nes-execution-engine)
add_test(NAME execution-engine-value-test COMMAND execution-engine-value-test)

add_executable(execution-engine-ir-creation-test "UnitTests/Experimental/TraceToIRConversionPhaseTest.cpp")
target_link_libraries(execution-engine-ir-creation-test
        nes-execution-engine)
add_test(NAME execution-engine-ir-creation-test COMMAND execution-engine-ir-creation-test)

add_executable(execution-engine-operator-test "UnitTests/Experimental/Interpreter/OperatorTest.cpp")
target_link_libraries(execution-engine-operator-test
        nes-execution-engine)
add_test(NAME execution-engine-operator-test COMMAND execution-engine-operator-test)

add_executable(execution-engine-expression-test "UnitTests/Experimental/Interpreter/ExpressionTest.cpp")
target_link_libraries(execution-engine-expression-test
        nes-execution-engine)
add_test(NAME execution-engine-expression-test COMMAND execution-engine-expression-test)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

