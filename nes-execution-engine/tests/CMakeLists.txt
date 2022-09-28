
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

add_executable(execution-engine-trace-test "UnitTests/Experimental/TraceTest.cpp")
target_link_libraries(execution-engine-trace-test
         nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-trace-test COMMAND execution-engine-trace-test)

add_executable(execution-engine-loop-inference-phase-test "UnitTests/Experimental/LoopInferencePhaseTest.cpp")
target_link_libraries(execution-engine-loop-inference-phase-test
         nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-loop-inference-phase-test COMMAND execution-engine-loop-inference-phase-test)


add_executable(execution-engine-value-test "UnitTests/Experimental/ValueTest.cpp")
target_link_libraries(execution-engine-value-test
         nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-value-test COMMAND execution-engine-value-test)

add_executable(execution-engine-operator-test "UnitTests/Experimental/Interpreter/OperatorTest.cpp")
target_link_libraries(execution-engine-operator-test
         nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-operator-test COMMAND execution-engine-operator-test)

add_executable(execution-engine-expression-test "UnitTests/Experimental/Interpreter/ExpressionTest.cpp")
target_link_libraries(execution-engine-expression-test  nes-execution-engine ${GTEST_LIBRARIES})
add_test(NAME execution-engine-expression-test COMMAND execution-engine-expression-test)

add_subdirectory(UnitTests/Experimental/ExecutionTests)

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_FLOUNDER)
    add_executable(execution-engine-flounder-test "UnitTests/Experimental/Flounder/FlounderTest.cpp")
    target_link_libraries(execution-engine-flounder-test
             nes-execution-engine ${GTEST_LIBRARIES})
    add_test(NAME execution-engine-flounder-test COMMAND execution-engine-flounder-test)

    add_executable(execution-engine-flounder-exec-test "UnitTests/Experimental/Flounder/FlounderExpressionExecutionTest.cpp")
    target_link_libraries(execution-engine-flounder-exec-test  nes-execution-engine ${GTEST_LIBRARIES})
    add_test(NAME execution-engine-flounder-exec-test COMMAND execution-engine-flounder-exec-test)
endif ()


add_subdirectory(UnitTests/Experimental/UtilityTests)
add_subdirectory(UnitTests/Nautilus)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

