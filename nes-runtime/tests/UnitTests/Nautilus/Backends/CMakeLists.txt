
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(nautilus-expression-compilation-test "ExpressionCompilationTest.cpp")
target_link_libraries(nautilus-expression-compilation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-expression-compilation-test COMMAND nautilus-expression-compilation-test)

add_executable(nautilus-if-compilation-test "IfCompilationTest.cpp")
target_link_libraries(nautilus-if-compilation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-if-compilation-test COMMAND nautilus-if-compilation-test)

add_executable(nautilus-loop-compilation-test "LoopCompilationTest.cpp")
target_link_libraries(nautilus-loop-compilation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-loop-compilation-test COMMAND nautilus-loop-compilation-test)

add_executable(nautilus-memory-compilation-test "MemoryAccessCompilationTest.cpp")
target_link_libraries(nautilus-memory-compilation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-memory-compilation-test COMMAND nautilus-memory-compilation-test)

add_executable(nautilus-type-compilation-test "TypeCompilationTest.cpp")
target_link_libraries(nautilus-type-compilation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-type-compilation-test COMMAND nautilus-type-compilation-test)

add_executable(nautilus-function-compilation-test "FunctionCompilationTest.cpp")
target_link_libraries(nautilus-function-compilation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME nautilus-function-compilation-test COMMAND nautilus-function-compilation-test)