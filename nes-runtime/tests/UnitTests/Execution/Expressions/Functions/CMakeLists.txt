# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(runtime-mod-expression-test "ModExpressionTest.cpp")
target_link_libraries(runtime-mod-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-mod-expression-test COMMAND runtime-mod-expression-test)

add_executable(runtime-abs-expression-test "AbsExpressionTest.cpp")
target_link_libraries(runtime-abs-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-abs-expression-test COMMAND runtime-abs-expression-test)

add_executable(runtime-cos-expression-test "CosExpressionTest.cpp")
target_link_libraries(runtime-cos-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-cos-expression-test COMMAND runtime-cos-expression-test)

add_executable(runtime-sin-expression-test "SinExpressionTest.cpp")
target_link_libraries(runtime-sin-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-sin-expression-test COMMAND runtime-sin-expression-test)

add_executable(runtime-atan2-expression-test "Atan2ExpressionTest.cpp")
target_link_libraries(runtime-atan2-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-atan2-expression-test COMMAND runtime-atan2-expression-test)

add_executable(runtime-acos-expression-test "AcosExpressionTest.cpp")
target_link_libraries(runtime-acos-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-acos-expression-test COMMAND runtime-acos-expression-test)
