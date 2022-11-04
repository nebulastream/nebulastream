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

add_executable(runtime-radians-expression-test "RadiansExpressionTest.cpp")
target_link_libraries(runtime-radians-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-radians-expression-test COMMAND runtime-radians-expression-test)

add_executable(runtime-pi-expression-test "PiExpressionTest.cpp")
target_link_libraries(runtime-pi-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-pi-expression-test COMMAND runtime-pi-expression-test)

add_executable(runtime-degrees-expression-test "DegreesExpressionTest.cpp")
target_link_libraries(runtime-degrees-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-degrees-expression-test COMMAND runtime-degrees-expression-test)

add_executable(runtime-sin-expression-test "SinExpressionTest.cpp")
target_link_libraries(runtime-sin-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-sin-expression-test COMMAND runtime-sin-expression-test)

add_executable(runtime-atan-expression-test "AtanExpressionTest.cpp")
target_link_libraries(runtime-atan-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-atan-expression-test COMMAND runtime-atan-expression-test)

add_executable(runtime-atan2-expression-test "Atan2ExpressionTest.cpp")
target_link_libraries(runtime-atan2-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-atan2-expression-test COMMAND runtime-atan2-expression-test)

add_executable(runtime-acos-expression-test "AcosExpressionTest.cpp")
target_link_libraries(runtime-acos-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-acos-expression-test COMMAND runtime-acos-expression-test)

add_executable(runtime-max-expression-test "MaxExpressionTest.cpp")
target_link_libraries(runtime-max-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-max-expression-test COMMAND runtime-max-expression-test)

add_executable(runtime-min-expression-test "MinExpressionTest.cpp")
target_link_libraries(runtime-min-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-min-expression-test COMMAND runtime-min-expression-test)
