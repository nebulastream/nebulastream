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

add_executable(runtime-sqrt-expression-test "SqrtExpressionTest.cpp")
target_link_libraries(runtime-sqrt-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-sqrt-expression-test COMMAND runtime-sqrt-expression-test)

add_executable(runtime-cbrt-expression-test "CbrtExpressionTest.cpp")
target_link_libraries(runtime-cbrt-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-cbrt-expression-test COMMAND runtime-cbrt-expression-test)

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

add_executable(runtime-cot-expression-test "CotExpressionTest.cpp")
target_link_libraries(runtime-cot-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-cot-expression-test COMMAND runtime-cot-expression-test)

add_executable(runtime-asin-expression-test "AsinExpressionTest.cpp")
target_link_libraries(runtime-asin-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-asin-expression-test COMMAND runtime-asin-expression-test)

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

add_executable(runtime-factorial-expression-test "FactorialExpressionTest.cpp")
target_link_libraries(runtime-factorial-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-factorial-expression-test COMMAND runtime-factorial-expression-test)

add_executable(runtime-lgamma-expression-test "LGammaExpressionTest.cpp")
target_link_libraries(runtime-lgamma-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-lgamma-expression-test COMMAND runtime-lgamma-expression-test)

add_executable(runtime-gamma-expression-test "GammaExpressionTest.cpp")
target_link_libraries(runtime-gamma-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-gamma-expression-test COMMAND runtime-gamma-expression-test)

add_executable(runtime-log10-expression-test "Log10ExpressionTest.cpp")
target_link_libraries(runtime-log10-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-log10-expression-test COMMAND runtime-log10-expression-test)

add_executable(runtime-power-expression-test "PowerExpressionTest.cpp")
target_link_libraries(runtime-power-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-power-expression-test COMMAND runtime-power-expression-test)

add_executable(runtime-log2-expression-test "Log2ExpressionTest.cpp")
target_link_libraries(runtime-log2-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-log2-expression-test COMMAND runtime-log2-expression-test)

add_executable(runtime-ln-expression-test "LnExpressionTest.cpp")
target_link_libraries(runtime-ln-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-ln-expression-test COMMAND runtime-ln-expression-test)

add_executable(runtime-bitcounter-expression-test "BitcounterExpressionTest.cpp")
target_link_libraries(runtime-bitcounter-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-bitcounter-expression-test COMMAND runtime-bitcounter-expression-test)

add_executable(runtime-tan-expression-test "TanExpressionTest.cpp")
target_link_libraries(runtime-tan-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-tan-expression-test COMMAND runtime-tan-expression-test)

add_executable(runtime-floor-expression-test "FloorExpressionTest.cpp")
target_link_libraries(runtime-floor-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-floor-expression-test COMMAND runtime-floor-expression-test)

add_executable(runtime-ceil-expression-test "CeilExpressionTest.cpp")
target_link_libraries(runtime-ceil-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-ceil-expression-test COMMAND runtime-ceil-expression-test)

add_executable(runtime-random-expression-test "RandomExpressionTest.cpp")
target_link_libraries(runtime-random-expression-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-random-expression-test COMMAND runtime-random-expression-test)