# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

add_executable(value-test "UnitTests/Execution/ValueTest.cpp")
target_link_libraries(value-test ${GTEST_LIBRARIES} nes-execution)
add_test(NAME value-test COMMAND value-test)

#add_executable(interpreter-test "UnitTests/Execution/InterpreterTest.cpp")
#target_link_libraries(interpreter-test ${GTEST_LIBRARIES} nes-execution)
#add_test(NAME interpreter-test COMMAND interpreter-test)


set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

