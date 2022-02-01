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

### Thread Naming Test ###
add_executable(thread-naming-tests "Util/ThreadNamingTest.cpp")
target_link_libraries(thread-naming-tests nes_common ${GTEST_LIBRARIES})
add_test(NAME thread-naming-tests COMMAND thread-naming-tests)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

