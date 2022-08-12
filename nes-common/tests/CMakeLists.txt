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

#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

add_library(nes-tests-util SHARED "util/NesBaseTest.cpp")
message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-tests-util PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/util/")
target_link_libraries(nes-tests-util PUBLIC ${GTEST_LIBRARIES} nes-common)

### Thread Naming Test ###
add_nes_unit_test(thread-naming-tests "UnitTests/Util/ThreadNamingTest.cpp")

### Non Blocking Monotonic Seq Queue ###
add_nes_unit_test(non-blocking-monotonic-seq-queue "UnitTests/Util/NonBlockingMonotonicSeqQueueTest.cpp")

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

