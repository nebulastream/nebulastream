# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-executable-test-utils TestUtils/TestTaskQueue.cpp)
target_include_directories(nes-executable-test-utils PUBLIC TestUtils)
target_link_libraries(nes-executable-test-utils PUBLIC nes-executable nes-memory nes-runtime nes-configurations)

add_executable(backpressure-channel-test BackpressureChannelTest.cpp)
target_include_directories(backpressure-channel-test PRIVATE ${CMAKE_SOURCE_DIR}/nes-common/tests/Util/include)
target_link_libraries(backpressure-channel-test PRIVATE nes-executable nes-common GTest::gtest GTest::gtest_main)

