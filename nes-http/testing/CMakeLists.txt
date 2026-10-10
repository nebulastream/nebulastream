# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A server to call in tests and demos. Built whenever tests are enabled, because the end-to-end
# test for the asynchronous framework is what it exists for -- it is the only way to run that
# framework against real sockets without a model server.
add_library(nes-http-testing STATIC DummyHttpServer.cpp)
target_include_directories(nes-http-testing PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(nes-http-testing PRIVATE nes-common)

# Runs the same server standalone, for trying the demo operator out by hand.
add_executable(dummy-http-server dummy-http-server.cpp)
target_link_libraries(dummy-http-server PRIVATE nes-http-testing nes-common)
