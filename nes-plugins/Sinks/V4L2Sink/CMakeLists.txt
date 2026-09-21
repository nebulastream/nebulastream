# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

add_library(v4l2_sink_plugin_library STATIC V4L2Sink.cpp)
target_link_libraries(v4l2_sink_plugin_library PRIVATE nes-sinks nes-nautilus)
target_include_directories(v4l2_sink_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-sinks v4l2_sink_plugin_library)
add_registry_entry(Sink V4L2)
add_registry_entry(SinkValidation V4L2)

if(NES_ENABLES_TESTS)
    add_nes_unit_test(V4L2SinkTest V4L2SinkTest.cpp)
    target_link_libraries(V4L2SinkTest v4l2_sink_plugin_library nes-sinks nes-nautilus nes-test-util)
endif()

