# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# Registered with nes-sinks rather than built into nes-async, so that nes-async does not
# have to depend on the sink library it plugs into.
add_library(handoff_sink_plugin STATIC HandoffSink.cpp)
target_link_libraries(handoff_sink_plugin PRIVATE nes-sinks nes-async)
target_include_directories(handoff_sink_plugin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

link_plugin_library(nes-sinks handoff_sink_plugin)
add_registry_entry(Sink Handoff)
add_registry_entry(SinkValidation Handoff)

# See nes_async_add_glue_include_dir in the parent CMakeLists for why this is deferred.
# EVAL CODE expands the arguments now; a deferred call would otherwise evaluate them at the end of
# configuration, where neither the loop variable nor this directory's variables still exist.
foreach (registry IN ITEMS Sink SinkValidation)
    cmake_language(EVAL CODE
            "cmake_language(DEFER DIRECTORY \"${CMAKE_SOURCE_DIR}\"
                CALL nes_async_add_glue_include_dir ${registry} \"${CMAKE_CURRENT_SOURCE_DIR}/../include\")")
endforeach ()
