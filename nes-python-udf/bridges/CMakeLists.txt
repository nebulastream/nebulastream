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

add_subdirectory(cpython)

# Announces the supported executions to the frontend; InProcess only when its library is built.
add_library(nes-python-udf-bridges STATIC PythonUdfBridges.cpp)
target_include_directories(nes-python-udf-bridges PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(nes-python-udf-bridges PRIVATE nes-udf)
link_plugin_library(nes-udf nes-python-udf-bridges)
add_registry_entry(UdfBridge Python KEY PythonInline)
if (TARGET nes-cpython-udf-bridge)
    add_registry_entry(UdfBridge Python KEY PythonInProcess)
endif ()
