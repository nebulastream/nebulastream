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

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# Out-of-process UDFs: the PythonProcess sink ends the pipeline in front of a UDF operator and the PythonProcess source, which owns
# the process, starts the pipeline behind it. The source generates the code of the process.
add_library(nes-python-process-channel STATIC PythonProcessChannel.cpp)
target_link_libraries(nes-python-process-channel PUBLIC nes-memory)
target_include_directories(nes-python-process-channel PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_library(python_process_source_plugin_library STATIC PythonProcessSource.cpp PythonBatchWrapper.cpp)
target_link_libraries(python_process_source_plugin_library PRIVATE nes-sources nes-nautilus nes-udf PUBLIC nes-python-process-channel)
link_plugin_library(nes-sources python_process_source_plugin_library)
add_registry_entry(Source PythonProcess)
add_registry_entry(SourceConfigSchema PythonProcess)
add_registry_entry(SourceConfig PythonProcess)

add_library(python_process_sink_plugin_library STATIC PythonProcessSink.cpp)
target_link_libraries(python_process_sink_plugin_library PRIVATE nes-sinks PUBLIC nes-python-process-channel)
link_plugin_library(nes-sinks python_process_sink_plugin_library)
add_registry_entry(Sink PythonProcess)
add_registry_entry(SinkConfigSchema PythonProcess)
add_registry_entry(SinkConfig PythonProcess)
