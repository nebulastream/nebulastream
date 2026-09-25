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

add_library(grpc_source_plugin_library STATIC GrpcSource.cpp)
target_link_libraries(grpc_source_plugin_library PRIVATE nes-sources)
# PUBLIC: GrpcSource.hpp includes grpcpp headers and the generated service, so the registration glue (which
# includes the plugin header) needs nes-grpc's include directories propagated.
target_link_libraries(grpc_source_plugin_library PUBLIC nes-grpc)
target_include_directories(grpc_source_plugin_library PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

link_plugin_library(nes-sources grpc_source_plugin_library)
add_registry_entry(Source Grpc)
add_registry_entry(SourceValidation Grpc)
