# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
add_plugin_as_library(TCP Sink nes-sinks-registry tcp_sink_plugin_library TCPSink.cpp)
add_plugin_as_library(TCP SinkValidation nes-sinks-registry tcp_sink_validation_plugin_library TCPSink.cpp)

# Boost.Asio is already in the vcpkg manifest (boost-asio); no new dependency.
find_package(boost_asio CONFIG REQUIRED)

foreach (_lib tcp_sink_plugin_library tcp_sink_validation_plugin_library)
    target_include_directories(${_lib} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)
    target_link_libraries(${_lib} PRIVATE Boost::asio)
endforeach ()
