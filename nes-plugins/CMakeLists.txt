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

# NOTE: Since these plugins are part of the main NebulaStream repo,
#       we apply the same care and test coverage requirements to these plugins that we apply to the rest of the system.

# add plugin directories to the build (adding all dependencies of each plugin)
add_plugin("Sources/TCPSource")
add_plugin("Sources/MQTTSource")
add_plugin("Sources/GeneratorSource")
add_plugin("Sinks/VoidSink")
add_plugin("Sinks/MQTTSink")
add_plugin("Sinks/KafkaSink")
add_plugin("InputFormatters/JSONInputFormatter")
add_plugin("OutputFormatters/JSONOutputFormatter")
add_plugin("Rules/RedundantUnionRemovalRule")
add_plugin("Functions/Conditional")
add_plugin("OutputFormatters/JSONOutputFormatter/Serializers")
add_plugin("InputFormatters/JSONInputFormatter/Deserializers")
add_plugin("DataTypes/Image")
add_plugin("DataTypes/Reading")
add_plugin("DataTypes/GeoSpatial")
add_plugin("DataTypes/Matrices")
add_plugin("DataTypes/Time")

# MEOS is a dependency
add_plugin("MEOS")
message(STATUS "Enable MEOS Plugin")

# Detect the platform
if (APPLE)
  message(STATUS "Building on macOS")
  # Set the include and library directories for Homebrew (macOS)
  set(MEOS_INCLUDE_DIR "/opt/homebrew/include" CACHE PATH "Path to MEOS include directory")
  set(MEOS_PLUGIN_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include" CACHE PATH "Path to MEOS plugin include directory")
  include_directories(SYSTEM ${MEOS_INCLUDE_DIR} ${MEOS_PLUGIN_INCLUDE_DIR})
  include_directories(SYSTEM ${MEOS_INCLUDE_DIR} ${MEOS_PLUGIN_INCLUDE_DIR})
  link_directories(${MEOS_LIBRARY_DIR} )

else()
  message(STATUS "Building on Linux")

  # Default behavior for Linux
  find_package(meos REQUIRED)
  if(meos_FOUND)
    message(STATUS "MEOS found: include=${meos_INCLUDE_DIR}, library=${meos}")
    include_directories(SYSTEM ${meos_INCLUDE_DIR})
  else()
    message(FATAL_ERROR "MEOS library not found")
  endif()
endif()

if (NES_ENABLES_TESTS)
  # ChecksumSink depends on Checksum.cpp from systest which is only added when tests are enabled.
  add_plugin("Sinks/ChecksumSink")
endif()
