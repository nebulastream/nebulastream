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

# activate optional plugins and add the path "THE/PATH" to the build (adding all dependencies of the optional plugin)
activate_optional_plugin("Sources/TCPSource" ON)
activate_optional_plugin("Sources/MQTTSource" ON)
activate_optional_plugin("Sinks/MQTTSink" ON)
activate_optional_plugin("ImageManip" ON)
activate_optional_plugin("Sources/GeneratorSource" ON)

if (NES_ENABLES_TESTS)
  # ChecksumSink depends on Checksum.cpp from systest which is only added when tests are enabled.
  activate_optional_plugin("Sinks/ChecksumSink" ON)
endif()
