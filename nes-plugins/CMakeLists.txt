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
add_plugin("InputFormatters/JSONInputFormatter")
add_plugin("OutputFormatters/JSONOutputFormatter")
add_plugin("Rules/RedundantUnionRemovalRule")
add_plugin("Functions/Conditional")

# The demonstration that the asynchronous operator framework is not about language models: three
# plugins -- an SQL function, the rule that marks it for asynchronous execution, and the executor --
# put an operator that waits on an HTTP service onto the framework without touching a single core
# module. Its end-to-end test is the only one the framework has that needs no model server.
add_plugin("Functions/HttpCall")
add_plugin("Rules/HttpCallAsyncRule")
add_plugin("Async/HttpCall")

if (NES_ENABLES_TESTS)
  # ChecksumSink depends on Checksum.cpp from systest which is only added when tests are enabled.
  add_plugin("Sinks/ChecksumSink")
endif()
