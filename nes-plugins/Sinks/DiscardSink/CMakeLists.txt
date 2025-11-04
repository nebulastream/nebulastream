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
add_plugin_as_library(Discard Sink nes-sinks-registry discard_sink_plugin DiscardSink.cpp)
add_plugin_as_library(Discard SinkValidation nes-sinks-registry discard_sink_validation_plugin DiscardSink.cpp)

target_include_directories(discard_sink_plugin
        PUBLIC include
        PRIVATE .
)
target_include_directories(discard_sink_validation_plugin
        PUBLIC include
        PRIVATE .
)
