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
add_plugin_as_library(DefaultDate ValueSerializer time_value_serializer_plugin src/DefaultTimeSerializers.cpp)
add_plugin(DefaultTime ValueSerializer src/DefaultTimeSerializers.cpp)
add_plugin(DefaultTimestamp ValueSerializer src/DefaultTimeSerializers.cpp)

target_include_directories(time_value_serializer_plugin
        PUBLIC include
        PRIVATE .
)
