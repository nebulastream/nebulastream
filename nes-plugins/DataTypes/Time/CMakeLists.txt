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
add_plugin_as_library(Date DataType date_data_type_plugin DateDataType.cpp)
add_plugin_as_library(Time DataType time_data_type_plugin TimeDataType.cpp)
add_plugin_as_library(Timestamp DataType timestamp_data_type_plugin TimestampDataType.cpp)

target_include_directories(date_data_type_plugin PRIVATE .)
target_include_directories(time_data_type_plugin PRIVATE .)
target_include_directories(timestamp_data_type_plugin PRIVATE .)