# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)
include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

FetchContent_Declare(
        xjb
        GIT_REPOSITORY https://github.com/xjb714/xjb.git
        GIT_TAG        cfe766c898ae03c4b83662b485987b97a110ff6d
)
FetchContent_MakeAvailable(xjb)

if(TARGET xjb)
    target_compile_options(xjb PRIVATE -w)
endif()

add_plugin_as_library(XJBF32 OutputParser nes-output-formatters-registry xjb_output_parser_plugin XJBOutputParser.cpp)
add_plugin(XJBF64 OutputParser xjb_output_parser_plugin XJBOutputParser.cpp)
nautilus_inline(xjb_output_parser_plugin)

target_link_libraries(xjb_output_parser_plugin PRIVATE xjb)
target_include_directories(xjb_output_parser_plugin PUBLIC include PRIVATE . ${xjb_SOURCE_DIR}/src)
