# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(CURL REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

add_library(nes-semantic
        src/SemanticModelCatalog.cpp
        src/CurlLlmClient.cpp
        src/ResponseParsing.cpp)
target_include_directories(nes-semantic PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(nes-semantic
        PUBLIC nes-data-types nes-schema
        PRIVATE nes-common CURL::libcurl nlohmann_json::nlohmann_json)

add_tests_if_enabled(tests)
