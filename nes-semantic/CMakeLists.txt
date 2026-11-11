# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(nlohmann_json CONFIG REQUIRED)
find_package(reflectcpp REQUIRED)

add_library(nes-semantic
        src/SemanticModelCatalog.cpp
        src/ResponseParsing.cpp
        src/SemanticPromptLayout.cpp
        src/SemanticMapCodec.cpp
        src/SemanticFilterCodec.cpp
        src/SemanticJoinCodec.cpp
        src/SemanticAggCodec.cpp
        src/SemanticKeyedCodec.cpp
        src/MockSemanticBackend.cpp
        src/SemanticCallPayload.cpp
        src/SemanticCallStats.cpp
        src/SemanticImage.cpp
        src/SemanticLabelAnswer.cpp)
target_include_directories(nes-semantic PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
# nlohmann is public because ResponseParsing.hpp, the envelope decoding shared by all semantic
# operator codecs, returns nlohmann::json.
target_link_libraries(nes-semantic
        PUBLIC nes-data-types nes-schema nlohmann_json::nlohmann_json
        PRIVATE nes-common reflectcpp::reflectcpp)

# SEM_CALL, the async function the semantic operators are translated into: its logical function is a
# plugin of nes-logical-operators, its implementation one of the Async source in nes-sources.
add_subdirectory(function)
add_subdirectory(async)

add_tests_if_enabled(tests)
