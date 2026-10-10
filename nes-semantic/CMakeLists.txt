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
        src/ChatCompletionProtocol.cpp
        src/ResponseParsing.cpp
        src/SemanticMapCodec.cpp
        src/HttpSemanticBackend.cpp
        src/MockSemanticBackend.cpp
        src/SemanticBackendFactory.cpp
        src/SemanticAsyncWiring.cpp
        src/SemanticLatencyReport.cpp)
target_include_directories(nes-semantic PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
# nlohmann is public because ResponseParsing.hpp, the envelope decoding shared by all semantic
# operator codecs, returns nlohmann::json.
#
# nes-http is private, and HttpSemanticBackend.hpp forward declares HttpBackend to keep it that
# way: this module contributes the LLM protocol, and the four modules that link nes-semantic
# publicly have no business seeing a transport.
if (NOT TARGET nes-http)
    # A name that is not a target is not a configure error -- CMake would quietly turn it into
    # -lnes-http and fail at link time instead.
    message(FATAL_ERROR "nes-semantic needs the nes-http module, which was not configured.")
endif ()
target_link_libraries(nes-semantic
        PUBLIC nes-data-types nes-schema nlohmann_json::nlohmann_json
        PRIVATE nes-common nes-http reflectcpp::reflectcpp)

# The executor that lets SEM_MAP run on the asynchronous framework. It is a plugin library of its
# own, so nes-semantic does not depend on nes-async.
add_subdirectory(async)

add_tests_if_enabled(tests)
