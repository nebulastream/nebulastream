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

# The HTTP transport and nothing above it: a request goes in, a response comes out. Every protocol
# spoken over it -- chat completions, a JSON API, a plain text service -- belongs to the module of
# the operator that speaks it.
#
# The invariant that keeps that line visible: nes-http must never need nlohmann_json. A transport
# has no reason to parse a body; if it does, protocol knowledge has leaked down here.
#
# Deliberately a module of its own rather than a part of nes-async: nothing about an HTTP request is
# asynchronous, and a synchronous operator that calls a service needs this transport just as much.
# Living under the asynchronous framework, it would make every such operator depend on that
# framework. nes-async is a consumer of this module, not its owner. It also keeps nes-nautilus and
# nes-memory, which nes-async needs and an HTTP client does not, off this module's include path.
add_library(nes-http
        src/CurlHttpBackend.cpp
        src/HttpEventLoop.cpp
        src/HttpLatencyStats.cpp)
target_include_directories(nes-http
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
# CURL::libcurl is private because the easy handle is kept as a void*, so no consumer of these
# headers sees libcurl. It still reaches every executable's link line as a transitive LINK_ONLY
# dependency.
target_link_libraries(nes-http PRIVATE nes-common CURL::libcurl)

# The dummy server and its standalone binary. Only useful with tests, and the end-to-end proof for
# the asynchronous framework is what it exists for.
add_tests_if_enabled(testing)
add_tests_if_enabled(tests)
