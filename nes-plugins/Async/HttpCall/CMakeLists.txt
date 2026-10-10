# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

# Its own library, like every out-of-module plugin here: the registry entry is compiled into
# nes-async's generated glue translation unit, so everything linking nes-async has to be able to
# resolve this executor's symbols, while nes-async itself must not depend on any operator.
add_library(http_call_async_plugin STATIC HttpCallExecutor.cpp)
target_include_directories(http_call_async_plugin PUBLIC include)
target_link_libraries(http_call_async_plugin PRIVATE nes-async nes-common)

link_plugin_library(nes-async http_call_async_plugin)
add_registry_entry(AsyncExecutor HttpCall)

# The glue translation unit includes Async/HttpCallExecutor.hpp by that path and does not inherit
# this plugin's include directories. Unlike the semantic executor, this header needs no pimpl: it
# exposes only nes-async and nes-http types, which the glue already sees through the registry owner.
nes_async_add_glue_include_dir(AsyncExecutor ${CMAKE_CURRENT_SOURCE_DIR}/include)
