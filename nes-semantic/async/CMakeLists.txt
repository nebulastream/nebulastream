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

# Its own library, and not part of nes-semantic, for the reason every out-of-module plugin here has
# one (see nes-plugins/OutputFormatters/JSONOutputFormatter): the registry entry is compiled into
# nes-async's generated glue translation unit, so everything linking nes-async has to be able to
# resolve this executor's symbols — while nes-async itself must not depend on any operator that
# uses it. link_plugin_library adds exactly that one edge, and keeps nes-semantic free of any
# dependency on the framework.
# SEM_FILTER's executor lives here too: both share SemanticExecutorCore and the wire payload.
add_library(semantic_map_async_plugin STATIC SemanticExecutorCore.cpp SemanticMapExecutor.cpp SemanticFilterExecutor.cpp)
target_include_directories(semantic_map_async_plugin PUBLIC include PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(semantic_map_async_plugin PRIVATE nes-semantic nes-async nes-common)

link_plugin_library(nes-async semantic_map_async_plugin)
add_registry_entry(AsyncExecutor SemanticMap)
add_registry_entry(AsyncExecutor SemanticFilter)

# The generated glue translation unit includes Async/<Name>Executor.hpp by that path and does
# not inherit this plugin's include directories. Only this one is needed: the header deliberately
# exposes no semantic type.
nes_async_add_glue_include_dir(AsyncExecutor ${CMAKE_CURRENT_SOURCE_DIR}/include)
