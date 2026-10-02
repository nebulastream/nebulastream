# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The asynchronous SEM_MAP executor lives here rather than in nes-async: the framework must not
# depend on any operator that uses it. nes-semantic is configured after nes-async — directories
# are added in alphabetical order — so the registry already exists at this point.
target_sources(nes-semantic PRIVATE SemanticMapExecutor.cpp)
target_include_directories(nes-semantic PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(nes-semantic PUBLIC nes-async)

add_registry_entry(AsyncExecutor SemanticMap)

# The generated glue translation unit includes Async/SemanticMapExecutor.hpp by that path and is
# compiled into the AsyncExecutor glue library, which does not inherit nes-semantic's includes.
# Only this one directory is needed: the header deliberately exposes no semantic type.
nes_async_add_glue_include_dir(AsyncExecutor ${CMAKE_CURRENT_SOURCE_DIR}/include)
