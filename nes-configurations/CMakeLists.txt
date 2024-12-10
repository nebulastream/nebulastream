# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
# Include dirs
add_subdirectory(src)
get_source(nes-configurations NES_CONFIGURATIONS_SOURCE_FILES)
include_directories(${PROJECT_SOURCE_DIR}/nes-runtime/include)
include_directories(${PROJECT_SOURCE_DIR}/nes-query-engine/include)

# Add Library
add_library(nes-configurations ${NES_CONFIGURATIONS_SOURCE_FILES}
        include/Configurations/ReadingVisitor.hpp
        include/Configurations/PrintingVisitor.hpp
        include/Configurations/ProtobufDeserializeVisitor.hpp
        include/Configurations/ProtobufConfigCache.hpp
        include/Configurations/ProtobufMessageTypeBuilderOptionVisitor.hpp
        include/Configurations/WritingVisitor.hpp
)
target_link_libraries(nes-configurations PUBLIC nes-common)
target_link_libraries(nes-configurations PRIVATE nes-query-engine)


target_include_directories(nes-configurations PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-configurations REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-configurations PUBLIC "-fPIC")
endif ()
add_tests_if_enabled(tests)
