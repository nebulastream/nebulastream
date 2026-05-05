# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-validator STATIC
    src/TopologyValidator.cpp
    src/YamlBinding.cpp
    src/ConfigMetadata.cpp
    src/DumpPipeline.cpp
)

# Public API headers
target_include_directories(nes-validator PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/nebulastream/>
)

# Stub headers override system/vcpkg headers for folly and cpptrace
# during nes-validator compilation only. PRIVATE prevents stubs from
# leaking to consumers (e.g., nes-cli) that use the real libraries.
# The BEFORE keyword ensures stubs are found first on the include path.
target_include_directories(nes-validator BEFORE PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/stubs
)

# Link NES dependencies needed for validation pipeline.
# nes-sql-parser provides: ANTLR4 (via vcpkg), StatementBinder, query parsing
# nes-logical-operators provides: LogicalPlan, logical operator types
# nes-data-types provides: Schema, DataType system
# nes-configurations provides: NES configuration types
# Note: nes-sources and nes-sinks come transitively via nes-sql-parser (PUBLIC link).
# We do NOT link them directly to avoid adding more transitive deps than necessary.
target_link_libraries(nes-validator
    PUBLIC
        nes-sql-parser
        nes-logical-operators
        nes-data-types
        nes-configurations
        nes-sources-validation
        nes-sinks-validation
        nes-inference
    PRIVATE
        nes-query-optimizer
        nes-sources-validation-registry
        nes-sinks-validation-registry
)

# C++23 standard (matches NES project standard)
target_compile_features(nes-validator PUBLIC cxx_std_23)

add_tests_if_enabled(tests)
