# WASM build of nes-validator using Emscripten + Embind
# Compiles NES source files directly (cannot use native NES CMake targets with Emscripten toolchain)

set(NES_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../..")

# ============================================================================
# ANTLR4 C++ runtime via vcpkg (migrated from FetchContent per locked decision)
# ============================================================================
find_package(antlr4-runtime CONFIG QUIET)

if(antlr4-runtime_FOUND AND TARGET antlr4_static)
    message(STATUS "ANTLR4 C++ runtime found via vcpkg")
    # Apply -fwasm-exceptions to antlr4_static from vcpkg
    target_compile_options(antlr4_static PRIVATE -fwasm-exceptions -w)
else()
    message(WARNING "ANTLR4 vcpkg port not found for wasm32-emscripten -- falling back to FetchContent")
    include(FetchContent)
    FetchContent_Declare(
        antlr4-runtime
        GIT_REPOSITORY https://github.com/antlr/antlr4.git
        GIT_TAG        4.13.2
        GIT_SHALLOW    TRUE
        SOURCE_SUBDIR  runtime/Cpp
    )
    set(ANTLR4_INSTALL OFF CACHE BOOL "" FORCE)
    set(ANTLR_BUILD_CPP_TESTS OFF CACHE BOOL "" FORCE)
    set(WITH_DEMO OFF CACHE BOOL "" FORCE)
    set(ANTLR_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ANTLR_BUILD_STATIC ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(antlr4-runtime)

    if(TARGET antlr4_static)
        target_compile_options(antlr4_static PRIVATE -fwasm-exceptions -w)
    endif()
endif()

# ============================================================================
# yaml-cpp via vcpkg
# ============================================================================
find_package(yaml-cpp CONFIG REQUIRED)

# ============================================================================
# spdlog + fmt via vcpkg (needed by NES Logger/ErrorHandling)
# ============================================================================
find_package(spdlog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

# ============================================================================
# libuuid via vcpkg (needed by UUID.cpp for uuid_generate/uuid_parse)
# ============================================================================
find_package(unofficial-libuuid CONFIG QUIET)
if(NOT unofficial-libuuid_FOUND)
    # Fallback: find via pkg-config
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(LIBUUID REQUIRED uuid)
endif()

# ============================================================================
# ANTLR4 generated parser sources (from Phase 5 spike)
# ============================================================================
file(GLOB ANTLR4_GENERATED_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/../spike-antlr4/generated/*.cpp")

# ============================================================================
# NES source files needed for the validation pipeline
# ============================================================================

# nes-validator own sources
set(VALIDATOR_SRCS
    ${NES_ROOT}/nes-validator/src/TopologyValidator.cpp
    ${NES_ROOT}/nes-validator/src/YamlBinding.cpp
    ${NES_ROOT}/nes-validator/src/ConfigMetadata.cpp
    ${NES_ROOT}/nes-validator/src/DumpPipeline.cpp
)

# nes-common sources
set(NES_COMMON_SRCS
    ${NES_ROOT}/nes-common/src/ErrorHandling.cpp
    ${NES_ROOT}/nes-common/src/Util/Strings.cpp
    ${NES_ROOT}/nes-common/src/Util/Logger/LogLevel.cpp
    ${NES_ROOT}/nes-common/src/Util/Logger/NesLogger.cpp
    ${NES_ROOT}/nes-common/src/Util/Common.cpp
    ${NES_ROOT}/nes-common/src/Util/UUID.cpp
    ${NES_ROOT}/nes-common/src/QueryId.cpp
)

# nes-data-types sources
set(NES_DATA_TYPES_SRCS
    ${NES_ROOT}/nes-data-types/src/DataTypes/DataType.cpp
    ${NES_ROOT}/nes-data-types/src/DataTypes/DataTypeProvider.cpp
    ${NES_ROOT}/nes-data-types/src/DataTypes/Schema.cpp
    ${NES_ROOT}/nes-data-types/src/DataTypes/TimeUnit.cpp
)

# nes-sql-parser sources
# NOTE: StatementBinder.cpp is excluded -- it has wasm32 portability issues (size_t vs uint64_t
# in variant) and is not needed by the validation path (we use createLogicalQueryPlanFromSQLString)
set(NES_SQL_PARSER_SRCS
    ${NES_ROOT}/nes-sql-parser/src/AntlrSQLQueryParser.cpp
    ${NES_ROOT}/nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp
    ${NES_ROOT}/nes-sql-parser/src/AntlrSQLHelper.cpp
    ${NES_ROOT}/nes-sql-parser/src/CommonParserFunctions.cpp
)

# nes-logical-operators sources (exclude Serialization which needs protobuf)
file(GLOB_RECURSE NES_LOGICAL_OPS_ALL "${NES_ROOT}/nes-logical-operators/src/*.cpp")
set(NES_LOGICAL_OPS_SRCS "")
foreach(f ${NES_LOGICAL_OPS_ALL})
    # Exclude serialization files (depend on protobuf)
    if(NOT f MATCHES "Serialization/")
        list(APPEND NES_LOGICAL_OPS_SRCS ${f})
    endif()
endforeach()

# nes-sources sources (excluding Source.cpp which needs Runtime/TupleBuffer)
set(NES_SOURCES_SRCS
    ${NES_ROOT}/nes-sources/src/LogicalSource.cpp
    ${NES_ROOT}/nes-sources/src/SourceCatalog.cpp
    ${NES_ROOT}/nes-sources/src/SourceDescriptor.cpp
    ${NES_ROOT}/nes-sources/src/SourceValidationProvider.cpp
)

# nes-sinks sources (excluding Sink.cpp which needs BackpressureChannel/Runtime)
set(NES_SINKS_SRCS
    ${NES_ROOT}/nes-sinks/src/SinkCatalog.cpp
    ${NES_ROOT}/nes-sinks/src/SinkDescriptor.cpp
)

# Source validation files (real validation, replaces stubs)
set(NES_SOURCE_VALIDATION_SRCS
    ${NES_ROOT}/nes-sources/src/FileSourceValidation.cpp
    ${NES_ROOT}/nes-plugins/Sources/TCPSource/TCPSourceValidation.cpp
    ${NES_ROOT}/nes-plugins/Sources/NetworkSource/NetworkSourceValidation.cpp
    ${NES_ROOT}/nes-plugins/Sources/GeneratorSource/GeneratorSourceValidation.cpp
    ${NES_ROOT}/nes-plugins/Sources/GeneratorSource/GeneratorFields.cpp
    ${NES_ROOT}/nes-plugins/Sources/GeneratorSource/FixedGeneratorRate.cpp
    ${NES_ROOT}/nes-plugins/Sources/GeneratorSource/SinusGeneratorRate.cpp
)

# Sink validation files (new — previously sinks had no validation in WASM)
set(NES_SINK_VALIDATION_SRCS
    ${NES_ROOT}/nes-sinks/src/FileSinkValidation.cpp
    ${NES_ROOT}/nes-sinks/src/PrintSinkValidation.cpp
    ${NES_ROOT}/nes-plugins/Sinks/NetworkSink/NetworkSinkValidation.cpp
    ${NES_ROOT}/nes-plugins/Sinks/ChecksumSink/ChecksumSinkValidation.cpp
    ${NES_ROOT}/nes-plugins/Sinks/VoidSink/VoidSinkValidation.cpp
)

# nes-configurations sources (Descriptor.hpp patched in stubs for wasm32 ConfigType compatibility)
# Descriptor.cpp excluded -- needs protobuf. Stub implementations provided in descriptor_stubs.cpp.
set(NES_CONFIGURATIONS_SRCS
    ${NES_ROOT}/nes-configurations/src/Configurations/Descriptor.cpp
    ${NES_ROOT}/nes-configurations/src/Configurations/BaseConfiguration.cpp
    ${NES_ROOT}/nes-configurations/src/Configurations/BaseOption.cpp
    ${NES_ROOT}/nes-configurations/src/Configurations/Validation/EndpointValidation.cpp
)

# nes-input-formatters: not needed in WASM (StatementBinder excluded, formatters stubbed)
set(NES_INPUT_FORMATTERS_SRCS "")

# nes-query-optimizer: full optimizer pipeline (same as nes-cli dump)
set(NES_QUERY_OPTIMIZER_SRCS
    # Traits
    ${NES_ROOT}/nes-query-optimizer/src/Traits/TraitSet.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Traits/Trait.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Traits/OutputOriginIdsTrait.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Traits/PlacementTrait.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Traits/ImplementationTypeTrait.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Traits/MemoryLayoutTypeTrait.cpp
    # Phases
    ${NES_ROOT}/nes-query-optimizer/src/Phases/SemanticAnalyzer.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Phases/QueryOptimizer.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Phases/QueryDecomposition.cpp
    # Semantic rules
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/InlineSinkBindingRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/SinkBindingRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/InlineSourceBindingRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/SourceInferenceRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/LogicalSourceExpansionRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/TypeInferenceRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Semantic/OriginIdInferenceRule.cpp
    # Static optimization rules
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Static/DecideJoinTypesRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Static/DecideMemoryLayoutRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Static/RedundantProjectionRemovalRule.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/Static/RedundantUnionRemovalRule.cpp
    # Placement
    ${NES_ROOT}/nes-query-optimizer/src/Rules/BottomUpPlacement.cpp
    ${NES_ROOT}/nes-query-optimizer/src/Rules/OperatorPlacement.cpp
)

# nes-distributed sources (topology, worker catalog, distributed query types)
set(NES_DISTRIBUTED_SRCS
    ${NES_ROOT}/nes-distributed/src/NetworkTopology.cpp
    ${NES_ROOT}/nes-distributed/src/WorkerCatalog.cpp
    ${NES_ROOT}/nes-distributed/src/DistributedQuery.cpp
    ${NES_ROOT}/nes-distributed/src/DistributedQueryIdGenerator.cpp
)

# nes-logical-operators serialization (only WindowTypeReflection, needed by window operators)
set(NES_LOGICAL_OPS_SERIALIZATION_SRCS
    ${NES_ROOT}/nes-logical-operators/src/Serialization/WindowTypeReflection.cpp
)

# ============================================================================
# Build the WASM executable
# ============================================================================
add_executable(nes-validator-wasm
    main.cpp
    ${VALIDATOR_SRCS}
    ${NES_COMMON_SRCS}
    ${NES_DATA_TYPES_SRCS}
    ${NES_SQL_PARSER_SRCS}
    ${NES_LOGICAL_OPS_SRCS}
    ${NES_SOURCES_SRCS}
    ${NES_SINKS_SRCS}
    ${NES_SOURCE_VALIDATION_SRCS}
    ${NES_SINK_VALIDATION_SRCS}
    ${NES_CONFIGURATIONS_SRCS}
    ${NES_INPUT_FORMATTERS_SRCS}
    ${NES_QUERY_OPTIMIZER_SRCS}
    ${NES_DISTRIBUTED_SRCS}
    ${NES_LOGICAL_OPS_SERIALIZATION_SRCS}
    ${ANTLR4_GENERATED_SRCS}
)

# ============================================================================
# Include directories
# ============================================================================

# CRITICAL: Stubs BEFORE all other includes to override system/vcpkg cpptrace, folly, and rfl
target_include_directories(nes-validator-wasm BEFORE PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/stubs
    ${NES_ROOT}/nes-validator/stubs
)

target_include_directories(nes-validator-wasm PRIVATE
    # nes-validator headers
    ${NES_ROOT}/nes-validator/include
    ${NES_ROOT}/nes-validator/src

    # NES module public headers
    ${NES_ROOT}/nes-common/include
    ${NES_ROOT}/nes-data-types/include
    ${NES_ROOT}/nes-sql-parser/include
    ${NES_ROOT}/nes-sql-parser/private
    ${NES_ROOT}/nes-logical-operators/include
    ${NES_ROOT}/nes-sources/include
    ${NES_ROOT}/nes-sinks/include
    ${NES_ROOT}/nes-configurations/include
    ${NES_ROOT}/nes-input-formatters/include
    ${NES_ROOT}/nes-query-optimizer/include
    ${NES_ROOT}/nes-query-optimizer/interface
    ${NES_ROOT}/nes-distributed/include
    ${NES_ROOT}/nes-single-node-worker/interface
    ${NES_ROOT}/nes-runtime/interface
    ${NES_ROOT}/nes-network/interface
    ${NES_ROOT}/nes-data-types/registry/include
    ${NES_ROOT}/nes-logical-operators/registry/include
    ${NES_ROOT}/nes-query-optimizer/registry/include
    ${NES_ROOT}/nes-sources/registry/include
    ${NES_ROOT}/nes-sinks/registry/include
    ${NES_ROOT}/nes-input-formatters/registry/include

    # Plugin validation header directories
    ${NES_ROOT}/nes-plugins/Sources/TCPSource
    ${NES_ROOT}/nes-plugins/Sources/GeneratorSource
    ${NES_ROOT}/nes-plugins/Sources/NetworkSource
    ${NES_ROOT}/nes-plugins/Sinks/NetworkSink
    ${NES_ROOT}/nes-plugins/Sinks/ChecksumSink
    ${NES_ROOT}/nes-plugins/Sinks/VoidSink

    # Generated registrar files (copied from native build output)
    ${CMAKE_CURRENT_SOURCE_DIR}/generated

    # ANTLR4 generated parser/lexer headers (from Phase 5 spike)
    ${CMAKE_CURRENT_SOURCE_DIR}/../spike-antlr4/generated
    # ANTLR4 spike stubs (DisableWarningsPragma.hpp)
    ${CMAKE_CURRENT_SOURCE_DIR}/../spike-antlr4/include
)

# ============================================================================
# Link libraries
# ============================================================================
# HiGHS LP solver (used by BottomUpPlacement for operator placement ILP)
find_package(highs CONFIG REQUIRED)

# scope-guard (used by BottomUpPlacement for RAII cleanup of HiGHS solver)
find_package(scope-guard CONFIG REQUIRED)

target_link_libraries(nes-validator-wasm PRIVATE
    antlr4_static
    yaml-cpp::yaml-cpp
    spdlog::spdlog
    fmt::fmt
    highs::highs
    scope_guard
    $<$<TARGET_EXISTS:unofficial::UUID::uuid>:unofficial::UUID::uuid>
    $<$<NOT:$<TARGET_EXISTS:unofficial::UUID::uuid>>:${LIBUUID_LIBRARIES}>
)

target_link_options(nes-validator-wasm PRIVATE -lembind)

# ============================================================================
# Compiler flags: -fwasm-exceptions on ALL translation units + optimization
# ============================================================================
# NES_LOGLEVEL_NONE: minimize logging in WASM build (no spdlog runtime logging)
# SYSTEST_DATA_DIR: needed by GeneratorFields.cpp WordListField validator (paths won't exist in WASM,
# so validation correctly rejects WordList sources with "path does not exist" errors)
target_compile_definitions(nes-validator-wasm PRIVATE NES_LOGLEVEL_NONE SYSTEST_DATA_DIR="")

target_compile_options(nes-validator-wasm PRIVATE
    -fwasm-exceptions
    -O2
    -Wno-deprecated-declarations
    -Wno-deprecated-copy
    -Wno-unused-parameter
    -Wno-logical-op-parentheses
)

# ============================================================================
# Emscripten linker settings
# ============================================================================
set_target_properties(nes-validator-wasm PROPERTIES
    SUFFIX ".mjs"
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=1 -sENVIRONMENT=web,worker,node -sSINGLE_FILE=1 -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1 -O2 -sASSERTIONS=0"
)
