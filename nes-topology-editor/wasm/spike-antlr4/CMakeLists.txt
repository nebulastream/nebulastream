# Build ANTLR4 C++ runtime from source (vcpkg not available for WASM target)
include(FetchContent)
FetchContent_Declare(
    antlr4-runtime
    GIT_REPOSITORY https://github.com/antlr/antlr4.git
    GIT_TAG        4.13.2
    GIT_SHALLOW    TRUE
    SOURCE_SUBDIR  runtime/Cpp
)

# ANTLR4 runtime build options
set(ANTLR4_INSTALL OFF CACHE BOOL "" FORCE)
set(ANTLR_BUILD_CPP_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_DEMO OFF CACHE BOOL "" FORCE)
set(ANTLR_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ANTLR_BUILD_STATIC ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(antlr4-runtime)

# ANTLR4 runtime uses exceptions -- ensure native WASM exception handling
# is used consistently for both compile and link phases
if(TARGET antlr4_static)
    target_compile_options(antlr4_static PRIVATE
        -fwasm-exceptions
        -Wno-deprecated-declarations
        -Wno-deprecated-copy
        -Wno-unused-parameter
        -Wno-logical-op-parentheses
        -w
    )
endif()

# Collect generated parser sources
file(GLOB ANTLR4_GENERATED_SRCS "generated/*.cpp")

add_executable(spike-antlr4
    main.cpp
    ${ANTLR4_GENERATED_SRCS}
)

target_include_directories(spike-antlr4 PRIVATE
    generated/       # Generated parser/lexer headers
    include/         # Stub headers (DisableWarningsPragma.hpp)
)

target_link_libraries(spike-antlr4 PRIVATE antlr4_static)

# Suppress warnings from generated ANTLR4 code; enable WASM exceptions
target_compile_options(spike-antlr4 PRIVATE
    -fwasm-exceptions
    -Wno-deprecated-declarations
    -Wno-deprecated-copy
    -Wno-unused-parameter
    -Wno-logical-op-parentheses
)

target_link_options(spike-antlr4 PRIVATE -lembind)
set_target_properties(spike-antlr4 PROPERTIES
    SUFFIX ".cjs"
    LINK_FLAGS "-sMODULARIZE=1 -sEXPORT_ES6=0 -sENVIRONMENT=node -fwasm-exceptions -sALLOW_MEMORY_GROWTH=1"
)
