# ANTLR4 C++ runtime via vcpkg
find_package(antlr4-runtime CONFIG REQUIRED)

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
