find_package(LLVM 19.1.7 EXACT REQUIRED CONFIG)

add_library(nes-passes SHARED FunctionInliningPass.cpp InliningUtils.cpp)

target_include_directories(nes-passes PRIVATE ${LLVM_INCLUDE_DIRS})
target_compile_definitions(nes-passes PRIVATE ${LLVM_DEFINITIONS})
set_target_properties(nes-passes PROPERTIES
        COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-unused-variable;-Wno-extra-semi;-Wno-deprecated-copy-with-dtor"
        LINK_OPTIONS ""
) #override global compile and link flags for the pass


target_include_directories(nes-passes PUBLIC
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-passes
        "$<$<PLATFORM_ID:Darwin>:-undefined dynamic_lookup>")

set_target_properties(nes-passes PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)


