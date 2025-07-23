add_executable(example main.cpp lib.cpp
        FunctionRegistry.cpp
        FunctionRegistry.h)

find_package(LLVM 20.1.7 CONFIG)

llvm_map_components_to_libnames(llvm_libs
        core
        irreader
        passes
        analysis
        transformutils
        scalaropts
)
target_include_directories(example PRIVATE ${LLVM_INCLUDE_DIRS})
target_compile_definitions(example PRIVATE ${LLVM_DEFINITIONS})

target_link_libraries(example
        "$<$<PLATFORM_ID:Darwin>:-undefined dynamic_lookup>")

set_target_properties(example PROPERTIES
        COMPILE_OPTIONS "-Wno-unused-parameter"
        LINK_OPTIONS ""
) #override global compile and link flags for the pass

target_link_libraries(example ${llvm_libs})