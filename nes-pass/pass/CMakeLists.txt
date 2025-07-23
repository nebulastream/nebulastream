find_package(LLVM 20.1.7 CONFIG)

add_library(MinimalPass SHARED main.cpp)
llvm_map_components_to_libnames(llvm_libs
        support
        core
        irreader
        passes
        analysis
        transformutils
        scalaropts
)
target_include_directories(MinimalPass PRIVATE ${LLVM_INCLUDE_DIRS})
target_compile_definitions(MinimalPass PRIVATE ${LLVM_DEFINITIONS})
set_target_properties(MinimalPass PROPERTIES
        COMPILE_OPTIONS "-Wno-unused-parameter"
        LINK_OPTIONS ""
) #override global compile and link flags for the pass


target_link_libraries(MinimalPass
        "$<$<PLATFORM_ID:Darwin>:-undefined dynamic_lookup>")


set_target_properties(MinimalPass PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)