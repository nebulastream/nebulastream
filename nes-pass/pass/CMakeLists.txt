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
target_compile_options(MinimalPass PRIVATE -Wno-unused-parameter)
target_link_options(MinimalPass PRIVATE
        $<$<LINK_LANGUAGE:C>:-Wl,--no-allow-shlib-undefined>
)
if(NOT LLVM_ENABLE_RTTI)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif()

target_link_libraries(MinimalPass
        "$<$<PLATFORM_ID:Darwin>:-undefined dynamic_lookup>")


set_target_properties(MinimalPass PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)