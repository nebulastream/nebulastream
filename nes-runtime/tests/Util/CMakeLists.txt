

add_library(nes-runtime-test-util SHARED
        "src/AbstractCompilationBackendTest.cpp"
        "src/MockedPipelineExecutionContext.cpp"
        "src/CollectOperator.cpp"
        "src/UtilityFunctions.cpp")

message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-runtime-test-util PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(nes-runtime-test-util PUBLIC nes-common nes-data-types nes-operators nes-grpc nes-runtime nes-test-util nes-configurations)
