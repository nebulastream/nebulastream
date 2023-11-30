find_package(Argumentum REQUIRED)

add_subdirectory(src)
set(BOOST_ROOT "${_VCPKG_INSTALLED_DIR}/x64-linux-nes")
find_package(Boost REQUIRED COMPONENTS system)
find_package(Argumentum REQUIRED)
get_source(unikernel-query-export UNIKERNEL_QUERY_EXPORT_SOURCES)
add_library(UnikernelQueryExport SHARED ${UNIKERNEL_QUERY_EXPORT_SOURCES})
target_include_directories(UnikernelQueryExport PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-compiler/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-client/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
)
target_link_libraries(UnikernelQueryExport PUBLIC
        nes-client
        nes-common
        nes-data-types
)

get_source(unikernel-llvm-export UNIKERNEL_LLVM_EXPORT_SOURCES)
add_library(UnikernelLLVMExport SHARED
        ${UNIKERNEL_LLVM_EXPORT_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/src/CompilerInvoker.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/LLVMImporter.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/UnikernelPipelineExport.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/UnikernelExport.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/LLVMModuleStripper.cpp
)
target_include_directories(UnikernelLLVMExport PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/tests/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-compiler/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
)
find_package(MLIR REQUIRED CONFIG)
get_property(mlir_libs GLOBAL PROPERTY MLIR_ALL_LIBS)
get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)
target_include_directories(UnikernelLLVMExport PUBLIC ${LLVM_INCLUDE_DIRS})
llvm_map_components_to_libnames(llvm_libs support core x86codegen)
target_link_libraries(UnikernelLLVMExport PRIVATE
        nes-common
        nes-data-types
        Boost::system
        ${conversion_libs}
        ${dialect_libs}
        ${mlir_libs}
        ${llvm_libs}
)

get_source(unikernel-export UNIKERNEL_EXPORT_SOURCES)
find_package(Boost 1.78.0 REQUIRED COMPONENTS system)
add_executable(NesUnikernelExporter src/main.cpp
        ${UNIKERNEL_EXPORT_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/src/CLIOptions.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/OperatorHandlerTracer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/YamlExport.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/QueryCompiler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ExportQueryOptimizer.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ExportPhaseFactory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/NoOpPhysicalSource.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/NoOpSourceCreator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/NoOpSourceDescriptor.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/NoOpNodeEngine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/NoOpQueryManager.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/NoOpWorkerContext.cpp
)


target_include_directories(NesUnikernelExporter PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/tests/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-compiler/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-optimizer/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-client/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
)

find_package(yaml-cpp REQUIRED)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=unused-parameter -Wno-error=unused-private-field -DUNIKERNEL_EXPORT=1 -std=c++20")

# Copy Public Headers
register_public_header_file(NesUnikernelExporter ${CMAKE_CURRENT_SOURCE_DIR}/include/OperatorHandler.hpp OperatorHandler.hpp)
register_public_header_file(NesUnikernelExporter ${CMAKE_CURRENT_SOURCE_DIR}/../nes-unikernel/include/UnikernelStage.hpp UnikernelStage.hpp)
file(GLOB HEADER_FILES ${CMAKE_CURRENT_SOURCE_DIR}/include/handler/*.hpp)
foreach(HEADER_FILE ${HEADER_FILES})
    get_filename_component(HEADER_NAME ${HEADER_FILE} NAME)
    register_public_header_file(NesUnikernelExporter ${CMAKE_CURRENT_SOURCE_DIR}/include/handler/${HEADER_NAME} handler/${HEADER_NAME})
endforeach()

target_link_libraries(NesUnikernelExporter PUBLIC
        Argumentum::argumentum
        yaml-cpp
        UnikernelCommon
        nes-common
        nes-data-types
        UnikernelQueryExport
        UnikernelLLVMExport
)


SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L /usr/local/lib -fuse-ld=lld")
