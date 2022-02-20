# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)

get_source(nes-compiler NES_COMPILER_SOURCE_FILES)

message(STATUS "Compiler Source Files: ${NES_COMPILER_SOURCE_FILES}")


# Add Library
add_library(nes-compiler SHARED ${NES_COMPILER_SOURCE_FILES})
target_link_libraries(nes-compiler PUBLIC ${LIBRARIES} nes-common)
target_include_directories(nes-compiler PUBLIC "include")

find_package(LLVM CONFIG REQUIRED)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

include_directories(${LLVM_INCLUDE_DIRS})
separate_arguments(LLVM_DEFINITIONS_LIST NATIVE_COMMAND ${LLVM_DEFINITIONS})
add_definitions(${LLVM_DEFINITIONS_LIST})

target_include_directories(nes-compiler PRIVATE ${LLVM_INCLUDE_DIRS})

# Find the libraries that correspond to the LLVM components that we wish to use
llvm_map_components_to_libnames(llvm_libs Support Core native IRReader ipo linker mcjit debuginfodwarf orcjit Target X86 )
message(STATUS "Using llvm_libs.cmake in: ${llvm_libs}")
# Link against LLVM libraries
target_link_libraries(nes-compiler PRIVATE ${llvm_libs})

find_package(Clang CONFIG REQUIRED)
target_include_directories(nes-compiler PRIVATE ${CLANG_INCLUDE_DIRS})
target_link_libraries(nes-compiler PRIVATE clangAST clangBasic clangLex clangCodeGen clangFrontend clangEdit
        clangSerialization clangSema clangDriver clangParse clangAnalysis)

find_package(MLIR CONFIG REQUIRED)
target_include_directories(nes-compiler PRIVATE ${MLIR_INCLUDE_DIRS})
target_link_libraries(nes-compiler PRIVATE MLIRIR MLIRParser MLIRPass MLIRSupport)

if (NOT LLVM_ENABLE_RTTI)

    message(STATUS "Found LLVM_ENABLE_RTTI ${LLVM_ENABLE_RTTI}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-compiler
        COMPONENT nesLibrary
        DESTINATION lib)