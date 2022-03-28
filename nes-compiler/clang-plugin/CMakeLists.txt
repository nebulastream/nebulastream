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

cmake_minimum_required(VERSION 3.20.0)
include(CMakeDependentOption)
set(VCPKG_TARGET_TRIPLET x64-linux-nes)
set(CMAKE_TOOLCHAIN_FILE "/home/pgrulich/projects/nes/nebulastream/cmake-build-debug/nes-dependencies-v18-x64-linux-nes/scripts/buildsystems/vcpkg.cmake")

set(NES_DEPENDENCIES_BINARY_ROOT /home/pgrulich/projects/nes/nebulastream/cmake-build-debug/nes-dependencies-v18-x64-linux-nes/installed/x64-linux-nes)
set(CMAKE_C_COMPILER "${NES_DEPENDENCIES_BINARY_ROOT}/tools/llvm/clang")
set(CMAKE_CXX_COMPILER "${NES_DEPENDENCIES_BINARY_ROOT}/tools/llvm/clang++")
project(NES CXX C)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "-fno-omit-frame-pointer -fstandalone-debug -g -Wno-error -Wno-deprecated-enum-enum-conversion")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -fno-omit-frame-pointer -DNES_RELEASE_MODE=1")

find_package(Clang CONFIG REQUIRED)
find_package(LLVM CONFIG REQUIRED)
#add_executable(nes-clang-tool ${CMAKE_CURRENT_SOURCE_DIR}/src/ClangPlugin/ProxyFunctionCheck.cpp)
#target_include_directories(nes-clang-tool PRIVATE ${CLANG_INCLUDE_DIRS})
#llvm_map_components_to_libnames(llvm_libs Support Core IRReader)
#target_compile_options(nes-clang-tool PRIVATE "-fno-rtti")
#target_link_libraries(nes-clang-tool PRIVATE ${llvm_libs})
#target_link_libraries(nes-clang-tool
#        PRIVATE
#        clangAST
#        clangASTMatchers
#        clangBasic
#        clangFrontend
#        clangSerialization
#        clangTooling
#        clangBasic
#        clangDriver
#        clangFrontend
#        clangRewriteFrontend
#        clangSerialization
#        clangStaticAnalyzerFrontend
#        clangTooling
#        clangToolingSyntax
#        )


add_library(nes-clang-plugin SHARED ${CMAKE_CURRENT_SOURCE_DIR}/ExampleAttrInfo.cpp)
target_include_directories(nes-clang-plugin PRIVATE ${CLANG_INCLUDE_DIRS})
set_property(GLOBAL PROPERTY NES_CLANG_PLUGIN "${CMAKE_CURRENT_BINARY_DIR}/libnes-clang-plugin.so")

install(TARGETS nes-clang-plugin
        EXPORT NebulaStreamClangPlugin
        COMPONENT nesLibrary
        DESTINATION lib)


