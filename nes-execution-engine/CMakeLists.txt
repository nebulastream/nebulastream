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
set(CMAKE_VERBOSE_MAKEFILE on)

include(../cmake/LLVMIRUtil.cmake)
add_library(nes-runtime /home/pgrulich/projects/nes/nebulastream/nes-execution-engine/src/Experimental/Interpreter/PipelineContext.cpp /home/pgrulich/projects/nes/nebulastream/nes-execution-engine/src/Experimental/Runtime/RuntimePipelineContext.cpp)
target_include_directories(nes-runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(nes-runtime PUBLIC /home/pgrulich/projects/nes/nebulastream/nes-core/include)
target_include_directories(nes-runtime PUBLIC /home/pgrulich/projects/nes/nebulastream/nes-common/include)
set_target_properties(nes-runtime PROPERTIES LINKER_LANGUAGE CXX)
llvmir_attach_bc_target(qux_bc nes-runtime)
llvmir_attach_link_target(bar_llvmlink qux_bc)
llvmir_attach_opt_pass_target(bar_llvmlink_opt bar_llvmlink --inline -S -O3)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-execution-engine NES_EXECUTION_ENGINE_SOURCE_FILES)

# Add Library
add_library(nes-execution-engine SHARED ${NES_EXECUTION_ENGINE_SOURCE_FILES})
message(STATUS "Here: ${NES_EXECUTION_ENGINE_SOURCE_FILES}")
target_link_libraries(nes-execution-engine PUBLIC nes)
target_include_directories(nes-execution-engine PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

# include MLIR. This is currently required
find_package(MLIR REQUIRED CONFIG)
if (MLIR_FOUND)
    target_include_directories(nes-execution-engine SYSTEM PUBLIC
            $<BUILD_INTERFACE:${MLIR_INCLUDE_DIRS}>
            $<INSTALL_INTERFACE:include/nebulastream/>)
    add_compile_definitions(MLIR_COMPILER)
else ()
    message(FATAL_ERROR "Cannot find mlir")
endif (MLIR_FOUND)
message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")


if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-execution-engine
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)