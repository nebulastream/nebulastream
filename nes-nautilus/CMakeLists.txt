# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-nautilus NES_NAUTILUS_SOURCE_FILES)

# Add Library
add_library(nes-nautilus ${NES_NAUTILUS_SOURCE_FILES})
target_link_libraries(nes-nautilus PUBLIC nes-schema nes-memory nes-output-formatters)


add_tests_if_enabled(tests)

# Add nes-nautilus to the include directories
target_include_directories(nes-nautilus PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)

# Link with the nautilus library
find_package(spdlog REQUIRED CONFIG)
find_package(MLIR REQUIRED CONFIG)
find_package(nautilus REQUIRED CONFIG)

# We have to set the nes-nautilus to public to use the nautilus library in other modules
# nautilus::nautilus-std provides the implementations of the nautilus/std/* wrappers (memcmp, cmath, ...)
target_link_libraries(nes-nautilus PUBLIC nautilus::nautilus
        nautilus::nautilus-std
        MLIRExecutionEngine
        MLIRFuncAllExtensions

        # Dialects
        MLIRSCFDialect
        MLIRFuncToLLVM
        MLIRMathToLLVM
        MLIRSCFToControlFlow
)
