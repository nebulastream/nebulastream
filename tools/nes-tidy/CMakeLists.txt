# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

find_package(Clang REQUIRED CONFIG HINTS /usr/lib/llvm-19/lib/cmake/clang NO_DEFAULT_PATH)

message(STATUS "Found Clang: ${Clang_DIR}")
message(STATUS "LLVM_INCLUDE_DIRS: ${LLVM_INCLUDE_DIRS}")
message(STATUS "CLANG_INCLUDE_DIRS: ${CLANG_INCLUDE_DIRS}")

add_library(NesClangTidyChecks MODULE
    NesModule.cpp
    checks/MultiReturnValCheck.cpp
)

target_include_directories(NesClangTidyChecks PRIVATE
    ${LLVM_INCLUDE_DIRS}
    ${CLANG_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(NesClangTidyChecks PRIVATE
    clangTidy
    clangASTMatchers
)

# Disable RTTI to match LLVM/Clang build settings
target_compile_options(NesClangTidyChecks PRIVATE -fno-rtti)

# Custom target to run clang-tidy with our checks on the NES codebase
find_program(RUN_CLANG_TIDY NAMES run-clang-tidy-${LLVM_TOOLCHAIN_VERSION} run-clang-tidy)
find_program(CLANG_TIDY_EXE NAMES clang-tidy-${LLVM_TOOLCHAIN_VERSION} clang-tidy)

if (RUN_CLANG_TIDY AND CLANG_TIDY_EXE)
    add_custom_target(clang-tidy-check
        COMMAND ${RUN_CLANG_TIDY}
            -clang-tidy-binary ${CLANG_TIDY_EXE}
            -p ${CMAKE_BINARY_DIR}
            -load $<TARGET_FILE:NesClangTidyChecks>
            -checks="-*,nes-*"
            -header-filter=".*\/nes-.*"
            "nes-.*/src/.*\\.cpp$$"
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        DEPENDS NesClangTidyChecks
        COMMENT "Running NES custom clang-tidy checks..."
        USES_TERMINAL
    )
else ()
    message(WARNING "run-clang-tidy or clang-tidy not found, clang-tidy-check target will not be available")
endif ()
