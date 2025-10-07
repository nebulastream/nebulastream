# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#right now the inlining is restricted to clang-19
#probably also supports other clang versions like clang-20 without changes to the source code, but this should be validated beforehand, as the pass API changes sometimes
function(does_clang_support_inline_pass result_var)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang"
            AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "19.0.0"
            AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "20")
        set(${result_var} TRUE PARENT_SCOPE)
    else ()
        set(${result_var} FALSE PARENT_SCOPE)
    endif ()
endfunction()


does_clang_support_inline_pass(NAUTILUS_INLINE_SUPPORTED)
if (NAUTILUS_INLINE_SUPPORTED)

    string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" SYSTEM_PROCESSOR)

    if (SYSTEM_PROCESSOR MATCHES "arm" OR SYSTEM_PROCESSOR MATCHES "aarch64")
        message(STATUS "Detected ARM architecture: ${CMAKE_SYSTEM_PROCESSOR}")
        set(IS_ARM_VALUE 1)
    else ()
        set(IS_ARM_VALUE 0)
    endif ()


    # Match llvm to the clang version
    find_package(LLVM ${CMAKE_CXX_COMPILER_VERSION} CONFIG REQUIRED)

    add_subdirectory(src)

    get_source(llvm-passes-sources LLVM_PASSES_SOURCE_FILES)
    add_library(llvm-passes SHARED ${LLVM_PASSES_SOURCE_FILES})

    #include LLVM headers
    target_include_directories(llvm-passes PRIVATE ${LLVM_INCLUDE_DIRS})
    target_compile_definitions(llvm-passes PRIVATE ${LLVM_DEFINITIONS})

    #include nautilus headers to load pass source code
    find_package(nautilus REQUIRED CONFIG)
    target_include_directories(llvm-passes PRIVATE $<TARGET_PROPERTY:nautilus::nautilus,INTERFACE_INCLUDE_DIRECTORIES>)

    target_include_directories(llvm-passes PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

    #override global compile and link flags for the pass, as e.g. changes to the stdlib break the pass when used by clang
    set_target_properties(llvm-passes PROPERTIES
            COMPILE_OPTIONS "-Wno-unused-parameter;-Wno-unused-variable;-Wno-extra-semi;-Wno-deprecated-copy-with-dtor"
            LINK_OPTIONS ""
    )

    # required for LLVM passes
    target_link_libraries(llvm-passes "$<$<PLATFORM_ID:Darwin>:-undefined dynamic_lookup>")

    set_target_properties(llvm-passes PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
    target_compile_definitions(llvm-passes PRIVATE IS_ARM=${IS_ARM_VALUE})

    add_custom_target(PassDependency DEPENDS $<TARGET_FILE:llvm-passes>)
endif ()

function(nautilus_inline target)
    does_clang_support_inline_pass(NAUTILUS_INLINE_SUPPORTED)
    if (NAUTILUS_INLINE_SUPPORTED AND TARGET PassDependency)
        #add nautilus as dependency if not yet done for the target
        #required because inline.hpp, which is required for inlining, needs to be linked against nautilus
        find_package(nautilus REQUIRED CONFIG)
        find_package(fmt REQUIRED CONFIG)
        find_package(spdlog REQUIRED CONFIG)
        find_package(MLIR REQUIRED CONFIG)
        target_link_libraries(${target} PUBLIC nautilus::nautilus)
        add_dependencies(${target} PassDependency)
        target_compile_options(${target} PRIVATE "-fpass-plugin=$<TARGET_FILE:llvm-passes>")
        message(STATUS "Applying nautilus inlining pass to target ${target}")
    else ()
        message(WARNING "Function inlining requires clang 19 during compilation. Probably also works with other clang versions. Adjust the version-check in CMake and find out")
    endif ()
endfunction()
