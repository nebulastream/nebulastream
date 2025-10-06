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
target_link_libraries(nes-nautilus PUBLIC nes-memory)

# Use C++20 to stay compatible with reflect-cpp's requirements
target_compile_features(nes-nautilus PUBLIC cxx_std_20)

find_program(CAPNP_EXECUTABLE capnp
    HINTS
        "$ENV{HOME}/.nix-profile/bin"
        /nix/profile/bin
        /run/current-system/sw/bin
        /nix/var/nix/profiles/default/bin
)
if(CAPNP_EXECUTABLE)
    get_filename_component(_capnp_bin_dir "${CAPNP_EXECUTABLE}" DIRECTORY)
    get_filename_component(_capnp_root "${_capnp_bin_dir}/.." ABSOLUTE)
    list(APPEND CMAKE_PREFIX_PATH "${_capnp_root}")
    set(NES_CAPNP_ROOT "${_capnp_root}" CACHE PATH "Detected Cap'n Proto root" FORCE)
    set(CapnProto_DIR "${_capnp_root}/lib/cmake/CapnProto" CACHE PATH "CapnProto package path" FORCE)
    message(STATUS "Detected capnp at ${CAPNP_EXECUTABLE}, root ${_capnp_root}")
else()
    message(STATUS "capnp executable not found on PATH")
endif()
unset(_capnp_bin_dir)
unset(_capnp_root)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-nautilus REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-nautilus PUBLIC "-fPIC")
endif ()

add_tests_if_enabled(tests)

# Add nes-nautilus to the include directories
target_include_directories(nes-nautilus PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)

# Resolve reflect-cpp: use provided package if available, otherwise fetch it locally.
set(_nes_reflectcpp_available FALSE)
if(NOT TARGET reflectcpp::reflectcpp)
    find_package(reflectcpp QUIET)
    if(reflectcpp_FOUND)
        set(_nes_reflectcpp_available TRUE)
    else()
        include(FetchContent)
        FetchContent_Declare(
            reflect-cpp
            GIT_REPOSITORY https://github.com/getml/reflect-cpp.git
            GIT_TAG v0.20.0
            GIT_SHALLOW TRUE
        )
        set(REFLECTCPP_ALL_FORMATS ON CACHE BOOL "Enable all reflect-cpp formats" FORCE)
        set(REFLECTCPP_CAPNPROTO ON CACHE BOOL "Enable Cap'n Proto support in reflect-cpp" FORCE)
        FetchContent_MakeAvailable(reflect-cpp)
        if(TARGET reflectcpp::reflectcpp)
            get_target_property(_nes_reflectcpp_impl_target reflectcpp::reflectcpp ALIASED_TARGET)
            if(_nes_reflectcpp_impl_target)
                set(_nes_reflectcpp_target ${_nes_reflectcpp_impl_target})
            else()
                set(_nes_reflectcpp_target reflectcpp::reflectcpp)
            endif()
            get_target_property(_nes_reflectcpp_is_imported ${_nes_reflectcpp_target} IMPORTED)
            if(NOT _nes_reflectcpp_is_imported)
                if(NOT TARGET CapnProto::capnp)
                    find_package(CapnProto CONFIG REQUIRED)
                endif()
                if(NOT TARGET nes-reflectcpp-capnp)
                    add_library(nes-reflectcpp-capnp STATIC ${reflectcpp_SOURCE_DIR}/src/reflectcpp_capnproto.cpp)
                    target_include_directories(nes-reflectcpp-capnp PRIVATE ${reflectcpp_SOURCE_DIR}/src)
                    target_compile_features(nes-reflectcpp-capnp PUBLIC cxx_std_20)
                    target_link_libraries(nes-reflectcpp-capnp PUBLIC ${_nes_reflectcpp_target} CapnProto::capnp CapnProto::kj CapnProto::capnpc CapnProto::kj-gzip)
                endif()
            endif()
            unset(_nes_reflectcpp_target)
            unset(_nes_reflectcpp_impl_target)
            unset(_nes_reflectcpp_is_imported)
        endif()
        set(_nes_reflectcpp_available TRUE)
    endif()
else()
    set(_nes_reflectcpp_available TRUE)
endif()

if(NOT TARGET nes-reflectcpp-capnp AND DEFINED reflectcpp_SOURCE_DIR)
    add_library(nes-reflectcpp-capnp STATIC ${reflectcpp_SOURCE_DIR}/src/reflectcpp_capnproto.cpp)
    target_include_directories(nes-reflectcpp-capnp PRIVATE ${reflectcpp_SOURCE_DIR}/src)
    target_compile_features(nes-reflectcpp-capnp PUBLIC cxx_std_20)
    target_compile_options(nes-reflectcpp-capnp PRIVATE -Wno-unused-parameter -Wno-missing-designated-field-initializers -Wno-missing-field-initializers)
    if(NOT TARGET CapnProto::capnp)
        find_package(CapnProto CONFIG REQUIRED)
    endif()
    target_link_libraries(nes-reflectcpp-capnp PUBLIC reflectcpp::reflectcpp CapnProto::capnp CapnProto::kj CapnProto::capnpc CapnProto::kj-gzip)
endif()

find_package(CapnProto QUIET)
if(NOT CapnProto_FOUND AND NES_CAPNP_ROOT)
    set(CapnProto_DIR "${NES_CAPNP_ROOT}/lib/cmake/CapnProto" CACHE PATH "CapnProto package path" FORCE)
    find_package(CapnProto QUIET)
endif()

# Link with the nautilus library
find_package(spdlog REQUIRED CONFIG)
find_package(MLIR REQUIRED CONFIG)
find_package(nautilus REQUIRED CONFIG)

# We have to set the nes-nautilus to public to use the nautilus library in other modules
target_link_libraries(nes-nautilus PUBLIC
        nautilus::nautilus
        MLIRExecutionEngine
        MLIRFuncAllExtensions

        # Dialects
        MLIRSCFDialect
        MLIRFuncToLLVM
        MLIRSCFToControlFlow)

if(TARGET nes-reflectcpp-capnp)
    target_link_libraries(nes-nautilus PUBLIC nes-reflectcpp-capnp)
endif()

if(_nes_reflectcpp_available)
    target_link_libraries(nes-nautilus PUBLIC reflectcpp::reflectcpp)
    if(TARGET reflectcpp::reflectcpp)
        get_target_property(_nes_reflectcpp_is_imported reflectcpp::reflectcpp IMPORTED)
        if(NOT _nes_reflectcpp_is_imported AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            get_target_property(_nes_reflectcpp_impl_target reflectcpp::reflectcpp ALIASED_TARGET)
            if(_nes_reflectcpp_impl_target)
                set(_nes_reflectcpp_target ${_nes_reflectcpp_impl_target})
            else()
                set(_nes_reflectcpp_target reflectcpp::reflectcpp)
            endif()
            target_compile_options(${_nes_reflectcpp_target} PRIVATE
                -Wno-missing-designated-field-initializers
                -Wno-unused-parameter)
            unset(_nes_reflectcpp_target)
            unset(_nes_reflectcpp_impl_target)
        endif()
        get_target_property(_nes_reflectcpp_include_dirs reflectcpp::reflectcpp INTERFACE_INCLUDE_DIRECTORIES)
        if(_nes_reflectcpp_include_dirs)
            target_include_directories(nes-nautilus SYSTEM PUBLIC ${_nes_reflectcpp_include_dirs})
        endif()
        unset(_nes_reflectcpp_include_dirs)
    endif()
endif()
if(CapnProto_FOUND)
    target_link_libraries(nes-nautilus PUBLIC CapnProto::capnp)
endif()
