# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        # NebulaStream's fork of Codon: the former port patches as commits, and a token-based parser
        # (codon/parser/tok), on the branch nebulastream.
        REPO ls-1801/codon
        REF a855d464e9ca6687de0231915a87ab70c57aafdc
        SHA512 3cde994c4c21bd7eae17e61e64efb90e07b1bb05bc472990c4eae0f5f9b85db84f5ea8787ba69bb48608cc16ef770309182ccd73e7de40253bcf9ce716958060
        HEAD_REF nebulastream
)

set(ADDITIONAL_CMAKE_OPTIONS "")
if (NOT "llvm" IN_LIST FEATURES)
    if (DEFINED ENV{LLVM_DIR} AND NOT "$ENV{LLVM_DIR}" STREQUAL "")
        list(APPEND ADDITIONAL_CMAKE_OPTIONS "-DLLVM_DIR=$ENV{LLVM_DIR}")
    endif()
endif()

vcpkg_cmake_configure(
        SOURCE_PATH "${SOURCE_PATH}"
        # Codon's CMakeLists writes config.h into the source tree; dbg/rel configuring in parallel races on it.
        DISABLE_PARALLEL_CONFIGURE
        OPTIONS
        -DCODON_COMPILER_ONLY=ON
        ${ADDITIONAL_CMAKE_OPTIONS}
)

vcpkg_cmake_install()

file(WRITE "${CURRENT_PACKAGES_DIR}/share/codon-compiler/codon-compiler-config.cmake" [=[
include(CMakeFindDependencyMacro)
find_dependency(LLVM CONFIG)
find_dependency(fmt CONFIG)
include("${CMAKE_CURRENT_LIST_DIR}/codon-compiler-targets.cmake")
set_property(TARGET codon::compiler APPEND PROPERTY
             INTERFACE_INCLUDE_DIRECTORIES "${LLVM_INCLUDE_DIRS}")
]=])

vcpkg_cmake_config_fixup(CONFIG_PATH "share/codon-compiler")
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE
        "${CURRENT_PACKAGES_DIR}/include/codon/app"
        "${CURRENT_PACKAGES_DIR}/include/codon/runtime/numpy"
)
file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
