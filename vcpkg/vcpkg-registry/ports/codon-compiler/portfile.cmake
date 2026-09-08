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
        REPO exaloop/codon
        REF f1cd7006ad659a80852fd6de25885a0c639c94dc
        SHA512 89feb7ce0530252c1202a425c98d3bac2ece2ce34abb8326da424ed93e790430ba15cc0ef7d7f3a7f174796afa3576bcb8ff265ec0d75bd91dc006134b0fd2c1
        HEAD_REF develop
        PATCHES
        0001-build-compiler-only-with-llvm-21.patch
)

set(ADDITIONAL_CMAKE_OPTIONS "")
if (NOT "llvm" IN_LIST FEATURES)
    if (DEFINED ENV{LLVM_DIR} AND NOT "$ENV{LLVM_DIR}" STREQUAL "")
        list(APPEND ADDITIONAL_CMAKE_OPTIONS "-DLLVM_DIR=$ENV{LLVM_DIR}")
    endif()
endif()

vcpkg_cmake_configure(
        SOURCE_PATH "${SOURCE_PATH}"
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
