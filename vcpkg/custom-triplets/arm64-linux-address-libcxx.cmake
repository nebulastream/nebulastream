# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${CMAKE_CURRENT_LIST_DIR}/bits/arch/arm64.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/bits/sanitizers/asan.cmake)
set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE ${CMAKE_CURRENT_LIST_DIR}/toolchains/libcxx.cmake)

<<<<<<< HEAD
=======
# boost-context does not recognize arm64
if (PORT STREQUAL "boost-context")
    SET(VCPKG_CMAKE_CONFIGURE_OPTIONS -DCMAKE_SYSTEM_PROCESSOR=aarch64)
endif ()

if (PORT STREQUAL "libpng")
    SET(VCPKG_CMAKE_CONFIGURE_OPTIONS -DCMAKE_SYSTEM_PROCESSOR=aarch64)
endif ()

# Building LLVM with the `-fsanitize=address` flag causes the sanitizer itself to be built sanitized which is not
# possible. In general if the port supports sanitization via a CMake Option this should be the preferred way, to avoid
# incompatibilities.
if (PORT STREQUAL llvm)
    set(VCPKG_CXX_FLAGS "")
    set(VCPKG_C_FLAGS "")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS -DLLVM_USE_SANITIZER="Address")
endif ()

if (PORT STREQUAL ireeruntime)
    set(VCPKG_CXX_FLAGS "")
    set(VCPKG_C_FLAGS "")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS -DIREE_ENABLE_ASAN=ON)
endif()
>>>>>>> df25c19ee3 (feat(Inference): Adds Inference Support)
