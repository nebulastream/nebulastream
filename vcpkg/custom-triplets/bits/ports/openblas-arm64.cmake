# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# OpenBLAS needs an explicit processor to detect the ARM64 architecture.
list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DCMAKE_SYSTEM_PROCESSOR=aarch64)

cmake_host_system_information(RESULT _NES_HOST_PLATFORM QUERY OS_PLATFORM)
if (_NES_HOST_PLATFORM MATCHES "x86_64|AMD64")
    # Cross compiling: the SVE cores (NEOVERSEV1, NEOVERSEN2, ARMV8SVE, A64FX) that dynamic-arch adds for clang reference
    # kernel files missing in OpenBLAS 0.3.29's CMake build. Keep dispatch for the non-SVE cores.
    list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS "-DDYNAMIC_LIST=CORTEXA53;CORTEXA57;THUNDERX;THUNDERX2T99;TSV110;EMAG8180;NEOVERSEN1;THUNDERX3T110")
endif ()
