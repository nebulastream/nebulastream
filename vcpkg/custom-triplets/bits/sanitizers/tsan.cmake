# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(VCPKG_CXX_FLAGS -fsanitize=thread)
set(VCPKG_C_FLAGS -fsanitize=thread)

# Building LLVM with the `-fsanitize=thread` flag causes the sanitizer itself to be built sanitized which is not
# possible. In general if the port supports sanitization via a CMake Option this should be the preferred way, to avoid
# incompatibilities.
if (PORT STREQUAL llvm)
    set(VCPKG_CXX_FLAGS "")
    set(VCPKG_C_FLAGS "")
    set(VCPKG_CMAKE_CONFIGURE_OPTIONS -DLLVM_USE_SANITIZER="Thread")
endif()
