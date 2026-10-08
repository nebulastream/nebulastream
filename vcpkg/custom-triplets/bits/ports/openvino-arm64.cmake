# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# OpenVINO does not recognize arm64.
list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DCMAKE_SYSTEM_PROCESSOR=aarch64)

cmake_host_system_information(RESULT _NES_HOST_PLATFORM QUERY OS_PLATFORM)
if (_NES_HOST_PLATFORM MATCHES "x86_64|AMD64")
    # Cross compiling: ComputeLibrary's scons build calls aarch64-linux-gnu-ar etc. by bare name, so the wrapper
    # directory has to be on PATH.
    cmake_path(ABSOLUTE_PATH CMAKE_CURRENT_LIST_DIR BASE_DIRECTORY / NORMALIZE OUTPUT_VARIABLE _NES_LIST_DIR)
    set(ENV{PATH} "${_NES_LIST_DIR}/../../../../cmake/toolchains/arm64-bin:$ENV{PATH}")
endif ()
