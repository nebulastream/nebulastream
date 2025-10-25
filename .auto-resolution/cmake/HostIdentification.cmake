# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# HostIdentification should set all system relevant information and make them available for rest of the CMake
# configuration. Currently it provides:
# - `NES_ARCH`: either `x64` or `arm64`
# - `NES_OS`: `linux`

find_program(UNAME uname REQUIRED)

execute_process(COMMAND ${UNAME} -m OUTPUT_VARIABLE UNAME_HOST_PROCESSOR)
if (UNAME_HOST_PROCESSOR MATCHES "x86_64")
    MESSAGE(STATUS "Found x64 architecture")
    SET(NES_ARCH "x64")
elseif (UNAME_HOST_PROCESSOR MATCHES "arm64" OR UNAME_HOST_PROCESSOR MATCHES "aarch64")
    MESSAGE(STATUS "Found arm64 architecture")
    SET(NES_ARCH "arm64")
else ()
    message(FATAL_ERROR "Only x86_64 and arm64 supported")
endif ()

execute_process(COMMAND ${UNAME} -s OUTPUT_VARIABLE UNAME_HOST_OS)
if (UNAME_HOST_OS MATCHES "Linux")
    MESSAGE(STATUS "Found linux operating system")
    set(NES_OS "linux")
else ()
    message(FATAL_ERROR "Only linux is supported. Use the nebulastream/nes-development:latest docker image, check the docs: https://github.com/nebulastream/nebulastream-public/blob/main/docs/development.md")
endif ()
