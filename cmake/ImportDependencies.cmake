
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SET(VCPKG_OVERLAY_TRIPLETS "${CMAKE_SOURCE_DIR}/vcpkg/custom-triplets")
SET(VCPKG_OVERLAY_PORTS "${CMAKE_SOURCE_DIR}/vcpkg/vcpkg-registry/ports")

# Default Settings:
# VCPKG_ROOT -> Docker Environment with pre-built sdk.
# CMAKE_TOOLCHAIN_FILE -> Local VCPKG Repository. Will build dependencies locally
# NONE -> Create new local VCPKG Repository in project. Will build dependencies locally
if (DEFINED ENV{VCPKG_ROOT})
    # If we detect the VCPKG_ROOT environment we assume we are running in an environment
    # where an exported vcpkg sdk was prepared. This means we will not check if the current
    # vcpkg.json manifest matches the pre-built dependencies.
    message(STATUS "VCPKG_ROOT Environment is set: Assuming Docker Development Environment with pre-built dependencies at $ENV{VCPKG_ROOT}")
    SET(CMAKE_TOOLCHAIN_FILE $ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)
elseif (DEFINED CMAKE_TOOLCHAIN_FILE)
    # If the user supplies a custom CMAKE_TOOLCHAIN_FILE we assume we are running in manifest
    # mode and VCPKG will try to install or update dependencies based on the vcpkg.json
    # manifest. This requires a fully setup vcpkg installation, not just a pre-built sdk.
    message(STATUS "CMAKE_TOOLCHAIN_FILE was supplied: Assuming independent vcpkg-repository at ${CMAKE_TOOLCHAIN_FILE}")
    SET(VCPKG_MANIFEST_DIR "${CMAKE_SOURCE_DIR}/vcpkg")
else ()
    message(STATUS "Neither VCPKG_ROOT Environment nor CMAKE_TOOLCHAIN_FILE was supplied: Creating new internal vcpkg-repository and building dependencies")
    SET(CLONE_DIR ${CMAKE_SOURCE_DIR}/vcpkg-repository)
    SET(REPO_URL https://github.com/microsoft/vcpkg.git)
    if (NOT EXISTS ${CLONE_DIR})
        execute_process(
                COMMAND git clone --branch master ${REPO_URL} ${CLONE_DIR}
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                RESULT_VARIABLE GIT_CLONE_RESULT
        )
        if (GIT_CLONE_RESULT)
            message(FATAL_ERROR "Failed to clone repository: ${REPO_URL}")
        endif ()
    else ()
        message(STATUS "Repository already cloned in ${CLONE_DIR}")
    endif ()
    SET(VCPKG_MANIFEST_DIR "${CMAKE_SOURCE_DIR}/vcpkg")
    SET(CMAKE_TOOLCHAIN_FILE ${CLONE_DIR}/scripts/buildsystems/vcpkg.cmake)
endif ()

if (${NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR})
    message(STATUS "Enabling MLIR feature for the VPCKG install")
    list(APPEND VCPKG_MANIFEST_FEATURES "mlir")
endif ()

## Depending on the Sanitizers we choose different toolchains to build vcpkg dependencies and
## add global compiler / linker flags
if (NES_ENABLE_THREAD_SANITIZER)
    SET(VCPKG_TARGET_TRIPLET "x64-linux-tsan")
elseif (NES_ENABLE_UB_SANITIZER)
    SET(VCPKG_TARGET_TRIPLET "x64-linux-ubsan")
elseif (NES_ENABLE_ADDRESS_SANITIZER)
    SET(VCPKG_TARGET_TRIPLET "x64-linux-asan")
else ()
    SET(VCPKG_TARGET_TRIPLET "x64-linux-nes")
endif ()

SET(VCPKG_HOST_TRIPLET "x64-linux-nes")