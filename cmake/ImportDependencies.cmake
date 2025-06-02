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
SET(VCPKG_MANIFEST_DIR "${CMAKE_SOURCE_DIR}/vcpkg")

option(USE_LOCAL_MLIR "Does not build llvm and mlir via vcpkg, rather uses a locally installed version" OFF)
option(USE_LIBCXX_IF_AVAILABLE "Use Libc++ if supported by the system" ON)

if (DEFINED ENV{NES_PREBUILT_VCPKG_ROOT})
    SET(DOCKER_DEV_IMAGE ON CACHE BOOL "Using Docker Development Image")
endif ()

if (NOT DEFINED ENV{LLVM_TOOLCHAIN_VERSION})
    set(ENV{LLVM_TOOLCHAIN_VERSION} "${LLVM_TOOLCHAIN_VERSION}")
endif ()

# Default Settings:
# CMAKE_TOOLCHAIN_FILE    -> Local VCPKG Repository. Will build dependencies locally
# NES_PREBUILT_VCPKG_ROOT -> Docker Environment with pre-built sdk.
# VCPKG_ROOT              -> user-managed vcpkg install. Will build dependencies locally
# NONE                    -> setup VCPKG Repository in project. Will build dependencies locally
if ($CACHE{DOCKER_DEV_IMAGE})
    # If we detect the NES_PREBUILT_VCPKG_ROOT environment we assume we are running in an environment
    # where an exported vcpkg sdk was prepared. This means we will not run in manifest mode,
    # We check if the VCPKG_DEPENDENCY_HASH environment matches the current hash
    message(STATUS "NES_PREBUILT_VCPKG_ROOT Environment is set: Assuming Docker Development Environment with pre-built dependencies at $ENV{NES_PREBUILT_VCPKG_ROOT}")
    execute_process(COMMAND ${CMAKE_SOURCE_DIR}/docker/dependency/hash_dependencies.sh
                    OUTPUT_VARIABLE VCPKG_HASH
                    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if ((NOT DEFINED ENV{VCPKG_DEPENDENCY_HASH}) OR (NOT $ENV{VCPKG_DEPENDENCY_HASH} STREQUAL "${VCPKG_HASH}"))
        message(WARNING
                "VCPKG Hash does not match, this is most likely due to an outdated development image. "
                "Make sure to update the current development image. "
                "The build will continue, but you may encounter errors during the build."
                "\nExpected Hash: ${VCPKG_HASH}\n"
                "Development Image Hash: $ENV{VCPKG_DEPENDENCY_HASH}\n"
                "To update the development image, you can call the install local script via "
                "./scripts/install-local-docker-environment.sh\n"
        )
    endif ()

    # Overwriting stdlib and sanitizer option based on docker image
    if ($ENV{VCPKG_STDLIB} STREQUAL "libstdcxx")
        SET(USE_LIBCXX_IF_AVAILABLE OFF)
    endif()

    SET(SANITIZER_OPTION $ENV{VCPKG_SANITIZER})
    unset(VCPKG_MANIFEST_DIR) # prevents vcpkg from finding the vcpkg.json and building dependencies
    SET(CMAKE_TOOLCHAIN_FILE $ENV{NES_PREBUILT_VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)
elseif (DEFINED CMAKE_TOOLCHAIN_FILE)
    # If the user supplies a custom CMAKE_TOOLCHAIN_FILE we assume we are running in manifest
    # mode and VCPKG will try to install or update dependencies based on the vcpkg.json
    # manifest. This requires a fully setup vcpkg installation, not just a pre-built sdk.
    message(STATUS "CMAKE_TOOLCHAIN_FILE was supplied: Assuming independent vcpkg-repository at ${CMAKE_TOOLCHAIN_FILE}")
elseif (DEFINED ENV{VCPKG_ROOT})
    message(STATUS "VCPKG_ROOT Environment is set: Assuming user-managed vcpkg install at $ENV{VCPKG_ROOT}")
    SET(CMAKE_TOOLCHAIN_FILE $ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)
else ()
    message(WARNING "Neither VCPKG_ROOT/NES_PREBUILT_VCPKG_ROOT Environment nor CMAKE_TOOLCHAIN_FILE was supplied: Creating new internal vcpkg-repository and building dependencies. This might take a while. If possible, use the development container, check the docs: https://github.com/nebulastream/nebulastream-public/blob/main/docs/development.md")
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
    SET(CMAKE_TOOLCHAIN_FILE ${CLONE_DIR}/scripts/buildsystems/vcpkg.cmake)
endif ()

# By default we build MLIR via VCPKG. However the USE_LOCAL_MLIR options allows to supply a locally installed version
# of MLIR.
if (NOT ${USE_LOCAL_MLIR})
    message(STATUS "Enabling MLIR feature for the VPCKG install")
    list(APPEND VCPKG_MANIFEST_FEATURES "mlir")
else ()
    message(STATUS "Using locally installed MLIR")
    # This code is called before VCPKG and before project() has been called. This means many configuration have not
    # been set by cmake. LLVM has a few shared libraries (which we do not use), that require the target machine to
    # support dynamic linking (which is usually the case unless working with small embedded devices).
    SET_PROPERTY(GLOBAL PROPERTY TARGET_SUPPORTS_SHARED_LIBS TRUE)
    find_package(MLIR CONFIG QUIET)
    # One way to propagate configurations to third-party libraries (in this case nautilus) is via environment variables.
    # The nautilus vcpkg port script will pick up the MLIR_DIR environment variable during build, which allows the
    # nautilus cmake configuration to find the locally installed version of MLIR.
    SET(ENV{MLIR_DIR} "${MLIR_DIR}")
    SET(VCPKG_ENV_PASSTHROUGH "MLIR_DIR")
endif ()

SET(VCPKG_STDLIB "libcxx")
if (NOT USE_LIBCXX_IF_AVAILABLE)
    SET(VCPKG_STDLIB "local")
endif ()

execute_process(COMMAND uname -m OUTPUT_VARIABLE VCPKG_HOST_PROCESSOR)
if (VCPKG_HOST_PROCESSOR MATCHES "x86_64")
    set(VCPKG_HOST_PROCESSOR "x64")
elseif (VCPKG_HOST_PROCESSOR MATCHES "arm64" OR VCPKG_HOST_PROCESSOR MATCHES "aarch64")
    set(VCPKG_HOST_PROCESSOR "arm64")
else ()
    message(FATAL_ERROR "Only x86_64 and arm64 supported")
endif ()

execute_process(COMMAND uname -s OUTPUT_VARIABLE VCPKG_HOST_OS)
if (NOT VCPKG_HOST_OS MATCHES "Linux")
    message(FATAL_ERROR "Only linux is supported. Use the nebulastream/nes-development:latest docker image, check the docs: https://github.com/nebulastream/nebulastream-public/blob/main/docs/development.md")
endif ()

#SET(VCPKG_TARGET_TRIPLET "${VCPKG_HOST_PROCESSOR}-linux-${SANITIZER_OPTION}-${VCPKG_STDLIB}")
SET(VCPKG_TARGET_TRIPLET arm64-linux-none-libcxx-zynq)
SET(VCPKG_HOST_TRIPLET "${VCPKG_HOST_PROCESSOR}-linux-none-${VCPKG_STDLIB}")

message(STATUS "VPCKG target triplet: ${VCPKG_TARGET_TRIPLET}")
message(STATUS "VPCKG host triplet: ${VCPKG_HOST_TRIPLET}")
