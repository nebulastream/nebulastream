
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (DEFINED CMAKE_TOOLCHAIN_FILE)
    SET(VCPKG_MANIFEST_MODE ON)
    SET(VCPKG_MANIFEST_DIR "${CMAKE_SOURCE_DIR}/vcpkg")
    SET(VCPKG_OVERLAY_TRIPLETS "${CMAKE_SOURCE_DIR}/vcpkg/custom-triplets")
    SET(VCPKG_OVERLAY_PORTS "${CMAKE_SOURCE_DIR}/vcpkg/vcpkg-registry/ports")
    message(STATUS "Using independent vcpkg-repository: ${CMAKE_TOOLCHAIN_FILE}")
elseif (DEFINED ENV{VCPKG_ROOT})
    SET(CMAKE_TOOLCHAIN_FILE $ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)
    message(STATUS "Using independent vcpkg-repository: ${CMAKE_TOOLCHAIN_FILE}")
else ()
    message(STATUS "Using internal vcpkg-repository")
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
    SET(VCPKG_OVERLAY_TRIPLETS "${CMAKE_SOURCE_DIR}/vcpkg/custom-triplets")
    SET(VCPKG_OVERLAY_PORTS "${CMAKE_SOURCE_DIR}/vcpkg/vcpkg-registry/ports")
    SET(CMAKE_TOOLCHAIN_FILE ${CLONE_DIR}/scripts/buildsystems/vcpkg.cmake)
endif ()

## Depending on the Sanitizers we choose different toolchains to build vcpkg dependencies and
## add global compiler / linker flags
if (NES_ENABLE_THREAD_SANITIZER)
    MESSAGE(STATUS "Enabling Thread Sanitizer")
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
    SET(VCPKG_TARGET_TRIPLET "x64-linux-tsan")
elseif (NES_ENABLE_UB_SANITIZER)
    MESSAGE(STATUS "Enabling UB Sanitizer")
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
    SET(VCPKG_TARGET_TRIPLET "x64-linux-ubsan")
elseif (NES_ENABLE_ADDRESS_SANITIZER)
    MESSAGE(STATUS "Enabling Address Sanitizer")
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
    SET(VCPKG_TARGET_TRIPLET "x64-linux-asan")
else ()
    MESSAGE(STATUS "Enabling No Sanitizer")
    SET(VCPKG_TARGET_TRIPLET "x64-linux-nes")
endif ()
