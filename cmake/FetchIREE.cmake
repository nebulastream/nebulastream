# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)

# The patches here:
# (1) disables `add_subdirectory(googletest)`
# (2) disables `find_package(CLANG/LLD)`
set(add_patches git apply
        ${PROJECT_SOURCE_DIR}/cmake/patches/iree/0001_ignore_gtest.patch
        ${PROJECT_SOURCE_DIR}/cmake/patches/iree/0002_ignore_clang_lld.patch)

FetchContent_Declare(iree
        GIT_REPOSITORY      https://github.com/iree-org/iree.git
        GIT_TAG             f3bef2de123f08b4fc3b0ce691494891bd6760d0
        PATCH_COMMAND       ${add_patches}
        UPDATE_DISCONNECTED 1
        GIT_CONFIG          "submodule.third_party/googletest.update=none"
                            "submodule.third_party/llvm-project.update=none"
)

# Ensure the library is fetched
FetchContent_GetProperties(iree)
if (NOT iree_POPULATED)
    message(STATUS "Fetching IREE")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
    set(IREE_ERROR_ON_MISSING_SUBMODULES OFF CACHE BOOL "Error if submodules have not been initialized.")

    # Options to reduce the build
    set(IREE_BUILD_TESTS OFF CACHE BOOL "Builds IREE unit tests.")
    set(IREE_BUILD_SAMPLES OFF CACHE BOOL "Builds IREE sample projects.")
    set(IREE_BUILD_ALL_CHECK_TEST_MODULES OFF CACHE BOOL "Builds all modules for iree_check_test, regardless of which would be tested")
    set(IREE_BUILD_BUNDLED_LLVM OFF CACHE BOOL "Builds the bundled llvm-project (vs using installed)")
    set(IREE_BUILD_BINDINGS_TFLITE_JAVA OFF CACHE BOOL "Builds the IREE TFLite Java bindings with the C API compatibility shim")

    # Disable HAL drivers for accelerators
    set(IREE_HAL_DRIVER_DEFAULTS OFF CACHE BOOL "Sets the default value for all runtime HAL drivers")
    set(IREE_HAL_DRIVER_LOCAL_SYNC ON CACHE BOOL "Enables the 'local-sync' runtime HAL driver") # single-threaded, synchronous
    set(IREE_HAL_DRIVER_LOCAL_TASK ON CACHE BOOL "Enables the 'local-task' runtime HAL driver") # multi-threaded, batch processing

    # Disable all backends except CPU
    set(IREE_TARGET_BACKEND_DEFAULTS OFF)
    set(IREE_TARGET_BACKEND_LLVM_CPU ON)

    set(IREE_ENABLE_SPLIT_DWARF ON CACHE BOOL "Enable gsplit-dwarf for debug information if the platform supports it")
    set(IREE_ENABLE_THIN_ARCHIVES ON CACHE BOOL "Enables thin ar archives (elf systems only). Disable for released static archives")
    FetchContent_MakeAvailable(iree)
endif()
