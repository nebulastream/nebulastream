# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO iree-org/iree
    REF "4fa6868eea932128282aa3df38a771a4032ba42a"
    SHA512 0db8ccbbc514758fba812fe6e872603846796e18ffad075beab2fbc768a63d1f7019b02346c171e96e44e525cf59ce9cd1a637cab71609712628941caa2492fc
    PATCHES
        0001-runtime-only-build.patch
        0002-fixup-install-destination.patch
        0003-do-not-enable-null-driver-in-debug.patch
        0004-fix-missing-check-c-source-compiles-include.patch
        0005-include-printf-in-install.patch
)

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH_STABLEHLO
        REPO iree-org/stablehlo
        REF "46af9d3590bc712d1c9e2f8ef408d2ecbb6108a3"
        SHA512 e7186df414dd2d860fa4c13d02e6bbc0a339f787486d599d6c0a2cb6aa1e64480e4b8cae0f558447d7291a997e20d8c8f35a37314209141363c530efbc8d3c61
)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH_FLATCC
    REPO dvidelabs/flatcc
    REF "9362cd00f0007d8cbee7bff86e90fb4b6b227ff3"
    SHA512 a3110765d493a30fa1d2b08face65e1ad5461aa8cea38969307137bb1436db85b08cbf504650a53a59ccaeaf76d33ebbd2fe92b3b518f7041afdc390e7253109
)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH_PRINTF
    REPO eyalroz/printf
    REF "f1b728cbd5c6e10dc1f140f1574edfd1ccdcbedb"
    SHA512 947be29ae0ec5d76277a404ee7d86d811a9d37c2e3463e42f73f401df968494f3a5f8c58b43fd34d66217f8750c10369930250f40d3c4b10dcc4745f8b4fd6cc
)

FILE(REMOVE_RECURSE ${SOURCE_PATH}/third_party/stablehlo)
FILE(REMOVE_RECURSE ${SOURCE_PATH}/third_party/flatcc)
FILE(REMOVE_RECURSE ${SOURCE_PATH}/third_party/printf)

FILE(CREATE_LINK ${SOURCE_PATH_STABLEHLO} ${SOURCE_PATH}/third_party/stablehlo SYMBOLIC)
FILE(CREATE_LINK ${SOURCE_PATH_FLATCC} ${SOURCE_PATH}/third_party/flatcc SYMBOLIC)
FILE(CREATE_LINK ${SOURCE_PATH_PRINTF} ${SOURCE_PATH}/third_party/printf SYMBOLIC)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DIREE_BUILD_TESTS=OFF
        -DIREE_BUILD_COMPILER=OFF
        -DIREE_BUILD_SAMPLES=OFF
        -DIREE_BUILD_ALL_CHECK_TEST_MODULES=OFF
        -DIREE_BUILD_BUNDLED_LLVM=OFF
        -DIREE_BUILD_BINDINGS_TFLITE=OFF
        -DIREE_BUILD_BINDINGS_TFLITE_JAVA=OFF
        -DIREE_ENABLE_THREADING=OFF
        -DIREE_HAL_DRIVER_DEFAULTS=OFF
        -DIREE_HAL_DRIVER_LOCAL_SYNC=ON
        -DIREE_TARGET_BACKEND_DEFAULTS=OFF
        -DIREE_ERROR_ON_MISSING_SUBMODULES=OFF
        -DIREE_ENABLE_CPUINFO=OFF
        -DIREE_ENABLE_LIBBACKTRACE=OFF
        -DIREE_INPUT_TORCH=OFF
)

vcpkg_cmake_build(
        LOGFILE_BASE install-build
)

vcpkg_cmake_build(
        LOGFILE_BASE install
        TARGET iree-install-dist
)

function(_copy_runtime_source_headers)
    set(_RUNTIME_HEADER_SOURCE_DIR "${SOURCE_PATH}/runtime/src")
    file(
            GLOB_RECURSE _RUNTIME_SRC_HEADERS
            RELATIVE "${_RUNTIME_HEADER_SOURCE_DIR}"
            "${_RUNTIME_HEADER_SOURCE_DIR}/*.h")
    foreach(_RUNTIME_HEADER ${_RUNTIME_SRC_HEADERS})
        set(_src_file "${_RUNTIME_HEADER_SOURCE_DIR}/${_RUNTIME_HEADER}")
        set(_dst_file "${CURRENT_PACKAGES_DIR}/include/${_RUNTIME_HEADER}")
        get_filename_component(_parent_dir "${_dst_file}" DIRECTORY)
        file(MAKE_DIRECTORY "${_parent_dir}")
        file(COPY_FILE "${_src_file}" "${_dst_file}" ONLY_IF_DIFFERENT)
    endforeach()
endfunction()
_copy_runtime_source_headers()


FILE(MAKE_DIRECTORY ${CURRENT_PACKAGES_DIR}/debug/share/ireeruntime)
FILE(MAKE_DIRECTORY ${CURRENT_PACKAGES_DIR}/share/ireeruntime)

FILE(RENAME ${CURRENT_PACKAGES_DIR}/debug/IREETargets-Runtime.cmake ${CURRENT_PACKAGES_DIR}/debug/share/ireeruntime/IREETargets-Runtime.cmake)
FILE(RENAME ${CURRENT_PACKAGES_DIR}/debug/IREETargets-Runtime-debug.cmake ${CURRENT_PACKAGES_DIR}/debug/share/ireeruntime/IREETargets-Runtime-debug.cmake)


FILE(RENAME ${CURRENT_PACKAGES_DIR}/lib/cmake/IREE/IREERuntimeConfig.cmake ${CURRENT_PACKAGES_DIR}/share/ireeruntime/IREERuntimeConfig.cmake)
FILE(RENAME ${CURRENT_PACKAGES_DIR}/IREETargets-Runtime.cmake ${CURRENT_PACKAGES_DIR}/share/ireeruntime/IREETargets-Runtime.cmake)
FILE(RENAME ${CURRENT_PACKAGES_DIR}/IREETargets-Runtime-release.cmake ${CURRENT_PACKAGES_DIR}/share/ireeruntime/IREETargets-Runtime-release.cmake)
file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/IREERuntime" RENAME copyright)

vcpkg_cmake_config_fixup()
FILE(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include)
FILE(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/lib/cmake)
FILE(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/lib/cmake)
FILE(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include)
FILE(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/share)
