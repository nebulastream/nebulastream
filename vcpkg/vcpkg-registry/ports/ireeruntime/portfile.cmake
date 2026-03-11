vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO iree-org/iree
    REF "ae97779f59a81bc56f804927be57749bb22548fa"
    SHA512 536be2e9e7069afe8558da548c46c3d35d07ceb37f6fec44df4ca7ff1826412fb91de52a8732c50083d5e9a4fea9a692f9e87fcd68b7795debe6426f05e091ce
    PATCHES
        0001-runtime-only-build.patch
        0002-fixup-install-destination.patch
        0003-do-not-enable-null-driver-in-debug.patch
)

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH_STABLEHLO
        REPO iree-org/stablehlo
        REF "6fabd27b15885179a3b6a601ea1e4171f2ed2c91"
        SHA512 03c55aec6d333064b276b283d2ef85e1714d293124c14264cbe442386b17aa3d6de81bd57165a1a84f712739058e7d0de724345e966c08b2b243b9b6ffc52f23
)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH_FLATCC
    REPO dvidelabs/flatcc
    REF "9362cd00f0007d8cbee7bff86e90fb4b6b227ff3"
    SHA512 a3110765d493a30fa1d2b08face65e1ad5461aa8cea38969307137bb1436db85b08cbf504650a53a59ccaeaf76d33ebbd2fe92b3b518f7041afdc390e7253109
)

FILE(REMOVE_RECURSE ${SOURCE_PATH}/third_party/stablehlo)
FILE(REMOVE_RECURSE ${SOURCE_PATH}/third_party/flatcc)

FILE(CREATE_LINK ${SOURCE_PATH_STABLEHLO} ${SOURCE_PATH}/third_party/stablehlo SYMBOLIC)
FILE(CREATE_LINK ${SOURCE_PATH_FLATCC} ${SOURCE_PATH}/third_party/flatcc SYMBOLIC)

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
