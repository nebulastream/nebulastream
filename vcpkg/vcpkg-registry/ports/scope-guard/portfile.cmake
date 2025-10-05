vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO Neargye/scope_guard
    REF "v${VERSION}"
    SHA512 dea7e59917518a957f869bd8d455d1a118baf894368056925bf3c2d31eba8aa03ae4260e3e6ea401d31d7604091eb1315cdfe3487b897cd8cd1275d01b1b3bf9
    HEAD_REF master
)

set(VCPKG_BUILD_TYPE release)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DSCOPE_GUARD_OPT_BUILD_EXAMPLES=OFF
        -DSCOPE_GUARD_OPT_BUILD_TESTS=OFF
        -DSCOPE_GUARD_OPT_INSTALL=ON
)

vcpkg_cmake_install()

vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/${PORT})

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/lib")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
