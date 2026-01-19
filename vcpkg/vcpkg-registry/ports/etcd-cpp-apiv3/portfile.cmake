vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO etcd-cpp-apiv3/etcd-cpp-apiv3
    REF "v${VERSION}"
    SHA512 52f3cf14ad5594c04a086786d3459aee0986017a0314dfdf3fff1715677ff7a7ebedcc0afc28e1d7e75b8991ab6ede95eeded472d85ac1def84343cc1c54a30a
    HEAD_REF master
    PATCHES
        fix-gpr-assert.patch
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DBUILD_ETCD_TESTS=OFF
        -DBUILD_ETCD_CORE_ONLY=ON
)

vcpkg_cmake_install()

vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/etcd-cpp-api)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE.txt")
