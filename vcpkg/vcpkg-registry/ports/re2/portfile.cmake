vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO google/re2
    REF "${VERSION}"
    SHA512 1cf7da605bef0c9de385d6e76d3a66b9c2677734306edca71f9581fb14e1c08bd8818a844623b4cf2833ee473aa760e90e737792ec67b7bb79fc0249a76a67ca
    HEAD_REF master
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DRE2_BUILD_TESTING=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/${PORT}")
vcpkg_fixup_pkgconfig()

vcpkg_copy_pdbs()

# Handle copyright
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
file(GLOB_RECURSE INTERNAL_HEADER "${SOURCE_PATH}/re2/*.h")
file(COPY ${INTERNAL_HEADER} DESTINATION "${CURRENT_PACKAGES_DIR}/include/re2")
file(GLOB_RECURSE INTERNAL_HEADER "${SOURCE_PATH}/util/*.h")
file(COPY ${INTERNAL_HEADER} DESTINATION "${CURRENT_PACKAGES_DIR}/include/util")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
