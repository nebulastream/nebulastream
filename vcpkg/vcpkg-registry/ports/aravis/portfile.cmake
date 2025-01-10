vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO AravisProject/aravis
        REF "0.8.35"
        SHA512 c00feefb89757ad56cf6781ff347cc2e11d9d17c54e7053916e41a15656e9ebaf24c9f62f521fce760e0e97b2dba3002a270555d6c084461f968f9572a53bb58
        PATCHES
            0001-Always-uses-legacy-endinaness.patch
)


vcpkg_configure_meson(
        SOURCE_PATH "${SOURCE_PATH}"
        LANGUAGES C
        OPTIONS
        -Dtests=false
)

vcpkg_install_meson()
vcpkg_fixup_pkgconfig()
vcpkg_copy_pdbs()
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYING")
