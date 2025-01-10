vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO AravisProject/aravis
        REF main
        SHA512 de70b2921c8e768d55040fa089a4bef6b8a27a901cb2af2be61fe632bc63f5059eff7da994c024060fc560fb8e9f9e639524db0892932c25a97baf439a02a4f3
        HEAD_REF main
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
