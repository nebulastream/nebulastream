vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO facebook/folly
	REF c47d0c778950043cbbc6af7fde616e9aeaf054ca
        SHA512 d74db09cfc1407a16a5b77b2911a7e599c3dbc477c15173e4635b0721e496a10a8d5eaf6b045b28d4e56163a694f66a63bf0a107420193d27f18ff1068136e53
        PATCHES
		0001-Folly-Patch.patch
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        ${FEATURE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup()

file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/share/doc
                    ${CURRENT_PACKAGES_DIR}/debug/share
                    ${CURRENT_PACKAGES_DIR}/debug/include
)

file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
