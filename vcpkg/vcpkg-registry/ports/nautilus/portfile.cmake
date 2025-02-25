vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO nebulastream/nautilus
		REF a48e9513def6000b49717a5e8dce954511b1e649
        SHA512 a343ad0e5d3babb1ed071eede64ea2047871f6f139adc094ae43b0fbff5cd950be9c62871ee7cf5068d5e56f05d7647ccdccef29107cfdd4ddc201d9d2c82971
		PATCHES
		mod_signess__Signed-off-by__Nils_Schubert__nilslpschubert@gmail_com_.patch
		cache.patch
		llvm.patch
)

set(ADDITIONAL_CMAKE_OPTIONS "")
if (NOT "mlir" IN_LIST FEATURES)
    if($ENV{MLIR_DIR})
	    set(ADDITIONAL_CMAKE_OPTIONS "${ADDITIONAL_CMAKE_OPTIONS}-DMLIR_DIR=$ENV{MLIR_DIR} ")
    endif ()
endif ()

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
		OPTIONS
		-DENABLE_TESTS=OFF
		-DENABLE_MLIR_BACKEND=ON
		-DENABLE_BC_BACKEND=OFF
		-DENABLE_C_BACKEND=OFF
		-DENABLE_STACKTRACE=OFF
		-DUSE_EXTERNAL_MLIR=ON
		-DUSE_EXTERNAL_SPDLOG=ON
		-DUSE_EXTERNAL_FMT=ON
		${ADDITIONAL_CMAKE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/nautilus")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
