vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO nebulastream/nautilus
		REF 364c89c184430e1ccd0d9422fc73b1cc54c33ca8
        SHA512 e152ecd75fc881bf12622fd443890d7a33c86ce1f31ce3b59ecce5878ac03bb75210048af53dbf714095332c9d6acd86434ea1ef7e59a20c2fd97fd5e8023b08
		PATCHES
		0001_fixes_clang_tidy.patch
		0002_llvm_v20.patch
		0003-disable-ubsan-function-call-check.patch
		0004-disable-mlir-multithreading.patch
		0005-clang-tidy-memory-leak.patch
)

set(ADDITIONAL_CMAKE_OPTIONS "")
set(ENABLE_MLIR ON)
if (NOT "mlir" IN_LIST FEATURES)
    set(ENABLE_MLIR OFF)
    if($ENV{MLIR_DIR})
        set(ENABLE_MLIR ON)
        set(ADDITIONAL_CMAKE_OPTIONS "${ADDITIONAL_CMAKE_OPTIONS}-DMLIR_DIR=$ENV{MLIR_DIR} ")
    endif ()
endif ()

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
		OPTIONS
		-DENABLE_TESTS=OFF
		-DENABLE_MLIR_BACKEND=${ENABLE_MLIR}
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
