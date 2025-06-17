# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
		0006-fix-wrong-default-constructor.patch
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
