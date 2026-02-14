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
		#		REF 13ec1d010a08a825cd77c4073950b33f2c816ea3
		#		SHA512 7fcb5e6977a605d106591b90a528069ee4850e507aad93070f443c31e36926550487ce9f9af33f23b373d295ae8d1b8df2063f05219bd08dce555c9d26c94aeb
		REF 2e856d08ea32f16c80681205551c96e31f49d4c7
		SHA512 a5527beb9eecc761b56cf47e75c4eafbbdca3bb75771a975b9a24ba8d8d52341b36191ec59eb01f1fa3be98b737f54bfc3f1530e052b481a460c777ba5d324d8
		PATCHES
		0001-disable-ubsan-function-call-check.patch
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
		-DENABLE_LOGGING=OFF
		${ADDITIONAL_CMAKE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/nautilus")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
