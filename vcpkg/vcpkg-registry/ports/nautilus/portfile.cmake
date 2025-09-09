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
		REF e72dabf14b2a6e110e5fe0c8845cd964623dbdd4
        SHA512 74bbae26bbde69afbbdc12b7573cb3e7188a150e7b6a04dc6c6cc0f204a415cad8ca4e8b369bfee811a9c5119dd7c623de6ed9906b68e01da3184b170c9c401f
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
		${ADDITIONAL_CMAKE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/nautilus")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
