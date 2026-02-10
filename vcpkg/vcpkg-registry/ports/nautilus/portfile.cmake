# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(NAUTILUS_PATCHES "0001-disable-ubsan-function-call-check.patch")

if("debug" IN_LIST FEATURES)
    list(APPEND NAUTILUS_PATCHES
        "timing.patch"
        "logging.patch"
        "find_forks.patch"
    )
endif()

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO nebulastream/nautilus
		REF 6286b5d80e5b60ac84f370bdd80f8201c6a51309
		SHA512 dab7e426dd055472ceade4fb57dcea9a6063baaabc000c7e575f1b688e77406ba5e6d5b4d8e4c5426bcbc38f01315f5546b46e29f3b24c887c2bd58b5396021b
		PATCHES ${NAUTILUS_PATCHES}
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
		-DENABLE_LOGGING=${ENABLE_LOGGING_FLAG}
		${ADDITIONAL_CMAKE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/nautilus")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
