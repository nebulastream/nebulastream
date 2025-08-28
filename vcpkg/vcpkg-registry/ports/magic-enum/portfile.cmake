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
        REPO Neargye/magic_enum
		REF d468f23408f1207a4c63b370bb34e4cfabffad83
        SHA512 69b8779c7987490ec7b277cfacb6afaac293ef4e291b835ed1aeba515a4ba98ab09c883f07993b0644672f34639e816e9fed5313f97135e1678536d4a00c62af
		PATCHES
		0001-disable-magic-enum-value.patch
)

vcpkg_cmake_configure(
		SOURCE_PATH "${SOURCE_PATH}"
		OPTIONS
		${FEATURE_OPTIONS}
		-DMAGIC_ENUM_OPT_BUILD_TESTS=OFF
		-DMAGIC_ENUM_OPT_BUILD_EXAMPLES=OFF
)

vcpkg_cmake_install()


file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/lib")
file(INSTALL ${SOURCE_PATH}/LICENSE DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
