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
        REPO ERGO-Code/HiGHS
        REF "v${VERSION}"
        SHA512 a4c3827e76f264c7fd155ce945d35b421027bf668cdee16088b7217e616f0bef739bd9ecf61db7b77b52c8af56ab0429f9aff5baf4e0fa36dbc2d7682ef4b21f
        HEAD_REF master
        PATCHES
        0001-override-cxx-standard.patch
        0002-fix-primal-dual-integral-offset-inconsistency.patch
)

vcpkg_cmake_configure(
        SOURCE_PATH "${SOURCE_PATH}"
        OPTIONS
        -DFAST_BUILD=ON
        -DBUILD_TESTING=OFF
        -DBUILD_EXAMPLES=OFF
        -DBUILD_CXX_EXE=OFF
        -DCMAKE_REQUIRE_FIND_PACKAGE_ZLIB=ON
)

vcpkg_cmake_install()
vcpkg_fixup_pkgconfig()

vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/highs")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE.txt")

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
