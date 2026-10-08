# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build tool only: codon-compiler runs peg2cpp while it is being built, so when cross compiling it has to be a host
# executable (this port is a host dependency of codon-compiler). Same source as codon-compiler.
vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO exaloop/codon
        REF f1cd7006ad659a80852fd6de25885a0c639c94dc
        SHA512 89feb7ce0530252c1202a425c98d3bac2ece2ce34abb8326da424ed93e790430ba15cc0ef7d7f3a7f174796afa3576bcb8ff265ec0d75bd91dc006134b0fd2c1
        HEAD_REF develop
)

set(VCPKG_BUILD_TYPE release)
set(VCPKG_POLICY_EMPTY_INCLUDE_FOLDER enabled)

vcpkg_cmake_configure(
        SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}"
        OPTIONS -DCODON_SOURCE_PATH=${SOURCE_PATH}
)
vcpkg_cmake_install()

file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
