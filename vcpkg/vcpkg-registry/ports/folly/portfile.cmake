# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## On macOS (case-insensitive filesystem), git apply cannot handle the patch because
## it renames CMake/ to cmake/ (case-only directory change). git apply checks all hunks
## upfront and sees the new cmake/ paths as "already existing" at the case-equivalent
## CMake/ paths. We use `patch -p1` on macOS instead, which applies hunks sequentially.
if(VCPKG_TARGET_IS_OSX)
    set(_FOLLY_PATCHES "")
else()
    set(_FOLLY_PATCHES PATCHES 0001-Folly-Patch.patch)
endif()

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO facebook/folly
        REF c47d0c778950043cbbc6af7fde616e9aeaf054ca
        SHA512 d74db09cfc1407a16a5b77b2911a7e599c3dbc477c15173e4635b0721e496a10a8d5eaf6b045b28d4e56163a694f66a63bf0a107420193d27f18ff1068136e53
        ${_FOLLY_PATCHES}
)

if(VCPKG_TARGET_IS_OSX)
    vcpkg_execute_required_process(
        COMMAND patch -p1 -f -E -i "${CMAKE_CURRENT_LIST_DIR}/0001-Folly-Patch.patch"
        WORKING_DIRECTORY "${SOURCE_PATH}"
        LOGNAME patch-folly-macos
    )
endif()

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
