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
        REPO exaloop/cpp-peglib
        REF c036fdd5289dabf6b5516b463e3eb51f339c9604
        SHA512 d182659b8d1e6d28383c1a4ee32858c3d1ab8563a1e26eccbdcf85c80469d6fdf9115472b96acec8956ce782582daf7da967e365159dccd3e26f18ca4c709348
        HEAD_REF codon
        PATCHES
        0001-support-builds-without-rtti.patch
)

file(INSTALL "${SOURCE_PATH}/peglib.h" DESTINATION "${CURRENT_PACKAGES_DIR}/include")
file(INSTALL "${SOURCE_PATH}/LICENSE" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
