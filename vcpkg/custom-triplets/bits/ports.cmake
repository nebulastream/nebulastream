# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Overrides run from least to most specific, after all triplet defaults.
# Register only existing, applicable files in the current port's ABI hash.
list(APPEND VCPKG_HASH_ADDITIONAL_FILES "${CMAKE_CURRENT_LIST_FILE}")

set(_NES_PORT_OVERRIDES
    "${PORT}"
    "${PORT}-${VCPKG_TARGET_ARCHITECTURE}")
if (NES_VCPKG_SANITIZER)
    list(APPEND _NES_PORT_OVERRIDES
        "${PORT}-${NES_VCPKG_SANITIZER}"
        "${PORT}-${VCPKG_TARGET_ARCHITECTURE}-${NES_VCPKG_SANITIZER}")
endif ()

foreach (_NES_PORT_OVERRIDE IN LISTS _NES_PORT_OVERRIDES)
    set(_NES_PORT_OVERRIDE_FILE "${CMAKE_CURRENT_LIST_DIR}/ports/${_NES_PORT_OVERRIDE}.cmake")
    if (EXISTS "${_NES_PORT_OVERRIDE_FILE}")
        list(APPEND VCPKG_HASH_ADDITIONAL_FILES "${_NES_PORT_OVERRIDE_FILE}")
        include("${_NES_PORT_OVERRIDE_FILE}")
    endif ()
endforeach ()

unset(_NES_PORT_OVERRIDES)
unset(_NES_PORT_OVERRIDE)
unset(_NES_PORT_OVERRIDE_FILE)
