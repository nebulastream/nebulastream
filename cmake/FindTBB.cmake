# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright (c) 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
#
#

include(FindPackageHandleStandardArgs)

if (NOT TBB_FIND_COMPONENTS)
    set(TBB_FIND_COMPONENTS tbb tbbmalloc tbbmalloc_proxy)
    foreach (_tbb_component ${TBB_FIND_COMPONENTS})
        set(TBB_FIND_REQUIRED_${_tbb_component} 1)
    endforeach()
endif()

# Add components with internal dependencies: tbbmalloc_proxy -> tbbmalloc
list(FIND TBB_FIND_COMPONENTS tbbmalloc_proxy _tbbmalloc_proxy_ix)
if (NOT _tbbmalloc_proxy_ix EQUAL -1)
    list(FIND TBB_FIND_COMPONENTS tbbmalloc _tbbmalloc_ix)
    if (_tbbmalloc_ix EQUAL -1)
        list(APPEND TBB_FIND_COMPONENTS tbbmalloc)
        set(TBB_FIND_REQUIRED_tbbmalloc ${TBB_FIND_REQUIRED_tbbmalloc_proxy})
    endif()
    unset(_tbbmalloc_ix)
endif()
unset(_tbbmalloc_proxy_ix)

find_path(_tbb_include_dir tbb/tbb.h)
if (_tbb_include_dir)
    file(READ "${_tbb_include_dir}/tbb/tbb_stddef.h" _tbb_stddef LIMIT 2048)
    string(REGEX REPLACE ".*#define TBB_VERSION_MAJOR ([0-9]+).*" "\\1" _tbb_ver_major "${_tbb_stddef}")
    string(REGEX REPLACE ".*#define TBB_VERSION_MINOR ([0-9]+).*" "\\1" _tbb_ver_minor "${_tbb_stddef}")
    string(REGEX REPLACE ".*#define TBB_INTERFACE_VERSION ([0-9]+).*" "\\1" TBB_INTERFACE_VERSION "${_tbb_stddef}")

    set(TBB_VERSION "${_tbb_ver_major}.${_tbb_ver_minor}")

    unset(_tbb_stddef)
    unset(_tbb_ver_major)
    unset(_tbb_ver_minor)

    foreach (_tbb_component ${TBB_FIND_COMPONENTS})
        find_library(_tbb_release_lib ${_tbb_component})
        if (_tbb_release_lib)
            set(TBB_${_tbb_component}_FOUND 1)

            add_library(TBB::${_tbb_component} SHARED IMPORTED)
            list(APPEND TBB_IMPORTED_TARGETS TBB::${_tbb_component})

            set(_tbb_lib_suffix)
            if (UNIX AND NOT APPLE)
                set(_tbb_lib_suffix ".2")
            endif()

            set_target_properties(TBB::${_tbb_component} PROPERTIES
                                  IMPORTED_CONFIGURATIONS       "RELEASE"
                                  IMPORTED_LOCATION_RELEASE     "${_tbb_release_lib}${_tbb_lib_suffix}"
                                  INTERFACE_INCLUDE_DIRECTORIES "${_tbb_include_dir}")

            # Add internal dependencies for imported targets: TBB::tbbmalloc_proxy -> TBB::tbbmalloc
            if (_tbb_component STREQUAL tbbmalloc_proxy)
                set_target_properties(TBB::tbbmalloc_proxy PROPERTIES INTERFACE_LINK_LIBRARIES TBB::tbbmalloc)
            endif()

            find_library(_tbb_debug_lib ${_tbb_component}_debug)
            if (_tbb_debug_lib)
                set_target_properties(TBB::${_tbb_component} PROPERTIES
                                      IMPORTED_CONFIGURATIONS "RELEASE;DEBUG"
                                      IMPORTED_LOCATION_DEBUG "${_tbb_debug_lib}${_tbb_lib_suffix}")
            endif()
            unset(_tbb_debug_lib CACHE)
            unset(_tbb_lib_suffix)
        endif()
        unset(_tbb_release_lib CACHE)
    endforeach()
endif()
unset(_tbb_include_dir CACHE)

find_package_handle_standard_args(TBB
                                  REQUIRED_VARS TBB_IMPORTED_TARGETS
                                  HANDLE_COMPONENTS
                                  VERSION_VAR TBB_VERSION)