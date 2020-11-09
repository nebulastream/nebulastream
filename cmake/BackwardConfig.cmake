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

include(GNUInstallDirs)
include(FindPackageHandleStandardArgs)

# find libdw
find_path(LIBDW_INCLUDE_DIR NAMES "elfutils/libdw.h" "elfutils/libdwfl.h")
find_library(LIBDW_LIBRARY dw)
set(LIBDW_INCLUDE_DIRS ${LIBDW_INCLUDE_DIR} )
set(LIBDW_LIBRARIES ${LIBDW_LIBRARY} )
find_package_handle_standard_args(libdw DEFAULT_MSG
        LIBDW_LIBRARY LIBDW_INCLUDE_DIR)
mark_as_advanced(LIBDW_INCLUDE_DIR LIBDW_LIBRARY)

# find libbfd
find_path(LIBBFD_INCLUDE_DIR NAMES "bfd.h")
find_path(LIBDL_INCLUDE_DIR NAMES "dlfcn.h")
find_library(LIBBFD_LIBRARY bfd)
find_library(LIBDL_LIBRARY dl)
set(LIBBFD_INCLUDE_DIRS ${LIBBFD_INCLUDE_DIR} ${LIBDL_INCLUDE_DIR})
set(LIBBFD_LIBRARIES ${LIBBFD_LIBRARY} ${LIBDL_LIBRARY})
find_package_handle_standard_args(libbfd DEFAULT_MSG
        LIBBFD_LIBRARY LIBBFD_INCLUDE_DIR
        LIBDL_LIBRARY LIBDL_INCLUDE_DIR)
mark_as_advanced(LIBBFD_INCLUDE_DIR LIBBFD_LIBRARY
        LIBDL_INCLUDE_DIR LIBDL_LIBRARY)

# find libdwarf
find_path(LIBDWARF_INCLUDE_DIR NAMES "libdwarf.h" PATH_SUFFIXES libdwarf)
find_path(LIBELF_INCLUDE_DIR NAMES "libelf.h")
find_path(LIBDL_INCLUDE_DIR NAMES "dlfcn.h")
find_library(LIBDWARF_LIBRARY dwarf)
find_library(LIBELF_LIBRARY elf)
find_library(LIBDL_LIBRARY dl)
set(LIBDWARF_INCLUDE_DIRS ${LIBDWARF_INCLUDE_DIR} ${LIBELF_INCLUDE_DIR} ${LIBDL_INCLUDE_DIR})
set(LIBDWARF_LIBRARIES ${LIBDWARF_LIBRARY} ${LIBELF_LIBRARY} ${LIBDL_LIBRARY})
find_package_handle_standard_args(libdwarf DEFAULT_MSG
        LIBDWARF_LIBRARY LIBDWARF_INCLUDE_DIR
        LIBELF_LIBRARY LIBELF_INCLUDE_DIR
        LIBDL_LIBRARY LIBDL_INCLUDE_DIR)
mark_as_advanced(LIBDWARF_INCLUDE_DIR LIBDWARF_LIBRARY
        LIBELF_INCLUDE_DIR LIBELF_LIBRARY
        LIBDL_INCLUDE_DIR LIBDL_LIBRARY)


if (LIBDW_FOUND)
    LIST(APPEND _BACKWARD_INCLUDE_DIRS ${LIBDW_INCLUDE_DIRS})
    LIST(APPEND _BACKWARD_LIBRARIES ${LIBDW_LIBRARIES})
    set(STACK_DETAILS_DW TRUE)
    set(STACK_DETAILS_BFD FALSE)
    set(STACK_DETAILS_DWARF FALSE)
    set(STACK_DETAILS_BACKTRACE_SYMBOL FALSE)
elseif(LIBBFD_FOUND)
    LIST(APPEND _BACKWARD_INCLUDE_DIRS ${LIBBFD_INCLUDE_DIRS})
    LIST(APPEND _BACKWARD_LIBRARIES ${LIBBFD_LIBRARIES})

    # If we attempt to link against static bfd, make sure to link its dependencies, too
    get_filename_component(bfd_lib_ext "${LIBBFD_LIBRARY}" EXT)
    if (bfd_lib_ext STREQUAL "${CMAKE_STATIC_LIBRARY_SUFFIX}")
        list(APPEND _BACKWARD_LIBRARIES iberty z)
    endif()

    set(STACK_DETAILS_DW FALSE)
    set(STACK_DETAILS_BFD TRUE)
    set(STACK_DETAILS_DWARF FALSE)
    set(STACK_DETAILS_BACKTRACE_SYMBOL FALSE)
elseif(LIBDWARF_FOUND)
    LIST(APPEND _BACKWARD_INCLUDE_DIRS ${LIBDWARF_INCLUDE_DIRS})
    LIST(APPEND _BACKWARD_LIBRARIES ${LIBDWARF_LIBRARIES})

    set(STACK_DETAILS_DW FALSE)
    set(STACK_DETAILS_BFD FALSE)
    set(STACK_DETAILS_DWARF TRUE)
    set(STACK_DETAILS_BACKTRACE_SYMBOL FALSE)
else()
    set(STACK_DETAILS_DW FALSE)
    set(STACK_DETAILS_BFD FALSE)
    set(STACK_DETAILS_DWARF FALSE)
    set(STACK_DETAILS_BACKTRACE_SYMBOL TRUE)
endif()

macro(map_definitions var_prefix define_prefix)
    foreach(def ${ARGN})
        if (${${var_prefix}${def}})
            LIST(APPEND _BACKWARD_DEFINITIONS "${define_prefix}${def}=1")
        else()
            LIST(APPEND _BACKWARD_DEFINITIONS "${define_prefix}${def}=0")
        endif()
    endforeach()
endmacro()

if (NOT _BACKWARD_DEFINITIONS)
    map_definitions("STACK_WALKING_" "BACKWARD_HAS_" UNWIND BACKTRACE)
    map_definitions("STACK_DETAILS_" "BACKWARD_HAS_" BACKTRACE_SYMBOL DW BFD DWARF)
endif()

set(BACKWARD_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}")

set(BACKWARD_HAS_EXTERNAL_LIBRARIES FALSE)
set(FIND_PACKAGE_REQUIRED_VARS BACKWARD_INCLUDE_DIR)
if(DEFINED _BACKWARD_LIBRARIES)
    set(BACKWARD_HAS_EXTERNAL_LIBRARIES TRUE)
    list(APPEND FIND_PACKAGE_REQUIRED_VARS _BACKWARD_LIBRARIES)
endif()

list(APPEND _BACKWARD_INCLUDE_DIRS ${BACKWARD_INCLUDE_DIR})

macro(add_backward target)
    target_include_directories(${target} PRIVATE ${BACKWARD_INCLUDE_DIRS})
    set_property(TARGET ${target} APPEND PROPERTY COMPILE_DEFINITIONS ${BACKWARD_DEFINITIONS})
    set_property(TARGET ${target} APPEND PROPERTY LINK_LIBRARIES ${BACKWARD_LIBRARIES})
endmacro()

set(BACKWARD_INCLUDE_DIRS ${_BACKWARD_INCLUDE_DIRS} CACHE INTERNAL "_BACKWARD_INCLUDE_DIRS")
set(BACKWARD_DEFINITIONS ${_BACKWARD_DEFINITIONS} CACHE INTERNAL "BACKWARD_DEFINITIONS")
set(BACKWARD_LIBRARIES ${_BACKWARD_LIBRARIES} CACHE INTERNAL "BACKWARD_LIBRARIES")
mark_as_advanced(BACKWARD_INCLUDE_DIRS BACKWARD_DEFINITIONS BACKWARD_LIBRARIES)

# Expand each definition in BACKWARD_DEFINITIONS to its own cmake var and export
# to outer scope
foreach(var ${BACKWARD_DEFINITIONS})
    string(REPLACE "=" ";" var_as_list ${var})
    list(GET var_as_list 0 var_name)
    list(GET var_as_list 1 var_value)
    set(${var_name} ${var_value})
    mark_as_advanced(${var_name})
endforeach()

message(STATUS "Backward: ${BACKWARD_DEFINITIONS} ${BACKWARD_INCLUDE_DIRS} ${BACKWARD_LIBRARIES}")