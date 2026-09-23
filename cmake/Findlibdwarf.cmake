# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Find module for libdwarf.
#
# cpptrace's installed CMake config calls `find_dependency(libdwarf REQUIRED)`, but some
# distributions (notably nixpkgs) ship libdwarf with a pkg-config file and no CMake config
# package, so that call fails in config mode and configuration aborts. find_package() tries
# MODULE mode first, and CMAKE_MODULE_PATH starts with this directory, so this shim answers
# the query before config mode is ever reached.
#
# Because MODULE mode wins unconditionally, this shim must NOT shadow a real libdwarf CONFIG
# package where one exists (e.g. the vcpkg-built libdwarf that cpptrace was compiled against):
# it first delegates to config mode and returns immediately when that succeeds, so it only
# falls back to pkg-config/find_library discovery on distributions (nixpkgs) that ship no
# CONFIG package. Discovery and version detection assume a libdwarf pkg-config file is present.
#
# Defines: libdwarf_FOUND, libdwarf_INCLUDE_DIRS, libdwarf_LIBRARIES, and the imported
# target libdwarf::dwarf.
#
# `libdwarf::dwarf` is canonical here: it is the target name cpptrace itself links against on
# its vcpkg build path (`CPPTRACE_VCPKG` -> `target_link_libraries(... libdwarf::dwarf)`) and the
# name the real upstream libdwarf CMake config exports (`libdwarf::dwarf` / `libdwarf::dwarf-static`,
# NAMESPACE `libdwarf::`). This is the single source of truth for the libdwarf target name/shim
# behavior in this repo; `.nix/cpptrace/package.nix` mirrors it (see the comment
# there for why it can't just include this file), and flake.nix intentionally defines no shim of
# its own because CMAKE_MODULE_PATH prepends this directory (see CMakeLists.txt), so any nix-side
# copy for the main project's own configure would be shadowed by this one anyway.

# Prefer a real CONFIG package when the distribution ships one; the explicit CONFIG keyword
# goes straight to config mode and does not re-enter this module.
find_package(libdwarf CONFIG QUIET)
if (libdwarf_FOUND AND TARGET libdwarf::dwarf)
    return ()
endif ()

find_package(PkgConfig QUIET)
if (PkgConfig_FOUND)
    pkg_check_modules(PC_libdwarf QUIET libdwarf)
endif ()

find_path(libdwarf_INCLUDE_DIR
        NAMES libdwarf.h
        HINTS ${PC_libdwarf_INCLUDE_DIRS}
        PATH_SUFFIXES libdwarf-2 libdwarf)

find_library(libdwarf_LIBRARY
        NAMES dwarf libdwarf
        HINTS ${PC_libdwarf_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libdwarf
        REQUIRED_VARS libdwarf_LIBRARY libdwarf_INCLUDE_DIR
        VERSION_VAR PC_libdwarf_VERSION)

if (libdwarf_FOUND)
    set(libdwarf_INCLUDE_DIRS "${libdwarf_INCLUDE_DIR}")
    set(libdwarf_LIBRARIES "${libdwarf_LIBRARY}")
    if (NOT TARGET libdwarf::dwarf)
        add_library(libdwarf::dwarf UNKNOWN IMPORTED)
        set_target_properties(libdwarf::dwarf PROPERTIES
                IMPORTED_LOCATION "${libdwarf_LIBRARY}"
                INTERFACE_INCLUDE_DIRECTORIES "${libdwarf_INCLUDE_DIR}")
    endif ()
    # Defensive alias: some historical nix-side shims used `libdwarf::libdwarf` instead of the
    # canonical `libdwarf::dwarf`. Keep both names resolvable so a consumer
    # written against either one keeps working. Guarded on libdwarf::dwarf existing too, so this
    # can't ever try to alias a target that was not actually created above.
    if (TARGET libdwarf::dwarf AND NOT TARGET libdwarf::libdwarf)
        add_library(libdwarf::libdwarf ALIAS libdwarf::dwarf)
    endif ()
endif ()

mark_as_advanced(libdwarf_INCLUDE_DIR libdwarf_LIBRARY)
