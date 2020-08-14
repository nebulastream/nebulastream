# - Finds ZeroMQ 3.x or 4.x
#
# This script is run by the command find_package(ZeroMQ), and will attempt to
# locate installed ZeroMQ libraries and header files. A CMake variable or
# environment variable named ZeroMQ_DIR may be used to provide an explicit
# location. (If both are set, the CMake variable takes precedence.)
#
# The following variables are set if ZeroMQ is found:
#
#   ZeroMQ_FOUND           - Set to TRUE
#   ZeroMQ_LIBRARIES       - The name of an imported target that refers to the
#                            ZeroMQ libraries.
#   ZeroMQ_INCLUDE_DIRS    - The ZeroMQ include directory (which contains zmq.h).
#                            This is also added as a dependency to the imported
#                            target.
#   ZeroMQ_VERSION_STRING  - The version of ZeroMQ which was found
#
# If ZeroMQ is not found, ZeroMQ_FOUND is set to false.

cmake_minimum_required (VERSION 2.8.11)

set (ZeroMQ_DIR "${ZeroMQ_DIR}" CACHE PATH
        "The directory where ZeroMQ is installed, i.e., the parent directory of the 'bin', 'lib' and 'include' directories that contain the ZeroMQ libraries and headers."
        )

# A function which prefixes all items in a list with a string.
function (_prefixStrings targetVarName prefix)
  set (v)
  foreach (s ${ARGN})
    list (APPEND v "${prefix}${s}")
  endforeach ()
  set (${targetVarName} ${v} PARENT_SCOPE)
endfunction ()

# Generates a HINTS directive for a find_XXX command, based on
# a known file path.
function (_getHintsDirective targetVarName knownPath)
  if (knownPath)
    get_filename_component (d "${knownPath}" DIRECTORY)
    set (${targetVarName} "HINTS" "${d}/.." PARENT_SCOPE)
  else ()
    set (${targetVarName} "" PARENT_SCOPE)
  endif ()
endfunction ()


# The function which searches for the DLLs, their corresponding import
# libraries, and include files on Windows.
function (_findWinLibs releaseDll releaseLib debugDll debugLib includeDir)
  set (canFind TRUE)

  # Extra search paths (typically "C:/Program Files/ZeroMQ ...")
  set (extraPrefixPaths)
  foreach (p ${CMAKE_SYSTEM_PREFIX_PATH})
    file (GLOB d "${p}/zeromq*" "${p}/zmq*")
    list (APPEND extraPrefixPaths ${d})
  endforeach ()

  # Supported compilers
  if (MSVC_VERSION EQUAL 1500)
    set (compiler "v90")
  elseif (MSVC_VERSION EQUAL 1600)
    set (compiler "v100")
  elseif (MSVC_VERSION EQUAL 1700)
    set (compiler "v110")
  elseif (MSVC_VERSION EQUAL 1800)
    set (compiler "v120")
  elseif (MSVC_VERSION EQUAL 1900)
    set (compiler "v140")
  elseif ((MSVC_VERSION GREATER 1909) AND (MSVC_VERSION LESS 1920))
    set (compiler "v141")
  else ()
    set (compiler "NOTFOUND")
    set (canFind FALSE)
    message (WARNING "Compiler version not supported by the FindZeroMQ module")
  endif ()

  # If no version is requested, start at 3.0.0.
  if (NOT ZeroMQ_FIND_VERSION)
    set (ZeroMQ_FIND_VERSION_MAJOR 3)
    set (ZeroMQ_FIND_VERSION_MINOR 0)
    set (ZeroMQ_FIND_VERSION_PATCH 0)
  endif ()

  # Make a list of possible version names, starting with the one we are
  # asked to search for.
  set (versionNames)
  if (ZeroMQ_FIND_VERSION_MAJOR EQUAL 3)
    foreach (m RANGE ${ZeroMQ_FIND_VERSION_MINOR} 2)
      set (pMin 0)
      if (m EQUAL ZeroMQ_FIND_VERSION_MINOR)
        set (pMin ${ZeroMQ_FIND_VERSION_PATCH})
      endif ()
      foreach (p RANGE ${pMin} 5)
        list (APPEND versionNames "3_${m}_${p}")
      endforeach()
    endforeach ()
  endif ()
  set (mMin 0)
  if (ZeroMQ_FIND_VERSION_MAJOR EQUAL 4)
    set (mMin ${ZeroMQ_FIND_VERSION_MINOR})
  endif ()
  foreach (m RANGE ${mMin} 4)
    set (pMin 0)
    if ((ZeroMQ_FIND_VERSION_MAJOR EQUAL 4) AND (m EQUAL ZeroMQ_FIND_VERSION_MINOR))
      set (pMin ${ZeroMQ_FIND_VERSION_PATCH})
    endif ()
    foreach (p RANGE ${pMin} 9)
      list (APPEND versionNames "4_${m}_${p}")
    endforeach()
  endforeach ()

  # Generate possible library file names
  if ((ZeroMQ_FIND_VERSION_MAJOR GREATER 2) AND (ZeroMQ_FIND_VERSION_MINOR LESS 5))
    _prefixStrings(releaseLibNames "libzmq-${compiler}-mt-"    ${versionNames})
    _prefixStrings(debugLibNames   "libzmq-${compiler}-mt-gd-" ${versionNames})
  else ()
    set (canFind FALSE)
    message (WARNING "ZeroMQ version not supported by the FindZeroMQ module: ${ZeroMQ_FIND_VERSION}")
  endif ()

  if (canFind)
    message (STATUS "Searching for ZeroMQ files. This may take a while...")
    set (CMAKE_FIND_LIBRARY_SUFFIXES ".lib")
    find_library (ZeroMQ_RELEASE_LIB
            NAMES ${releaseLibNames}
            PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR} ${extraPrefixPaths}
            PATH_SUFFIXES "lib")
    _getHintsDirective(hints ${ZeroMQ_RELEASE_LIB})
    find_library (ZeroMQ_DEBUG_LIB
            NAMES ${debugLibNames}
            ${hints}
            PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR} ${extraPrefixPaths}
            PATH_SUFFIXES "lib")
    set (CMAKE_FIND_LIBRARY_SUFFIXES ".dll")
    find_library (ZeroMQ_RELEASE_DLL
            NAMES ${releaseLibNames}
            ${hints}
            PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR} ${extraPrefixPaths}
            PATH_SUFFIXES "bin" "lib")
    find_library (ZeroMQ_DEBUG_DLL
            NAMES ${debugLibNames}
            ${hints}
            PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR} ${extraPrefixPaths}
            PATH_SUFFIXES "bin" "lib")
    find_path (ZeroMQ_HEADER_DIR "zmq.h"
            ${_hints}
            PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR} ${extraPrefixPaths}
            PATH_SUFFIXES "include")
    mark_as_advanced (
            ZeroMQ_RELEASE_LIB ZeroMQ_DEBUG_LIB
            ZeroMQ_RELEASE_DLL ZeroMQ_DEBUG_DLL
            ZeroMQ_HEADER_DIR
    )

    set (${releaseDll} "${ZeroMQ_RELEASE_DLL}" PARENT_SCOPE)
    set (${releaseLib} "${ZeroMQ_RELEASE_LIB}" PARENT_SCOPE)
    set (${debugDll}   "${ZeroMQ_DEBUG_DLL}"   PARENT_SCOPE)
    set (${debugLib}   "${ZeroMQ_DEBUG_LIB}"   PARENT_SCOPE)
    set (${includeDir} "${ZeroMQ_HEADER_DIR}"  PARENT_SCOPE)
  else ()
    set (${releaseDll} "NOTFOUND" PARENT_SCOPE)
    set (${releaseLib} "NOTFOUND" PARENT_SCOPE)
    set (${debugDll}   "NOTFOUND" PARENT_SCOPE)
    set (${debugLib}   "NOTFOUND" PARENT_SCOPE)
    set (${includeDir} "NOTFOUND" PARENT_SCOPE)
  endif ()
endfunction ()

# The function which searches for the libraries and headers on *NIX.
function (_findUnixLibs library includeDir)
  find_library (ZeroMQ_LIBRARY "zmq"
          PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR}
          PATH_SUFFIXES "lib")
  _getHintsDirective(hints ${ZeroMQ_LIBRARY})
  find_path (ZeroMQ_HEADER_DIR "zmq.h"
          ${hints}
          PATHS ${ZeroMQ_DIR} $ENV{ZeroMQ_DIR}
          PATH_SUFFIXES "include")
  mark_as_advanced (ZeroMQ_LIBRARY ZeroMQ_HEADER_DIR)
  set (${library} "${ZeroMQ_LIBRARY}" PARENT_SCOPE)
  set (${includeDir} "${ZeroMQ_HEADER_DIR}"  PARENT_SCOPE)
endfunction ()

# A function which extracts the ZeroMQ version from macro definitions in zmq.h.
function (_getZeroMQVersion targetVarName includeDir)
  file (STRINGS "${includeDir}/zmq.h" versionDefines REGEX "define +ZeroMQ_VERSION_")
  if (versionDefines)
    string (REGEX REPLACE
            ".*MAJOR ([0-9]+).*MINOR ([0-9]+).*PATCH ([0-9]+).*"
            "\\1.\\2.\\3" versionString "${versionDefines}")
  endif ()
  set (${targetVarName} "${versionString}" PARENT_SCOPE)
endfunction ()


unset (ZeroMQ_LIBRARIES)
unset (ZeroMQ_INCLUDE_DIRS)
unset (ZeroMQ_VERSION_STRING)

# Try to find the ZeroMQ-supplied config file first (only works for v4.2.2 upwards)
find_package (ZeroMQ QUIET CONFIG)
if (ZeroMQ_FOUND)
  set (ZeroMQ_LIBRARIES ${ZeroMQ_LIBRARY})
  set (ZeroMQ_INCLUDE_DIRS ${ZeroMQ_INCLUDE_DIR})
else ()
  if (WIN32)
    _findWinLibs(_releaseLocation _releaseImplib _debugLocation _debugImplib ZeroMQ_INCLUDE_DIRS)
    set (_releaseLinkLib _releaseImplib)
    set (_debugLinkLib _debugImplib)
  else ()
    _findUnixLibs(_releaseLocation ZeroMQ_INCLUDE_DIRS)
    unset (_releaseImplib)
    unset (_debugLocation)
    unset (_debugImplib)
    set (_releaseLinkLib _releaseLocation)
    unset (_debugLinkLib)
  endif ()

  if (_releaseLinkLib OR _debugLinkLib)
    add_library ("libzmq" SHARED IMPORTED)
    set (ZeroMQ_LIBRARIES "libzmq")
    set_target_properties ("libzmq" PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${ZeroMQ_INCLUDE_DIRS}"
            )
    if (_releaseLinkLib)
      set_property (TARGET "libzmq" APPEND PROPERTY IMPORTED_CONFIGURATIONS "RELEASE")
      set_property (TARGET "libzmq" PROPERTY IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C")
      if (_releaseLocation)
        set_property (TARGET "libzmq" PROPERTY IMPORTED_LOCATION_RELEASE "${_releaseLocation}")
      endif ()
      if (_releaseImplib)
        set_property (TARGET "libzmq" PROPERTY IMPORTED_IMPLIB_RELEASE "${_releaseImplib}")
      endif ()
    endif ()
    if (_debugLinkLib)
      set_property (TARGET "libzmq" APPEND PROPERTY IMPORTED_CONFIGURATIONS "DEBUG")
      set_property (TARGET "libzmq" PROPERTY IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C")
      if (_debugLocation)
        set_property (TARGET "libzmq" PROPERTY IMPORTED_LOCATION_DEBUG "${_debugLocation}")
      endif ()
      if (_debugImplib)
        set_property (TARGET "libzmq" PROPERTY IMPORTED_IMPLIB_DEBUG "${_debugImplib}")
      endif ()
    endif ()
  endif ()
endif()

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (ZeroMQ
        FOUND_VAR ZeroMQ_FOUND
        REQUIRED_VARS ZeroMQ_LIBRARIES ZeroMQ_INCLUDE_DIRS
        )