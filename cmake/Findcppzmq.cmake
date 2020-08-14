# - Finds zmq.hpp
#
# A CMake variable or environment variable named CPPZMQ_DIR may be used to
# provide an explicit location. (If both are set, the CMake variable takes
# precedence.)
#
# The following variables are set if zmq.hpp is found:
#
#   CPPZMQ_FOUND        - Set to TRUE
#   CPPZMQ_LIBRARIES    - The name of an INTERFACE IMPORTED target.
#   CPPZMQ_INCLUDE_DIRS - The directory which contains zmq.hpp.  This is also
#                         added as a dependency to the INTERFACE IMPORTED
#                         target.
#
# If zmq.hpp is not found, CPPZMQ_FOUND is set to false.
cmake_minimum_required (VERSION 3.0.0)
set (CPPZMQ_DIR "${CPPZMQ_DIR}" CACHE PATH "The directory that contains zmq.hpp")

if (NOT ZeroMQ_FOUND)
  find_package (ZeroMQ REQUIRED)
endif ()

find_path(CPPZMQ_INCLUDE_DIRS "zmq.hpp"
        HINTS "${ZeroMQ_INCLUDE_DIR}"
        PATHS "${CPPZMQ_DIR}" "$ENV{CPPZMQ_DIR}"
        PATH_SUFFIXES "include" "cppzmq"
        )
mark_as_advanced (CPPZMQ_INCLUDE_DIRS)

if (CPPZMQ_INCLUDE_DIRS)
  add_library ("cppzmq" INTERFACE IMPORTED)
  set_target_properties("cppzmq" PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${CPPZMQ_INCLUDE_DIRS}"
          INTERFACE_LINK_LIBRARIES "libzmq"
          )
  set (CPPZMQ_LIBRARIES "cppzmq")
endif ()

# Standard find_package stuff.
include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (CPPZMQ DEFAULT_MSG CPPZMQ_LIBRARIES CPPZMQ_INCLUDE_DIRS)