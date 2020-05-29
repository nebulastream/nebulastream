
find_package(Doxygen)
if (NOT ${DOXYGEN_FOUND})
        message(WARNING "Doxygen not found.")
        return ()
endif ()

if (${DOXYGEN_DOT_FOUND})
        set(DOXYGEN_HAVE_DOT YES)
else ()
        set(DOXYGEN_HAVE_DOT NO)
endif ()

execute_process(COMMAND ${DOXYGEN_EXECUTABLE} --version
                OUTPUT_VARIABLE DOXYGEN_VERSION
                OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Doxygen version: ${DOXYGEN_VERSION}")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
               "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
               ESCAPE_QUOTES)

add_custom_target(nes-doc ${DOXYGEN_EXECUTABLE} "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
                  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile")


