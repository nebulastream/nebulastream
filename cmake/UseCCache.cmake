option(USE_CCACHE_IF_AVAILABLE "Use ccache to speed up rebuilds" ON)
find_program(CCACHE_EXECUTABLE ccache)
if (DEFINED CCACHE_PROGRAM AND ${USE_CCACHE_IF_AVAILABLE})
    message(STATUS "Using ccache: ${CCACHE_EXECUTABLE}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")

    if (NES_ENABLE_PRECOMPILED_HEADERS)
        set(CMAKE_PCH_INSTANTIATE_TEMPLATES ON)
        # Need to set these to enable interplay between ccache and precompiled headers
        # https://ccache.dev/manual/4.8.html#_precompiled_headers
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fno-pch-timestamp")
        set(ENV{CCACHE_SLOPPINESS} "pch_defines,time_macros,include_file_ctime,include_file_mtime")
        message("CCACHE_SLOPPINESS: $ENV{CCACHE_SLOPPINESS}")
    endif ()

else ()
    message(STATUS "Not Using ccache")
endif ()