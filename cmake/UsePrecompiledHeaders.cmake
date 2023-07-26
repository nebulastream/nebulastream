

if (NES_ENABLE_PRECOMPILED_HEADERS)
    set(CMAKE_PCH_INSTANTIATE_TEMPLATES ON)
    if (NES_USE_CCACHE)
        # Need to set these to enable interplay between ccache and precompiled headers
        # https://ccache.dev/manual/4.8.html#_precompiled_headers
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Xclang -fno-pch-timestamp -fpch-validate-input-files-content")
        set(ENV{CCACHE_SLOPPINESS} "pch_defines,time_macros")
        message(STATUS "Set CCACHE_SLOPPINESS: $ENV{CCACHE_SLOPPINESS}")
    endif ()
    message(STATUS "Using Precompiled Headers")
elseif()
    message(STATUS "Not using Precompiled Headers")
endif ()
