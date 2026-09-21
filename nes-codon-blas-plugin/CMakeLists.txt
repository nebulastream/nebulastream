# Licensed under the Apache License, Version 2.0 (the "License").
include("${PROJECT_SOURCE_DIR}/nes-codon-plugin/cmake/NESCodonPlugin.cmake")
find_package(OpenBLAS CONFIG REQUIRED)

add_codon_extension_plugin(nes-codon-blas-plugin
    PLUGIN_NAME blas
    NATIVE_SYMBOLS cblas_sdot
    NATIVE_SYMBOL_HEADERS cblas.h
    LINK_LIBRARIES OpenBLAS::OpenBLAS)
