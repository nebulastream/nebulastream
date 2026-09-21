# Licensed under the Apache License, Version 2.0 (the "License").
include("${PROJECT_SOURCE_DIR}/nes-codon-plugin/cmake/NESCodonPlugin.cmake")
find_package(OpenCV CONFIG REQUIRED)

add_codon_extension_plugin(nes-codon-opencv-plugin
    PLUGIN_NAME opencv
    CODON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/codon"
    ADDITIONAL_SOURCES src/OpenCv.cpp
    NATIVE_SYMBOLS
        nes_cv_imencode
        nes_cv_cvt_color
        nes_cv_apply_color_map
        nes_cv_resize
        nes_cv_imdecode
        nes_cv_warp_affine
        nes_cv_normalize
        nes_cv_rectangle
    NATIVE_SYMBOL_HEADERS OpenCv.hpp
    LINK_LIBRARIES opencv_core opencv_imgcodecs opencv_imgproc)
target_include_directories(nes-codon-opencv-plugin PRIVATE src)
