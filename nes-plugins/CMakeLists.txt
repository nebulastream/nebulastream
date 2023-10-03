

set(NES_PLUGINS_BUILD_PATH "${CMAKE_BINARY_DIR}/nes-plugins")
set(NES_PLUGINS_INSTALL_PATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/nes-plugins")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/PathConfig.h.in ${CMAKE_CURRENT_BINARY_DIR}/PathConfig.h)

target_include_directories(nes-common PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_BUILD_PLUGIN_ARROW)
    add_subdirectory(arrow)
endif ()

if (NES_BUILD_PLUGIN_TENSORFLOW)
    add_subdirectory(tensorflow)
endif ()

if (NES_BUILD_PLUGIN_ONNX)
    add_subdirectory(onnx)
endif ()