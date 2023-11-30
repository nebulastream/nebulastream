find_package(Argumentum REQUIRED)

add_executable(unikernel-sink src/main.cpp src/Options.cpp)

target_include_directories(unikernel-sink PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_link_libraries(unikernel-sink PUBLIC UnikernelTest UnikernelCommon Argumentum::argumentum)
