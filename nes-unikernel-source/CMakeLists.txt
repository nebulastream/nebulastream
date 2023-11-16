find_package(Argumentum REQUIRED)
find_package(yaml-cpp REQUIRED)

add_executable(unikernel-source src/main.cpp src/CSVDataGenerator.cpp src/AutomaticDataGenerator.cpp src/Options.cpp)

target_include_directories(unikernel-source PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_link_libraries(unikernel-source PUBLIC Argumentum::argumentum yaml-cpp UnikernelCommon UnikernelTest)
