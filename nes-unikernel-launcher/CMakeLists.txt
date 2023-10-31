add_executable(UnikernelLauncher src/main.cpp)

find_package(Argumentum REQUIRED)
find_package(yaml-cpp REQUIRED)
add_dependencies(UnikernelLauncher unikernel-sink unikernel-source)
target_link_libraries(UnikernelLauncher PRIVATE
        yaml-cpp
        UnikernelCommon
        argumentum)
