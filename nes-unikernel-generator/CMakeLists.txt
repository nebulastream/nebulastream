find_package(Argumentum REQUIRED)
find_package(yaml-cpp REQUIRED)

add_executable(NesUnikernelGenerator src/main.cpp)
target_link_libraries(NesUnikernelGenerator PRIVATE
        yaml-cpp
        UnikernelCommon
        argumentum)