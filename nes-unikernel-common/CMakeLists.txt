find_package(yaml-cpp REQUIRED)
add_subdirectory(src)
set(BOOST_ROOT "${_VCPKG_INSTALLED_DIR}/x64-linux-nes")
find_package(Boost REQUIRED COMPONENTS system iostreams)
get_source(unikernel-common UNIKERNEL_COMMON_SRC)
add_library(UnikernelCommon STATIC ${UNIKERNEL_COMMON_SRC})
target_link_libraries(UnikernelCommon PUBLIC
        nes-common
        nes-data-types
        yaml-cpp
        Boost::iostreams
)
target_include_directories(UnikernelCommon PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
add_source_files(unikernel-test-lib
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/DefaultDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/YSBDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/ZipfianDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/Nextmark/NEBitDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/Nextmark/NEAuctionDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/DefaultDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/DataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/LightSaber/ClusterMonitoringDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/LightSaber/SmartGridDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/LightSaber/LinearRoadDataGenerator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/src/DataGeneration/LightSaber/ManufacturingEquipmentDataGenerator.cpp
)
get_source(unikernel-test-lib UNIKERNEL_TEST_SRC)
add_library(UnikernelTest SHARED ${UNIKERNEL_TEST_SRC})
target_link_libraries(UnikernelTest PUBLIC
        nes-common
        nes-data-types
        nes-grpc
)

target_include_directories(UnikernelTest
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-optimizer/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configuration/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-benchmark/include>
        $<INSTALL_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_compile_definitions(UnikernelTest PUBLIC BENCHMARK_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/benchmark-data/")
target_compile_definitions(UnikernelTest PUBLIC UNIKERNEL_TEST_LIBRARY)

target_link_options(UnikernelTest PRIVATE "-fuse-ld=lld")