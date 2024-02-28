find_package(Argumentum REQUIRED)

add_executable(unikernel-source src/main.cpp src/CSVDataGenerator.cpp src/AutomaticDataGenerator.cpp src/Options.cpp src/FileDataGenerator.cpp)
target_include_directories(unikernel-source PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

find_package(TBB)

if (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo" OR NOT ${TBB_FOUND})
    message("-- Not Using TBB")
    target_link_libraries(unikernel-source PUBLIC UnikernelTest UnikernelCommon Argumentum::argumentum)
else ()
    target_compile_definitions(unikernel-source PUBLIC NES_UNIKERNEL_SOURCE_HAS_TBB)
    target_link_libraries(unikernel-source PUBLIC UnikernelTest UnikernelCommon Argumentum::argumentum TBB::tbb TBB::tbbmalloc)
endif ()
