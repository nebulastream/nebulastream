find_package(Argumentum REQUIRED)

add_executable(unikernel src/main.cpp src/DataSource.cpp src/NetworkSource.cpp src/NetworkSink.cpp src/SinkMedium.cpp src/Window.cpp)

target_include_directories(unikernel PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
file(GLOB OPERATOR_FILES ${CMAKE_CURRENT_SOURCE_DIR}/operator/*.o)
target_link_libraries(unikernel PRIVATE ${OPERATOR_FILES})

SET(UNIKERNEL_WORKER_ID 0)
SET(UNIKERNEL_CONFIGURATION_FILE /home/ls/DIMA/nebulastream/cmake-build-unikernelexport-clang-16-debug/nes-unikernel-export/export.yaml)

add_custom_target(unikernel_generator
        COMMAND NesUnikernelGenerator ${UNIKERNEL_WORKER_ID} -c ${UNIKERNEL_CONFIGURATION_FILE} -o ${CMAKE_CURRENT_SOURCE_DIR}/include/Pipeline.h
        COMMENT "Run Unikernel Generator"
)
add_dependencies(unikernel unikernel_generator)

target_compile_options(unikernel PRIVATE "-DUNIKERNEL_LIB=1" "-DWINDOW_OPERATOR=1")
target_link_libraries(unikernel PUBLIC nes-unikernel argumentum)

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L /usr/local/lib")

set_property(TARGET unikernel PROPERTY EXPORT_COMPILE_COMMANDS TRUE)