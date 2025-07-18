add_executable(example main.cpp lib.cpp
        FunctionRegistry.cpp
        FunctionRegistry.h)

#target_link_libraries(toy PUBLIC nes-common nes-logical-operators nes-physical-operators nes-data-types nes-memory nes-nautilus nes-sources nes-query-engine)
#target_include_directories(toy PUBLIC
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#        $<INSTALL_INTERFACE:include/nebulastream/>)


