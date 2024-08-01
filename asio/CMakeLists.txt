
message(STATUS "adding boost asio headers")

add_library(boost-asio INTERFACE)
# add_library(spdlog::spdlog_header_only ALIAS ${BOOST_ASIO})
#target_include_directories(${BOOST_ASIO} INTERFACE
#        "$<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/include>"
#        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")

target_include_directories(boost-asio INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

#target_include_directories(${BOOST_ASIO} PUBLIC
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#        $<INSTALL_INTERFACE:include/nebulastream/>)
#target_link_libraries(${BOOST_ASIO} INTERFACE Threads::Threads)