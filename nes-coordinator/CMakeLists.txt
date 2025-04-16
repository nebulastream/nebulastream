
# Find Oat++ package
find_package(oatpp REQUIRED)

# Create an executable target
add_executable(nes-rest_server src/rest_server.cpp)

# Include the 'include' directory where DTOs are stored
# target_include_directories(nes-rest_server PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(nes-rest_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)


# Link Oat++ to the executable
target_link_libraries(nes-rest_server PUBLIC nes-catalogs nes-nebuli-lib PRIVATE oatpp::oatpp)

# Enable C++17 (or later)
set_target_properties(nes-rest_server PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)
