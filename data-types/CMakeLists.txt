add_library(data-type DataType.cpp DataTypeRegistrar.cpp DataTypeSerializationFactory.cpp)

find_package(reflectcpp CONFIG REQUIRED)
target_include_directories(data-type PUBLIC include PRIVATE .)
target_link_libraries(data-type PRIVATE nes-common reflectcpp::reflectcpp)

add_library(data-type-physical PhysicalType.cpp)
target_include_directories(data-type-physical PUBLIC physical PRIVATE .)
target_link_libraries(data-type-physical PRIVATE nes-common)

add_tests_if_enabled(tests)

function(add_data_type type_name)
    target_sources(data-type PRIVATE ${ARGN})
endfunction()

add_subdirectory(builtin)
add_subdirectory(custom)
