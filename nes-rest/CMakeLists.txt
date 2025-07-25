find_package(httplib CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

add_executable(restapi RestApiStarter.cpp)
target_link_libraries(restapi PRIVATE httplib::httplib nlohmann_json::nlohmann_json nes-nebuli-embedded-lib nes-sql-parser)
