add_executable(cc-parser main.cpp)
find_package(folly CONFIG REQUIRED)
target_compile_options(cc-parser PRIVATE -std=c++20 -fexperimental-library)
target_link_libraries(
        cc-parser
        PRIVATE
        folly::folly
)