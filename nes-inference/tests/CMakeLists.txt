add_nes_test_nebuli(ModelLoaderTest ModelLoaderTest.cpp)
target_compile_definitions(ModelLoaderTest PRIVATE "TEST_DATA_DIR=\"${CMAKE_CURRENT_SOURCE_DIR}/testdata\"")
