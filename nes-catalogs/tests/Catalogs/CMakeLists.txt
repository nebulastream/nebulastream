add_subdirectory(Topology)
add_nes_unit_test(catalog-test
        UDF/UDFCatalogTest.cpp
        UDF/JavaUDFDescriptorTest.cpp
        UDF/PythonUDFDescriptorTest.cpp
        Source/SourceCatalogTest.cpp)