add_source_nes(
    DistributionCharacteristic.cpp
    TimeCharacteristic.cpp
        WindowDefinition.cpp

)
add_subdirectory(Watermark)
add_subdirectory(Runtime)
add_subdirectory(WindowAggregations)
add_subdirectory(WindowMeasures)
add_subdirectory(WindowTypes)