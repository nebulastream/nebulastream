add_source_nes(
        DataSink.cpp
        BinarySink.cpp
        ZmqSink.cpp
        FileOutputSink.cpp
        PrintSink.cpp
        CSVSink.cpp
        TestSinks.cpp
)

if(NES_USE_KAFKA)
    add_source_nes(
            KafkaSink.cpp
    )
endif()