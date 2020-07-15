add_source_nes(
        DataSink.cpp
        PrintSink.cpp
        TestSinks.cpp
        FileSink.cpp
        ZmqSink.cpp
)

if(NES_USE_KAFKA)
    add_source_nes(
            KafkaSink.cpp
    )
endif()