add_source_nes(
        FileSink.cpp
        ZmqSink.cpp
        SinkMedium.cpp
        PrintSink.cpp
        KafkaSink.cpp
)

if (NES_USE_OPC)
    add_source_nes(
        OPCSink.cpp
    )
endif ()