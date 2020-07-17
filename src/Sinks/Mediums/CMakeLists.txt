add_source_nes(
        FileSink.cpp
        ZmqSink.cpp
        SinkMedium.cpp
        PrintSink.cpp
        KafkaSink.cpp
)

if(NES_USE_KAFKA)
    add_source_nes(
            Mediums/KafkaSink.cpp
    )
endif()