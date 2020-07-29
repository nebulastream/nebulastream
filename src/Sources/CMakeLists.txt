add_source_nes(
        DataSource.cpp
        GeneratorSource.cpp
        BinarySource.cpp
        ZmqSource.cpp
        CSVSource.cpp
        SourceCreator.cpp
        DefaultSource.cpp
        SenseSource.cpp
        AdaptiveSource.cpp
)

if(NES_USE_KAFKA)
    add_source_nes(
            KafkaSource.cpp
    )
endif()