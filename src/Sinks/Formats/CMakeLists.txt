add_source_nes(
        CsvFormat.cpp
        JsonFormat.cpp
        NesFormat.cpp
        SinkFormat.cpp
        TextFormat.cpp
)

if(NES_USE_KAFKA)
    add_source_nes(
            Mediums/KafkaSink.cpp
    )
endif()