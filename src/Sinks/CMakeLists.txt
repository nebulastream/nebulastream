add_source_nes(
        TestSinks.cpp
)

add_subdirectory(Mediums)
add_subdirectory(Formats)

if(NES_USE_KAFKA)
    add_source_nes(
            Mediums/KafkaSink.cpp
    )
endif()