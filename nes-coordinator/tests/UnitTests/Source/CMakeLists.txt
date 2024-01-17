### ZMQ Data Source Sink Tests ###
add_nes_unit_test(zmq-tests
        "ZMQTest.cpp"
        "SourceTest.cpp"
        "MillisecondIntervalTest.cpp"
        "CircularBufferTest.cpp")
if (NOT (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin"))
    add_nes_unit_test(tcp-source-tests
            "TCPSourceTest.cpp"
    )
    target_link_libraries(tcp-source-tests "-Wl,--wrap,read")
endif ()

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_nes_unit_test(kafka-source-tests "KafkaSourceTest.cpp")
endif (NES_USE_KAFKA)

### OPC Tests ###
if (NES_USE_OPC)
    add_nes_unit_test(opc-source-tests OPCSourceTest.cpp)
endif (NES_USE_OPC)

### MQTT Tests ###
if (NES_USE_MQTT)
    add_nes_unit_test(mqtt-source-tests MQTTSourceTest.cpp)
endif (NES_USE_MQTT)

