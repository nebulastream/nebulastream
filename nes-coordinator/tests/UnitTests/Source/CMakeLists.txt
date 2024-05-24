# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

