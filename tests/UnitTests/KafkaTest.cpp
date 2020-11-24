/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <gtest/gtest.h>
#ifdef ENABLE_KAFKA_BUILD
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <cstring>
#include <string>
#include <thread>

#include <Sources/KafkaSink.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/TimeMeasurement.hpp>

constexpr char* KAFKA_BROKER = "localhost:9092";

namespace NES {

class KafkaTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    void SetUp() {
        NES::setupLogging("KafkaTest.log", NES::LOG_DEBUG);

        schema = Schema::create()
                     ->addField("user_id", 16)
                     ->addField("page_id", 16)
                     ->addField("campaign_id", 16)
                     ->addField("ad_type", 9)
                     ->addField("event_type", 9)
                     ->addField("current_ms", UINT64)
                     ->addField("ip", INT32);

        uint64_t tuple_size = schema->getSchemaSizeInBytes();
        buffer_size = num_tuples_to_process * tuple_size / num_of_buffers;

        bufferManager = std::make_shared<BufferManager>();
        queryManager = std::make_shared<QueryManager>();

        ASSERT_GT(buffer_size, 0);

        NES_DEBUG("Setup KafkaTest");
    }
    void TearDown() { NES_DEBUG("Tear down KafkaTest"); }

  protected:
    const std::string brokers = std::string(KAFKA_BROKER);
    const std::string topic = std::string("nes");
    const std::string groupId = std::string("nes");

    const uint64_t num_of_buffers = 5;
    const uint64_t num_tuples_to_process = 100;
    uint64_t buffer_size;
    SchemaPtr schema;
};

TEST_F(KafkaTest, KafkaSourceInit) {

    const DataSourcePtr kafkaSource =
        std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, topic, "group", true, 100);
    SUCCEED();
}

TEST_F(KafkaTest, KafkaSourceInitWithoutGroupId) {

    try {
        const DataSourcePtr kafkaSource =
            std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, topic, "", true, 100);
        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(KafkaTest, KafkaSourceInitWithEmptyTopic) {

    try {
        const DataSourcePtr kafkaSource =
            std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, "", "group", true, 100);
        FAIL();
    } catch (...) {
        SUCCEED();
    }
}

TEST_F(KafkaTest, KafkaSinkInit) {

    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, KAFKA_BROKER, topic, 30000);
    SUCCEED();
}

// FIXME: c++ cannot capture error inside librdkafka or C error
TEST_F(KafkaTest, KafkaSinkInitWithInvalidBroker) {

    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, "invalid-kafka-broker", topic, 1000);
    SUCCEED();
}

TEST_F(KafkaTest, KafkaSinkInitWithEmptyTopic) {
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers, std::string(""), 1000);
    SUCCEED();
}

}// namespace NES
#endif
