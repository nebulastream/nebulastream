#include <gtest/gtest.h>

#include <string>
#include <cstring>
#include <thread>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/BufferManager.hpp>

#include <Util/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <SourceSink/KafkaSink.hpp>
#include <SourceSink/KafkaSource.hpp>
#include <SourceSink/SourceCreator.hpp>

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
        buffer_size = num_tuples_to_process*tuple_size/num_of_buffers;

        bufferManager = std::make_shared<BufferManager>();
        queryManager = std::make_shared<QueryManager>();

        ASSERT_GT(buffer_size, 0);

        NES_DEBUG("Setup KafkaTest");
    }
    void TearDown() {
        NES_DEBUG("Tear down KafkaTest");
    }

  protected:
    const std::string brokers = std::string(KAFKA_BROKER);
    const std::string topic = std::string("nes");
    const std::string groupId = std::string("nes");

    const size_t num_of_buffers = 5;
    const uint64_t num_tuples_to_process = 100;
    size_t buffer_size;
    SchemaPtr schema;

};

TEST_F(KafkaTest, KafkaSourceInit) {

    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, topic, "group",true, 100);
    SUCCEED();
}

TEST_F(KafkaTest, KafkaSourceInitWithoutGroupId) {

    try {
        const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, topic, "",true, 100);
        FAIL();
    } catch (...) {
        SUCCEED();
    }

}

TEST_F(KafkaTest, KafkaSourceInitWithEmptyTopic) {

    try {
        const DataSourcePtr
            kafkaSource = std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, "", "group", true, 100);
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

}
