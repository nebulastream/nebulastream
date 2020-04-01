#include <gtest/gtest.h>

#include <string>
#include <cstring>
#include <thread>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/BufferManager.hpp>

#include <Util/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <SourceSink/KafkaSink.hpp>
#include <SourceSink/KafkaSource.hpp>
#include <SourceSink/SourceCreator.hpp>

constexpr char *KAFKA_BROKER = "localhost:9092";

namespace NES {

class KafkaTest : public testing::Test {
 public:

  void SetUp() {
    NES::setupLogging("KafkaTest.log", NES::LOG_DEBUG);

    schema = SchemaTemp::create()->addField("user_id", 16)->addField("page_id", 16)
        ->addField("campaign_id", 16)->addField("ad_type", 9)->addField(
        "event_type", 9)->addField("current_ms", UINT64)->addField("ip", INT32);

    uint64_t tuple_size = schema->getSchemaSize();
    buffer_size = num_tuples_to_process * tuple_size / num_of_buffers;

    ASSERT_GT(buffer_size, 0);
    BufferManager::instance().resizeFixedBufferCnt(num_of_buffers);
    BufferManager::instance().resizeFixedBufferSize(buffer_size);


    NES_DEBUG("Setup KafkaTest")
  }
  void TearDown() {
    NES_DEBUG("Tear down KafkaTest")
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
// NOTE: ALL DISABLED TESTS ONLY WITH KAFKA INSTANCE, PLEASE SETUP KAFKA FIRST
// MAYBE YOU ALSO NEED TO UPDATE GLOBAL VARIABLE `KAFKA_BROKER`
TEST_F(KafkaTest, DISABLED_KafkaSinkSendDataInitByKafkaConfig) {
  const cppkafka::Configuration sinkConfig = { { "metadata.broker.list",
      KAFKA_BROKER } };

  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, topic,
                                                            sinkConfig);
  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  EXPECT_TRUE(kafkaSink->writeData(buf));
  BufferManager::instance().releaseFixedSizeBuffer(buf);
}

TEST_F(KafkaTest, DISABLED_KafkaSinkSendDataInitByBroker) {
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);

  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  EXPECT_TRUE(kafkaSink->writeData(buf));
  BufferManager::instance().releaseFixedSizeBuffer(buf);
}

TEST_F(KafkaTest, DISABLED_KafkaSinkSendNullPointer) {
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);
  EXPECT_FALSE(kafkaSink->writeData(nullptr));
}

TEST_F(KafkaTest, DISABLED_KafkaSinkSendNullData) {
  // NullData: tuple buffer's content is all zeros. we didn't check tuple buffer's content.
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);
  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  EXPECT_TRUE(kafkaSink->writeData(buf));
  BufferManager::instance().releaseFixedSizeBuffer(buf);
}

TEST_F(KafkaTest, DISABLED_KafkaSinkWithInvalidBroker) {
  const std::string invalid_broker = "invalid-kafka-broker";
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema,
                                                            invalid_broker,
                                                            topic, 5);

  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  try {
    kafkaSink->writeData(buf);
    FAIL();
  } catch (...) {
    SUCCEED();
  }

  BufferManager::instance().releaseFixedSizeBuffer(buf);
}

TEST_F(KafkaTest, DISABLED_KafkaSinkTimeout) {
  const std::string invalid_broker = "invalid-kafka-broker";
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema,
                                                            invalid_broker,
                                                            topic, 0);

  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
  try {
    kafkaSink->writeData(buf);
    FAIL();
  } catch (...) {
    SUCCEED();
  }
  BufferManager::instance().releaseFixedSizeBuffer(buf);
}

TEST_F(KafkaTest, DISABLED_KafkaSourceInitByKafkaConfig1) {
  const cppkafka::Configuration sourceConfig = { { "metadata.broker.list",
      brokers.c_str() }, { "group.id", groupId },
      { "enable.auto.commit", false } };

  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);
  TupleBufferPtr buf1 = BufferManager::instance().getFixedSizeBuffer();
  EXPECT_TRUE(kafkaSink->writeData(buf1));

  const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema, topic,
                                                                  sourceConfig);
  TupleBufferPtr buf2 = nullptr;
  while (!buf2)
    buf2 = kafkaSource->receiveData();

  size_t sz1 = buf1->getBufferSizeInBytes();
  size_t sz2 = buf2->getBufferSizeInBytes();

  EXPECT_EQ(sz1, sz2) << "sz1 is " << sz1 << ", whereas sz2 is " << sz2;
  void *_buf1 = buf2->getBuffer();
  void *_buf2 = buf2->getBuffer();
  EXPECT_TRUE(!memcmp(_buf1, _buf2, sz1));
}

TEST_F(KafkaTest, DISABLED_KafkaSourceInitByKafkaConfig2) {
  const cppkafka::Configuration sourceConfig = { { "metadata.broker.list",
      brokers.c_str() }, { "group.id", groupId } };

  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);
  TupleBufferPtr buf1 = BufferManager::instance().getFixedSizeBuffer();
  EXPECT_TRUE(kafkaSink->writeData(buf1));

  const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema, topic,
                                                                  sourceConfig);
  TupleBufferPtr buf2 = nullptr;
  while (!buf2)
    buf2 = kafkaSource->receiveData();

  size_t sz1 = buf1->getBufferSizeInBytes();
  size_t sz2 = buf2->getBufferSizeInBytes();

  EXPECT_EQ(sz1, sz2) << "sz1 is " << sz1 << ", whereas sz2 is " << sz2;
  void *_buf1 = buf2->getBuffer();
  void *_buf2 = buf2->getBuffer();
  EXPECT_TRUE(!memcmp(_buf1, _buf2, sz1));
}

TEST_F(KafkaTest, DISABLED_KafkaSourceInitByBroker) {
  const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema,
                                                                  brokers,
                                                                  topic);
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);

  TupleBufferPtr buf1 = BufferManager::instance().getFixedSizeBuffer();
  EXPECT_TRUE(kafkaSink->writeData(buf1));

  TupleBufferPtr buf2 = nullptr;
  while (!buf2)
    buf2 = kafkaSource->receiveData();

  size_t sz1 = buf1->getBufferSizeInBytes();
  size_t sz2 = buf2->getBufferSizeInBytes();

  EXPECT_EQ(sz1, sz2) << "sz1 is " << sz1 << ", whereas sz2 is " << sz2;
  void *_buf1 = buf2->getBuffer();
  void *_buf2 = buf2->getBuffer();
  EXPECT_TRUE(!memcmp(_buf1, _buf2, sz1));
}

TEST_F(KafkaTest, DISABLED_KafkaSourceWithInvalidBroker) {
  TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();

  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);

  EXPECT_TRUE(kafkaSink->writeData(buf));

  const std::string invalid_brokers = "invalid-kafka-broker";
  const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(
      schema, invalid_brokers, topic, groupId, 5);

  Timestamp startTick = getTimestamp();
  Timestamp endTick = getTimestamp();
  while ((endTick - startTick) / (float(1e9)) < 5) {
    TupleBufferPtr buf2 = kafkaSource->receiveData();
    if (buf2) {
      FAIL();
    }
    endTick = getTimestamp();
  }
  SUCCEED();
}

TEST_F(KafkaTest, DISABLED_KafkaSinkToSource) {
  BufferManager::instance().resizeFixedBufferCnt(0);
  BufferManager::instance().resizeFixedBufferCnt(num_of_buffers);
  BufferManager::instance().resizeFixedBufferSize(buffer_size);

  std::string path_to_file =
      "../tests/test_data/ysb-tuples-100-campaign-100.bin";
  const DataSourcePtr fileSource = createBinaryFileSource(schema, path_to_file);

  const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema,
                                                                  brokers,
                                                                  topic);
  const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                            topic);

  while (fileSource->getNumberOfGeneratedBuffers() < num_of_buffers) {
    TupleBufferPtr buf1 = fileSource->receiveData();
    kafkaSink->writeData(buf1);
    TupleBufferPtr buf2 = nullptr;
    while (!buf2)
      buf2 = kafkaSource->receiveData();
    void *_buf1 = buf1->getBuffer();
    void *_buf2 = buf2->getBuffer();
    size_t sz1 = buf1->getBufferSizeInBytes();
    size_t sz2 = buf2->getBufferSizeInBytes();

    EXPECT_TRUE(sz1 == sz2);
    EXPECT_FALSE(memcmp(_buf1, _buf2, sz1));
  }
}

TEST_F(KafkaTest, KafkaSourceInit) {
  const cppkafka::Configuration sourceConfig = { { "metadata.broker.list",
      brokers.c_str() }, { "group.id", groupId } };
  {
    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(
        schema, topic, sourceConfig);
  }
  {
    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(schema,
                                                                    brokers,
                                                                    topic);
  }
  SUCCEED();
}

TEST_F(KafkaTest, KafkaSourceInitWithoutGroupId) {
  const cppkafka::Configuration sourceConfig = { { "metadata.broker.list",
      brokers.c_str() } };

  try {
    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(
        schema, topic, sourceConfig);
    FAIL();
  } catch (...) {
    SUCCEED();
  }

}

TEST_F(KafkaTest, KafkaSourceInitWithEmptyConfig) {
  const cppkafka::Configuration sourceConfig = { };

  try {
    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(
        schema, topic, sourceConfig);
    FAIL();
  } catch (...) {
    SUCCEED();
  }
}

TEST_F(KafkaTest, KafkaSourceInitWithEmptyTopic) {
  const cppkafka::Configuration sourceConfig = { { "metadata.broker.list",
      brokers.c_str() }, { "group.id", groupId } };
  try {
    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(
        schema, std::string(""), sourceConfig);
    FAIL();
  } catch (...) {
    SUCCEED();
  }
  try {
    const DataSourcePtr kafkaSource = std::make_shared<KafkaSource>(
        schema, brokers, std::string(""));
    FAIL();
  } catch (...) {
    SUCCEED();
  }
}

TEST_F(KafkaTest, KafkaSinkInit) {
  const cppkafka::Configuration sinkConfig = { { "metadata.broker.list",
      KAFKA_BROKER }, { "request.timeout.ms", 30000 } };
  {
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, topic,
                                                              sinkConfig);
  }
  {
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                              topic);
  }
  SUCCEED();
}

TEST_F(KafkaTest, KafkaSinkInitWithEmptyConfig) {
  const cppkafka::Configuration sinkConfig = { };
  try {
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, topic,
                                                              sinkConfig);
    FAIL();
  } catch (...) {
    SUCCEED();
  }
}
// FIXME: c++ cannot capture error inside librdkafka or C error
TEST_F(KafkaTest, KafkaSinkInitWithInvalidBroker) {
  {
    const cppkafka::Configuration sinkConfig = { { "metadata.broker.list",
        "invalid-kafka-broker" } };
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, topic,
                                                              sinkConfig);
  }

  {
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(
        schema, std::string("invalid-kafka-broker"), topic);
  }
  SUCCEED();
}

TEST_F(KafkaTest, KafkaSinkInitWithEmptyTopic) {
  {
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema, brokers,
                                                              std::string(""));
  }
  {
    const cppkafka::Configuration sinkConfig = { { "metadata.broker.list",
        "invalid-kafka-broker" } };
    const DataSinkPtr kafkaSink = std::make_shared<KafkaSink>(schema,
                                                              std::string(""),
                                                              sinkConfig);
  }
  SUCCEED();
}

}
