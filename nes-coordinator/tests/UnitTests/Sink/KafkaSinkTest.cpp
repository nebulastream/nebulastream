/*
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
#ifdef ENABLE_KAFKA_BUILD
#include <BaseIntegrationTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/SinkCreator.hpp>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>

#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <gtest/gtest.h>
#include <string>

#ifndef OPERATORID
#define OPERATORID OperatorId(1)
#endif

#ifndef QUERYID
#define QUERYID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

const std::string KAFKA_BROKER = "localhost:9092";

namespace NES {

/**
* NOTE: this test requires a running kafka instance
*/
class KafkaSinkTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KafkaSinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("KafkaSinkTest::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        dataPort = Testing::BaseIntegrationTest::getAvailablePort();
        NES_DEBUG("Setup KafkaSinkTest test case.");
        auto sourceType = DefaultSourceType::create("x", "x1");
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->dataPort.setValue(*dataPort);
        workerConfiguration->physicalSourceTypes.add(sourceType);
        workerConfiguration->bufferSizeInBytes = 1024;
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        testSchema = Schema::create()->addField("KEY", BasicType::UINT32)->addField("VALUE", BasicType::UINT32);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        dataPort.reset();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("KafkaSinkTest::TearDown() Tear down KafkaSinkTest");
        Testing::BaseIntegrationTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("KafkaSinkTest::TearDownTestCases() Tear down KafkaSinkTest test class."); }

    static NES::Runtime::TupleBuffer createSimpleBuffer(uint64_t bufferSize,
                                                        const std::shared_ptr<Runtime::BufferManager>& buffMgr) {
        auto buffer = buffMgr->getBufferBlocking();
        for (uint32_t j = 0; j < bufferSize / sizeof(uint32_t); ++j) {
            buffer.getBuffer<uint32_t>()[j] = 2;
        }
        buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));

        return buffer;
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Testing::BorrowedPortPtr dataPort;
    SchemaPtr testSchema;
    uint64_t buffer_size{};
    KafkaSourceTypePtr kafkaSourceType;

    const std::string brokers = std::string(KAFKA_BROKER);
    const std::string topic = std::string("sinkTest");
    const std::string groupId = std::string("0");
};

/**
* Tests basic set up of Kafka sink
*/
TEST_F(KafkaSinkTest, KafkaSinkInit) {
    auto kafkaSink = createCsvKafkaSink(testSchema,
                                        SharedQueryId(QUERYID),
                                        DecomposedQueryId(QUERYID),
                                        DecomposedQueryPlanVersion(0),
                                        nodeEngine,
                                        1,
                                        brokers,
                                        topic,
                                        1,
                                        FaultToleranceType::NONE
                                        1);
}

/**
* Test if schema, Kafka server address, clientId, user, and topic are the same
*/
TEST_F(KafkaSinkTest, KafkaSourcePrint) {
    auto kafkaSink = createCsvKafkaSink(testSchema,
                                        SharedQueryId(QUERYID),
                                        DecomposedQueryId(QUERYID),
                                        DecomposedQueryPlanVersion(0),
                                        nodeEngine,
                                        1,
                                        brokers,
                                        topic,
                                        1,
                                        FaultToleranceType::NONE
                                        1);

    std::string expected = "KAFKA_SINK(BROKER(localhost:9092), TOPIC(sinkTest).";

    EXPECT_EQ(kafkaSink->toString(), expected);

    NES_DEBUG("kafka string={}", kafkaSink->toString());
}

#ifdef RUNNING_KAFKA_INSTANCE
/**
 * Tests if obtained value is valid.
 */
TEST_F(KafkaSinkTest, KafkaSinkWriteBuffer) {
    auto kafkaSink = createTextKafkaSink(testSchema, OPERATORID, OPERATORID, nodeEngine, 1, brokers, topic, 1, 1);

    auto test_schema = Schema::create()->addField("var", BasicType::UINT32);
    auto inputBuffer = createSimpleBuffer(nodeEngine->getBufferManager()->getBufferSize(), nodeEngine->getBufferManager());
    auto testBuffer = createSimpleBuffer(nodeEngine->getBufferManager()->getBufferSize(), nodeEngine->getBufferManager());
    Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    int partition_value = -1;

    //    #####################
    // Construct the configuration
    cppkafka::Configuration config = {{"metadata.broker.list", brokers},
                                      {"group.id", groupId},
                                      {"auto.offset.reset", "earliest"},
                                      // Disable auto commit
                                      {"enable.auto.commit", false}};

    // Create the consumer
    cppkafka::Consumer consumer(config);

    // Print the assigned partitions on assignment
    consumer.set_assignment_callback([](const cppkafka::TopicPartitionList& partitions) {
        NES_DEBUG("Got assigned: {}", partitions);
    });

    // Print the revoked partitions on revocation
    consumer.set_revocation_callback([](const cppkafka::TopicPartitionList& partitions) {
        NES_DEBUG("Got revoked: {}", partitions);
    });

    // Subscribe to the topic
    consumer.subscribe({topic});

    NES_DEBUG("Consuming messages from topic {}", topic);

    //write buffer
    auto tuple_bufferJ = kafkaSink->writeData(inputBuffer, workerContext);

    //################################
    bool pollSuccessFull = false;
    size_t cnt = 0;
    while (!pollSuccessFull) {
        NES_DEBUG("run ={}", cnt++);
        if (cnt > 10) {
            break;
        }
        cppkafka::Message msg = consumer.poll();
        if (msg) {
            // If we managed to get a message
            if (msg.get_error()) {
                // Ignore EOF notifications from rdkafka
                if (!msg.is_eof()) {
                    NES_DEBUG("[+] Received error notification: {}", msg.get_error());
                }
            } else {
                // Print the key (if any)
                if (msg.get_key()) {
                    NES_DEBUG("{} -> ", msg.get_key());
                }
                // Print the payload
                NES_DEBUG("{}", msg.get_payload());

                // Now commit the message
                consumer.commit(msg);
                pollSuccessFull = true;
                std::string expected =
                    "+----------------------------------------------------+\n|KEY:UINT32|VALUE:UINT32|\n+------------------------"
                    "----------------------------+\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|"
                    "\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|"
                    "2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|"
                    "2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|"
                    "\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|"
                    "2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|"
                    "2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|"
                    "\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|"
                    "2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n|2|2|\n+--------------------------------------"
                    "--------------+";
                std::string_view payload_view(reinterpret_cast<const char*>(msg.get_payload().get_data()), expected.size());
                EXPECT_EQ(expected, payload_view);
            }
        }
    }
}
#endif
}// namespace NES
#endif
