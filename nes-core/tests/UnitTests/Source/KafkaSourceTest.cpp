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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/Mediums//KafkaSink.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>
#include <cppkafka/cppkafka.h>
#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <thread>

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

const std::string KAFKA_BROKER = "localhost:9092";

namespace NES {

/**
 * NOTE: this test requires a running kafka instance
 */
class KafkaSourceTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("KAFKASourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("KAFKASOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("KAFKASOURCETEST::SetUp() KAFKASourceTest cases set up.");
        test_schema = Schema::create()->addField("var", BasicType::UINT32);
        kafkaSourceType = KafkaSourceType::create();
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        Testing::BaseIntegrationTest::TearDown();
        NES_DEBUG("KAFKASOURCETEST::TearDown() Tear down MQTTSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("KAFKASOURCETEST::TearDownTestCases() Tear down KAFKASourceTest test class."); }

    Runtime::NodeEnginePtr nodeEngine{nullptr};

    SchemaPtr test_schema;
    uint64_t buffer_size{};
    KafkaSourceTypePtr kafkaSourceType;

    const std::string brokers = std::string(KAFKA_BROKER);
    const std::string topic = std::string("sourceTest");
    const std::string groupId = std::string("0");
};

/**
 * Tests basic set up of Kafka source
 */
TEST_F(KafkaSourceTest, KafkaSourceInit) {
    auto kafkaSource = createKafkaSource(test_schema,
                                         nodeEngine->getBufferManager(),
                                         nodeEngine->getQueryManager(),
                                         2,
                                         brokers,
                                         topic,
                                         groupId,
                                         true,
                                         100,
                                         "earliest",
                                         kafkaSourceType,
                                         OPERATORID,
                                         OPERATORID,
                                         NUMSOURCELOCALBUFFERS,
                                         1,
                                         "defaultPhysicalStreamName",
                                         std::vector<Runtime::Execution::SuccessorExecutablePipeline>());

    SUCCEED();
}
/**
 * Test if schema, Kafka server address, clientId, user, and topic are the same
 */
TEST_F(KafkaSourceTest, KafkaSourcePrint) {

    auto kafkaSource = createKafkaSource(test_schema,
                                         nodeEngine->getBufferManager(),
                                         nodeEngine->getQueryManager(),
                                         2,
                                         brokers,
                                         topic,
                                         groupId,
                                         true,
                                         100,
                                         "earliest",
                                         kafkaSourceType,
                                         OPERATORID,
                                         OPERATORID,
                                         NUMSOURCELOCALBUFFERS,
                                         1,
                                         "defaultPhysicalStreamName",
                                         std::vector<Runtime::Execution::SuccessorExecutablePipeline>());

    std::string expected = "KAFKA_SOURCE(SCHEMA(var:INTEGER(32 bits) ), BROKER(localhost:9092), TOPIC(sourceTest). "
                           "OFFSETMODE(earliest). BATCHSIZE(1). ";

    EXPECT_EQ(kafkaSource->toString(), expected);

    NES_DEBUG("kafka string={}", kafkaSource->toString());

    SUCCEED();
}

#ifdef RUNNING_KAFKA_INSTANCE
/**
 * Tests if obtained value is valid.
 */
TEST_F(KafkaSourceTest, KafkaTestNative) {
    int partition_value = -1;

    //    #####################
    // Construct the configuration
    cppkafka::Configuration config = {{"metadata.broker.list", brokers},
                                      {"group.id", "123"},
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
    //    ##################################

    // Create a message builder for this topic
    cppkafka::MessageBuilder builder(topic);

    // Get the partition we want to write to. If no partition is provided, this will be
    // an unassigned one
    if (partition_value != -1) {
        builder.partition(partition_value);
    }

    // Construct the configuration
    cppkafka::Configuration configProd = {{"metadata.broker.list", KAFKA_BROKER}};

    // Create the producer
    cppkafka::Producer producer(configProd);

    NES_DEBUG("Producing messages into topic {}", topic);

    // Produce a message!
    string message = "32";
    builder.payload(message);

    // Actually produce the message we've built
    producer.produce(builder);
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
                    NES_DEBUG(" {} -> ", msg.get_key());
                }
                // Print the payload
                NES_DEBUG("{}", msg.get_payload());

                // Now commit the message
                consumer.commit(msg);
                std::string_view payload_view(reinterpret_cast<const char*>(msg.get_payload().get_data()), message.size());
                EXPECT_EQ(payload_view, message);
                pollSuccessFull = true;
            }
        }
    }

    EXPECT_EQ(true, pollSuccessFull);
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(KafkaSourceTest, KafkaSourceValue) {
    auto kafkaSource = createKafkaSource(test_schema,
                                         nodeEngine->getBufferManager(),
                                         nodeEngine->getQueryManager(),
                                         2,
                                         brokers,
                                         topic,
                                         groupId,
                                         true,
                                         100,
                                         "earliest",
                                         OPERATORID,
                                         OPERATORID,
                                         NUMSOURCELOCALBUFFERS,
                                         1,
                                         std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    auto test_schema = Schema::create()->addField("var", BasicType::UINT32);

    //first call to connect
    auto tuple_bufferJ = kafkaSource->receiveData();

    cppkafka::Configuration config = {{"metadata.broker.list", brokers.c_str()},
                                      {"group.id", groupId},
                                      {"enable.auto.commit", true}};
    cppkafka::Producer producer(config);

    // Produce a message!
    string message = "32";
    producer.produce(cppkafka::MessageBuilder(topic).partition(0).payload(message));
    producer.flush();

    auto tuple_buffer = kafkaSource->receiveData();
    EXPECT_TRUE(tuple_buffer.has_value());
    auto* tuple = (char*) tuple_buffer->getBuffer();
    std::string str(tuple);
    std::string expected = "32";
    NES_DEBUG("KAFKASOURCETEST::TEST_F(KAFKASourceTest, KAFKASourceValue) expected value is: {}. Received value is: {}",
              expected,
              str);
    EXPECT_EQ(str, expected);
}

// Disabled, because it requires a manually set up Kafka broker
TEST_F(KafkaSourceTest, DISABLED_testDeployOneWorkerWithKafkaSourceConfigJson) {
    // submit message to kafka
    cppkafka::Configuration config = {{"metadata.broker.list", "127.0.0.1:9092"}};
    cppkafka::Producer producer(config);
    string message = R"({"var": 6})";
    producer.produce(cppkafka::MessageBuilder(topic).partition(0).payload(message));
    producer.flush();

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("KAFKASOURCETEST:: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source
    std::string source = R"(Schema::create()->addField(createField("var", BasicType::UINT32));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", source);
    NES_INFO("KAFKASOURCETEST:: Coordinator started successfully");

    NES_INFO("KAFKASOURCETEST:: Start worker 1");
    wrkConf->coordinatorPort = port;
    kafkaSourceType->setBrokers(KAFKA_BROKER);
    kafkaSourceType->setTopic(topic);
    kafkaSourceType->setGroupId(groupId);
    kafkaSourceType->setAutoCommit(true);
    kafkaSourceType->setConnectionTimeout(100);
    kafkaSourceType->setNumberOfBuffersToProduce(1);
    auto physicalSource = PhysicalSource::create("stream", "test_stream", kafkaSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("KAFKASOURCETEST: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("KAFKASOURCETEST: Submit query");
    string query =
        R"(Query::from("stream").filter(Attribute("var") < 7).sink(FileSinkDescriptor::create(")" + outputFilePath + R"("));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("KAFKASOURCETEST: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|stream$var:UINT32|\n"
                             "+----------------------------------------------------+\n"
                             "|6|\n"
                             "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("KAFKASOURCETEST: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("KAFKASOURCETEST: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("KAFKASOURCETEST: Test finished");
}

// Disabled, because it requires a manually set up Kafka broker
TEST_F(KafkaSourceTest, DISABLED_testDeployOneWorkerWithKafkaSourceConfig) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("KAFKASOURCETEST:: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    std::string source =
        R"(Schema::create()->addField("type", DataTypeFactory::createArray(10, DataTypeFactory::createChar()))
                            ->addField(createField("hospitalId", BasicType::UINT64))
                            ->addField(createField("stationId", BasicType::UINT64))
                            ->addField(createField("patientId", BasicType::UINT64))
                            ->addField(createField("time", BasicType::UINT64))
                            ->addField(createField("healthStatus", BasicType::UINT8))
                            ->addField(createField("healthStatusDuration", BasicType::UINT32))
                            ->addField(createField("recovered", BasicType::BOOLEAN))
                            ->addField(createField("dead", BasicType::BOOLEAN));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", source);
    NES_INFO("KAFKASOURCETEST:: Coordinator started successfully");

    NES_INFO("KAFKASOURCETEST:: Start worker 1");
    wrkConf->coordinatorPort = port;
    kafkaSourceType->setBrokers(KAFKA_BROKER);
    kafkaSourceType->setTopic(topic);
    kafkaSourceType->setGroupId(groupId);
    kafkaSourceType->setAutoCommit(true);
    kafkaSourceType->setConnectionTimeout(100);
    kafkaSourceType->setNumberOfBuffersToProduce(2);
    auto physicalSource = PhysicalSource::create("stream", "test_stream", kafkaSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("hospitalId") < 5).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}
#endif
}// namespace NES
#endif