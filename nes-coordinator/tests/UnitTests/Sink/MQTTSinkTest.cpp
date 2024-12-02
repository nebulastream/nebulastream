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
#ifdef ENABLE_MQTT_BUILD
#include <API/Schema.hpp>
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Network/NetworkChannel.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <random>

using namespace NES;
/**
 * @brief this class implements tests for the MQTTSink class
 * Due to a LACK OF BROKER in the most common MQTT-CPP libraries, the broker must be set up manually.
 * An easy way is to use the Mosquitto broker. On Ubuntu: 'sudo apt install mosquitto'.
 * In order to start the broker with information for every received payload: 'mosquitto -v' (uses default port 1883).
 * BE AWARE that some of the tests require the tester to manually kill/disconnect the broker.
 * ALSO: Not all tests are meant to succeed, but might produce wanted errors.
 */

//FIXME tests right now rely on setting up a broker manually. Moreover, they intentionally fail. (covered in issue #1599)
// - find a way to fully automate tests (e.time::WorkerContext workerContext(g. using redBoltz c++ MQTT library, which offers a broker
// - fix tests, so they do not intentionally fail, but always succeed, if right behaviour is shown
class MQTTSinkTest : public Testing::BaseIntegrationTest {
  public:
    // constants used throughout the tests
    const std::string LOCAL_ADDRESS = "127.0.0.1:1883";
    const std::string CLIENT_ID = "nes-mqtt-test-client";
    const std::string TOPIC = "v1/devices/me/telemetry";
    const std::string USER = "rfRqLGZRChg8eS30PEeR";

    SchemaPtr testSchema;
    Runtime::NodeEnginePtr nodeEngine{};

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MQTTSinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup MQTTSinkTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        dataPort = Testing::BaseIntegrationTest::getAvailablePort();
        NES_DEBUG("Setup MQTTSinkTest test case.");
        auto conf = CSVSourceType::create("x", "x1");
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->dataPort.setValue(*dataPort);
        workerConfiguration->physicalSourceTypes.add(conf);

        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        testSchema = TestSchemas::getSchemaTemplate("id_val_u32");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        dataPort.reset();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("Setup MQTT test case.");
        Testing::BaseIntegrationTest::TearDown();
    }

    static NES::Runtime::TupleBuffer createTupleBuffer(uint64_t bufferSize,
                                                       const std::shared_ptr<Runtime::BufferManager>& buffMgr) {
        auto buffer = buffMgr->getBufferBlocking();
        std::mt19937 rnd;
        std::uniform_int_distribution gen(1, 100);

        /* Iterate over the input TupleBuffer. In this case each Tuple of the TupleBuffer contains 64 bit (uint64).
           32 bit are used for the key (uint32) and the remaining 32 bit for the value (also uint32).
           In each iteration one uint32_t, the KEY, is written to the first 32 bits of the uint64 and another randomly
           determined uint34_t, the VALUE, is written to the last 32 bits of the uint64 */
        for (uint32_t j = 0; j < bufferSize / sizeof(uint32_t); ++j) {
            buffer.getBuffer<uint32_t>()[j] = j / 2;
            ++j;
            buffer.getBuffer<uint32_t>()[j] = (uint32_t) gen(rnd);
        }
        buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));

        return buffer;
    }

    bool createMQTTSinkConnectToBrokerWriteData(uint64_t numTuples,
                                                uint64_t maxBufferedMSGs,
                                                MQTTSinkDescriptor::TimeUnits timeUnit,
                                                uint64_t msgDelay,
                                                MQTTSinkDescriptor::ServiceQualities qualityOfService,
                                                bool asynchronousClient,
                                                bool printBuffer) {
        // Create MQTT Sink
        Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
        SinkFormatPtr format = std::make_shared<JsonFormat>(testSchema, nodeEngine->getBufferManager());

        /* This was originally done like the other sink tests, using the createMQTTSink() function, which returns a DataSinkPtr.
           However, this caused a problem, because the DataSinkPtr is defined as 'std::shared_ptr<SinkMedium>' and thus only gives
           access to the SinkMedium functions and not the MQTTSink functions. This caused a problem, because the tests want to access
           the connect() function of MQTTSink, because it returns a bool that indicates whether the Sink successfully connected to a
           server/broker and SinkMedium does not provide such a connect() function. There also seems to be no 'good' way of casting
           a std::shared_ptr<SinkMedium> to a std::shared_ptr<MQTTSink>. As it seems, shared pointers should be initialized with
           std::make_shared.  */
        MQTTSinkPtr mqttSink = std::make_shared<MQTTSink>(format,
                                                          nodeEngine,
                                                          1,
                                                          INVALID_SHARED_QUERY_ID,
                                                          INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                          INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                          LOCAL_ADDRESS,
                                                          CLIENT_ID,
                                                          TOPIC,
                                                          USER,
                                                          maxBufferedMSGs,
                                                          timeUnit,
                                                          msgDelay,
                                                          qualityOfService,
                                                          asynchronousClient);

        // Create Buffer
        const uint64_t bufferSize = 8 * numTuples;
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, 1);
        auto inputBuffer = createTupleBuffer(bufferSize, buffMgr);
        if (printBuffer) {
            auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(testSchema, inputBuffer.getBufferSize());
            auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer(rowLayout, inputBuffer);
            std::stringstream testTupleBufferAsString;
            testTupleBufferAsString << testTupleBuffer;
            NES_DEBUG("bufferContent before write={}", testTupleBufferAsString.str());
        }
        bool connectSuccessful = mqttSink->connect();
        if (connectSuccessful) {
            return mqttSink->writeData(inputBuffer, workerContext);
        }
        NES_ERROR("MQTTSinkTest:createMQTTSinkConnectToBrokerWriteData: Could not connect to MQTT broker");
        return false;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down MQTT test class."); }

  protected:
    Testing::BorrowedPortPtr dataPort;
};

/* ------------------------------------------------------------------------------ */
/* ------------------------ ASYNCHRONOUS CLIENT TESTS --------------------------- */
/* ------------------------------------------------------------------------------ */
TEST_F(MQTTSinkTest, testMQTTClientCreation) {
    uint64_t maxBufferedMSGs = 120;
    const MQTTSinkDescriptor::TimeUnits timeUnit = MQTTSinkDescriptor::TimeUnits::milliseconds;
    uint64_t msgDelay = 500;
    MQTTSinkDescriptor::ServiceQualities qualityOfService = MQTTSinkDescriptor::ServiceQualities::atLeastOnce;
    bool asynchronousClient = true;
    auto mqttSink = createMQTTSink(testSchema,
                                   INVALID_SHARED_QUERY_ID,
                                   INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                   INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                   nodeEngine,
                                   1,
                                   LOCAL_ADDRESS,
                                   CLIENT_ID,
                                   TOPIC,
                                   USER,
                                   maxBufferedMSGs,
                                   timeUnit,
                                   msgDelay,
                                   qualityOfService,
                                   asynchronousClient,
                                   FaultToleranceType::NONE);
    NES_INFO("{}", mqttSink->toString());
    ASSERT_FALSE(mqttSink->toString().empty());
}

/* - MQTT Client SetUp / Connect to Broker ---------------------------------------------------- */
TEST_F(MQTTSinkTest, DISABLED_testMQTTConnectToBrokerAsynchronous) {
    uint64_t maxBufferedMSGs = 120;
    const MQTTSinkDescriptor::TimeUnits timeUnit = MQTTSinkDescriptor::TimeUnits::milliseconds;
    uint64_t msgDelay = 500;
    MQTTSinkDescriptor::ServiceQualities qualityOfService = MQTTSinkDescriptor::ServiceQualities::atLeastOnce;
    bool asynchronousClient = true;
    SinkFormatPtr format = std::make_shared<JsonFormat>(testSchema, nodeEngine->getBufferManager());
    MQTTSinkPtr mqttSink = std::make_shared<MQTTSink>(format,
                                                      nodeEngine,
                                                      1,
                                                      INVALID_SHARED_QUERY_ID,
                                                      INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                      INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                      LOCAL_ADDRESS,
                                                      CLIENT_ID,
                                                      TOPIC,
                                                      USER,
                                                      maxBufferedMSGs,
                                                      timeUnit,
                                                      msgDelay,
                                                      qualityOfService,
                                                      asynchronousClient);

    bool connectedToBroker = mqttSink->connect();
    EXPECT_TRUE(connectedToBroker);
}

/* - MQTT Client send a finite amount of Data to Broker ---------------------------------------------------- */
TEST_F(MQTTSinkTest, DISABLED_testMQTTsendFiniteDataToBrokerAsynchronous) {
    bool bufferDataSuccessfullyWrittenToBroker =
        createMQTTSinkConnectToBrokerWriteData(5,
                                               5,
                                               MQTTSinkDescriptor::TimeUnits::milliseconds,
                                               500,
                                               MQTTSinkDescriptor::ServiceQualities::atLeastOnce,
                                               true,
                                               false);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}

/* - MQTT Client kill broker, does client stop? ---------------------------------------------------- */
/* This test is meant to be done manually and to crash. Kill the client during the sending process. The MQTTSink writeData()
   call should fail and log: 'No more messages can be buffered' If the test is not stopped, it RUNS FOR AN HOUR" */
TEST_F(MQTTSinkTest, DISABLED_testMQTTbrokerDeathToClientStopAsynchronous) {
    bool bufferDataSuccessfullyWrittenToBroker =
        createMQTTSinkConnectToBrokerWriteData(3600,
                                               5,
                                               MQTTSinkDescriptor::TimeUnits::milliseconds,
                                               1,
                                               MQTTSinkDescriptor::ServiceQualities::atLeastOnce,
                                               true,
                                               false);
    NES_INFO("testMQTTbrokerDeathToClientStopAsynchronous result: {}", bufferDataSuccessfullyWrittenToBroker);
}

/* - MQTT Client kill disconnect and reconnect to broker, payloads lost? --------------------------- */
TEST_F(MQTTSinkTest, DISABLED_testMQTTsendMassiveDataQuicklyAsynchronous) {
    bool bufferDataSuccessfullyWrittenToBroker =
        createMQTTSinkConnectToBrokerWriteData(50000,
                                               1000,
                                               MQTTSinkDescriptor::TimeUnits::nanoseconds,
                                               100000,
                                               MQTTSinkDescriptor::ServiceQualities::atLeastOnce,
                                               true,
                                               false);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}

/* - What happens to the unsent messages when the broker dies, how does the client react? ----------- */
/* This test comprises two different tests. The first is not meant to succeed automatically but requires the tester to evaluate
   the result. TEST 1: Kill the broker during the sending process. The writeData() function should return false and the
   MQTTSink should log: 'Unsent messages'. TEST2: Kill the broker during the sending process, but this time reconnect before
   the client is finished sending messages. The test should succeed. */
TEST_F(MQTTSinkTest, DISABLED_testMQTTUnsentMessagesAsynchronous) {
    bool bufferDataSuccessfullyWrittenToBroker =
        createMQTTSinkConnectToBrokerWriteData(20,
                                               20,
                                               MQTTSinkDescriptor::TimeUnits::milliseconds,
                                               500,
                                               MQTTSinkDescriptor::ServiceQualities::atLeastOnce,
                                               true,
                                               false);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}

/* ------------------------------------------------------------------------------ */
/* ------------------------- SYNCHRONOUS CLIENT TESTS --------------------------- */
/* ------------------------------------------------------------------------------ */
/* - MQTT Client SetUp / Connect to Broker ---------------------------------------------------- */
TEST_F(MQTTSinkTest, DISABLED_testMQTTConnectToBrokerSynchronously) {
    uint64_t maxBufferedMSGs = 120;
    const MQTTSinkDescriptor::TimeUnits timeUnit = MQTTSinkDescriptor::TimeUnits::milliseconds;
    uint64_t msgDelay = 500;
    MQTTSinkDescriptor::ServiceQualities qualityOfService = MQTTSinkDescriptor::ServiceQualities::atLeastOnce;
    bool asynchronousClient = false;
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_u32");
    auto mqttSink = createMQTTSink(testSchema,
                                   INVALID_SHARED_QUERY_ID,
                                   INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                   INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                   nodeEngine,
                                   1,
                                   LOCAL_ADDRESS,
                                   CLIENT_ID,
                                   TOPIC,
                                   USER,
                                   maxBufferedMSGs,
                                   timeUnit,
                                   msgDelay,
                                   qualityOfService,
                                   asynchronousClient,
                                   FaultToleranceType::NONE);
    auto* testSink = dynamic_cast<MQTTSink*>(mqttSink.get());

    bool connectedToBroker = testSink->connect();
    ASSERT_EQ(true, connectedToBroker);
}

/* - MQTT Synchronous Client send a finite amount of Data to Broker --------------------------------------------- */
/* This test comprises two different tests. The first is not meant to succeed automatically but requires the tester to
   evaluate the result. TEST 1: Kill the broker during the sending process. The writeData() function should return false and
   the MQTTSink should log: 'Connection Lost'. TEST2: Do not kill the broker during the sending process. The client should
   send all messages successfully and the test should pass. */
TEST_F(MQTTSinkTest, DISABLED_testMQTTsendFiniteDataToBrokerSynchronously) {

    bool bufferDataSuccessfullyWrittenToBroker =
        createMQTTSinkConnectToBrokerWriteData(20,
                                               5,
                                               MQTTSinkDescriptor::TimeUnits::milliseconds,
                                               500,
                                               MQTTSinkDescriptor::ServiceQualities::atLeastOnce,
                                               false,
                                               false);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}
#endif
