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
#include <API/QueryAPI.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MaterializedViewManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/UtilityFunctions.hpp>

#include "../util/NesBaseTest.hpp"
#include "../util/TestQuery.hpp"
#include "../util/TestQueryCompiler.hpp"
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <gtest/gtest.h>
#include <random>
#include <utility>

using namespace std;

namespace NES {
using Runtime::TupleBuffer;

const uint64_t buffersManaged = 8 * 1024;
const uint64_t bufferSize = 32 * 1024;

struct TestStruct {
    int64_t id;
    int64_t one;
    int64_t value;
};

static constexpr auto NSOURCE_RETRIES = 100;
static constexpr auto NSOURCE_RETRY_WAIT = std::chrono::milliseconds(5);

namespace Network {
class NetworkStackIntegrationTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("NetworkStackIntegrationTest.log", NES::LOG_DEBUG);
        NES_INFO("SetUpTestCase NetworkStackIntegrationTest");
    }

    void SetUp() {
        Testing::NESBaseTest::SetUp();
        dataPort1 = Testing::NESBaseTest::getAvailablePort();
        dataPort2 = Testing::NESBaseTest::getAvailablePort();
    }

    void TearDown() {
        dataPort1.reset();
        dataPort2.reset();
        Testing::NESBaseTest::TearDown();
    }

    static void TearDownTestCase() { NES_INFO("TearDownTestCase NetworkStackIntegrationTest."); }

  protected:
    Testing::BorrowedPortPtr dataPort1;
    Testing::BorrowedPortPtr dataPort2;
};

class TestSink : public SinkMedium {
  public:
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    TestSink(const SchemaPtr& schema, Runtime::NodeEnginePtr nodeEngine, const Runtime::BufferManagerPtr& bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), nodeEngine, 0) {
        // nop
    }

    bool writeData(Runtime::TupleBuffer& input_buffer, Runtime::WorkerContextRef) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink:\n" << Util::prettyPrintTupleBuffer(input_buffer, getSchemaPtr()));

        uint64_t sum = 0;
        for (uint64_t i = 0; i < input_buffer.getNumberOfTuples(); ++i) {
            sum += input_buffer.getBuffer<TestStruct>()[i].value;
        }

        completed.set_value(sum);
        return true;
    }

    std::string toString() const override { return ""; }

    void setup() override{};

    void shutdown() override{};

    ~TestSink() override = default;

    std::mutex m;
    std::promise<uint64_t> completed;
};

void fillBuffer(TupleBuffer& buf, const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout) {
    auto recordIndexFields = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, buf);
    auto fields01 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, memoryLayout, buf);
    auto fields02 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(2, memoryLayout, buf);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 1;
        fields02[recordIndex] = recordIndex % 2;
    }
    buf.setNumberOfTuples(10);
}

TEST_F(NetworkStackIntegrationTest, testNetworkSource) {
    auto queryCompilerConfiguration = Configurations::QueryCompilerConfiguration();
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
    auto nodeEngine =
        Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", *dataPort1, {physicalSource}, 1, bufferSize, buffersManaged, 64, 64, queryCompilerConfiguration);
    auto netManager = nodeEngine->getNetworkManager();

    NesPartition nesPartition{1, 22, 33, 44};
    auto nodeLocation = NodeLocation(0, "127.0.0.1", *dataPort1);
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());
    auto networkSource = std::make_shared<NetworkSource>(schema,
                                                         nodeEngine->getBufferManager(),
                                                         nodeEngine->getQueryManager(),
                                                         netManager,
                                                         nesPartition,
                                                         nodeLocation,
                                                         64,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES);
    EXPECT_TRUE(networkSource->start());

    ASSERT_EQ(nodeEngine->getPartitionManager()->getSubpartitionConsumerCounter(nesPartition), 1UL);
    EXPECT_TRUE(networkSource->stop());
    netManager->unregisterSubpartitionConsumer(nesPartition);
    ASSERT_EQ(nodeEngine->getPartitionManager()->getConsumerRegistrationStatus(nesPartition),
              PartitionRegistrationStatus::Deleted);
    ASSERT_TRUE(nodeEngine->stop());
}

TEST_F(NetworkStackIntegrationTest, testStartStopNetworkSrcSink) {
    auto defaultSourceType = DefaultSourceType::create();
    auto queryCompilerConfiguration = Configurations::QueryCompilerConfiguration();
    auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
    auto nodeEngine =
        Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", *dataPort1, {physicalSource}, 1, bufferSize, buffersManaged, 64, 64, queryCompilerConfiguration);
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    auto networkSource = std::make_shared<NetworkSource>(schema,
                                                         nodeEngine->getBufferManager(),
                                                         nodeEngine->getQueryManager(),
                                                         nodeEngine->getNetworkManager(),
                                                         nesPartition,
                                                         nodeLocation,
                                                         64,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES);
    EXPECT_TRUE(networkSource->start());

    auto networkSink = std::make_shared<NetworkSink>(schema,
                                                     *dataPort1,
                                                     0,
                                                     nodeLocation,
                                                     nesPartition,
                                                     nodeEngine,
                                                     1,
                                                     NSOURCE_RETRY_WAIT,
                                                     NSOURCE_RETRIES);

    nodeEngine->getNetworkManager()->unregisterSubpartitionConsumer(nesPartition);

    EXPECT_TRUE(networkSource->stop());
    EXPECT_TRUE(nodeEngine->stop());
    networkSource.reset();
    nodeEngine.reset();
}

template<typename MockedNodeEngine, typename... ExtraParameters>
std::shared_ptr<MockedNodeEngine> createMockedEngine(const std::string& hostname,
                                                     uint16_t port,
                                                     uint64_t bufferSize,
                                                     uint64_t numBuffers,
                                                     ExtraParameters&&... extraParams) {
    try {
        auto defaultSourceType = DefaultSourceType::create();
        auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
        std::vector<PhysicalSourcePtr> physicalSources{physicalSource};
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        std::vector<Runtime::BufferManagerPtr> bufferManagers = {
            std::make_shared<Runtime::BufferManager>(bufferSize, numBuffers)};
        auto queryManager = std::make_shared<Runtime::QueryManager>(bufferManagers, 0, 1, nullptr);
        auto bufferStorage = std::make_shared<Runtime::BufferStorage>();
        auto networkManagerCreator = [=](const Runtime::NodeEnginePtr& engine) {
            return Network::NetworkManager::create(0,
                                                   hostname,
                                                   port,
                                                   Network::ExchangeProtocol(partitionManager, engine),
                                                   bufferManagers[0]);
        };
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        options->setNumSourceLocalBuffers(12);

        auto compiler = QueryCompilation::DefaultQueryCompiler::create(options, phaseFactory, jitCompiler);

        return std::make_shared<MockedNodeEngine>(std::move(physicalSources),
                                                  std::make_shared<Runtime::HardwareManager>(),
                                                  std::move(bufferManagers),
                                                  std::move(queryManager),
                                                  std::move(bufferStorage),
                                                  std::move(networkManagerCreator),
                                                  std::move(partitionManager),
                                                  std::move(compiler),
                                                  std::forward<ExtraParameters>(extraParams)...);

    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
        return nullptr;
    }
}

TEST_F(NetworkStackIntegrationTest, testNetworkSourceSink) {
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    uint64_t totalNumBuffer = 100;

    static constexpr int numSendingThreads = 4;
    auto sendingThreads = std::vector<std::thread>();
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    NodeLocation nodeLocation{0, "127.0.0.1", *dataPort1};
    NesPartition nesPartition{1, 22, 33, 44};

    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        NesPartition nesPartition;
        std::promise<bool>& completed;
        atomic<int> eosCnt = 0;
        atomic<int>& bufferCnt;

        explicit MockedNodeEngine(std::vector<PhysicalSourcePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  NES::Runtime::QueryManagerPtr&& queryManager,
                                  Runtime::BufferStoragePtr&& bufferStorage,
                                  std::function<Network::NetworkManagerPtr(NES::Runtime::NodeEnginePtr)>&& networkManagerCreator,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& queryCompiler,
                                  std::promise<bool>& completed,
                                  NesPartition nesPartition,
                                  std::atomic<int>& bufferCnt)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryManager),
                         std::move(bufferStorage),
                         std::move(networkManagerCreator),
                         std::move(partitionManager),
                         std::move(queryCompiler),
                         std::make_shared<NES::Runtime::StateManager>(0),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<NES::Experimental::MaterializedView::MaterializedViewManager>(),
                         0,
                         64,
                         64,
                         12),
              nesPartition(nesPartition), completed(completed), bufferCnt(bufferCnt) {}
        void onDataBuffer(Network::NesPartition id, TupleBuffer&) override {
            if (nesPartition == id) {
                bufferCnt++;
            }
            ASSERT_EQ(id, nesPartition);
        }

        void onEndOfStream(Network::Messages::EndOfStreamMessage) override {
            eosCnt++;
            if (eosCnt == 1) {
                completed.set_value(true);
            }
        }

        void onServerError(Network::Messages::ErrorMessage ex) override {
            if (ex.getErrorType() != Messages::ErrorType::PartitionNotRegisteredError) {
                completed.set_exception(make_exception_ptr(runtime_error("Error")));
            }
        }

        void onChannelError(Network::Messages::ErrorMessage message) override { NodeEngine::onChannelError(message); }
    };

    auto nodeEngine1 = createMockedEngine<MockedNodeEngine>("127.0.0.1",
                                                            *dataPort1,
                                                            bufferSize,
                                                            buffersManaged,
                                                            completed,
                                                            nesPartition,
                                                            bufferCnt);
    try {
        auto netManager = nodeEngine1->getNetworkManager();

        std::thread receivingThread([&]() {
            // register the incoming channel
            auto source = std::make_shared<NetworkSource>(schema,
                                                          nodeEngine1->getBufferManager(),
                                                          nodeEngine1->getQueryManager(),
                                                          netManager,
                                                          nesPartition,
                                                          nodeLocation,
                                                          64,
                                                          NSOURCE_RETRY_WAIT,
                                                          NSOURCE_RETRIES);
            EXPECT_TRUE(source->start());
            EXPECT_EQ(nodeEngine1->getPartitionManager()->getConsumerRegistrationStatus(nesPartition),
                      PartitionRegistrationStatus::Registered);
            completed.get_future().get();
            EXPECT_TRUE(source->stop());
            auto rt = Runtime::ReconfigurationMessage(-1, Runtime::Destroy, source);
            source->postReconfigurationCallback(rt);
            EXPECT_EQ(nodeEngine1->getPartitionManager()->getConsumerRegistrationStatus(nesPartition),
                      PartitionRegistrationStatus::Deleted);
        });

        auto defaultSourceType = DefaultSourceType::create();
        auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
        auto nodeEngine2 =
            Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", *dataPort2, {physicalSource}, 1, bufferSize, buffersManaged, 64, 64, Configurations::QueryCompilerConfiguration());

        auto networkSink = std::make_shared<NetworkSink>(schema,
                                                         0,
                                                         0,
                                                         nodeLocation,
                                                         nesPartition,
                                                         nodeEngine2,
                                                         numSendingThreads,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES);
        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
                Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), nodeEngine2->getBufferManager(), 64);
                auto rt = Runtime::ReconfigurationMessage(0, Runtime::Initialize, networkSink, std::make_any<uint32_t>(1));
                networkSink->reconfigure(rt, workerContext);
                for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                    auto buffer = nodeEngine2->getBufferManager()->getBufferBlocking();
                    for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                        buffer.getBuffer<uint64_t>()[j] = j;
                    }
                    buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                    networkSink->writeData(buffer, workerContext);
                    usleep(rand() % 10000 + 1000);
                }
            });
            sendingThreads.emplace_back(std::move(sendingThread));
        }

        for (std::thread& t : sendingThreads) {
            if (t.joinable()) {
                t.join();
            }
        }
        receivingThread.join();
        ASSERT_TRUE(nodeEngine2->stop());
    } catch (...) {
        FAIL();
    }
    auto const bf = bufferCnt.load();
    ASSERT_TRUE(bf > 0);
    ASSERT_EQ(static_cast<std::size_t>(bf), numSendingThreads * totalNumBuffer);
    ASSERT_EQ(nodeEngine1->getPartitionManager()->getConsumerRegistrationStatus(nesPartition),
              PartitionRegistrationStatus::Deleted);
    ASSERT_TRUE(nodeEngine1->stop());
    nodeEngine1 = nullptr;
}

namespace detail {
class TestSourceWithLatch : public DefaultSource {
  public:
    explicit TestSourceWithLatch(const SchemaPtr& schema,
                                 const Runtime::BufferManagerPtr& bufferManager,
                                 const Runtime::QueryManagerPtr& queryManager,
                                 OperatorId operatorId,
                                 size_t numSourceLocalBuffers,
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                 ThreadBarrierPtr latch)
        : DefaultSource(schema,
                        bufferManager,
                        queryManager,
                        /*bufferCnt*/ 1,
                        /*frequency*/ 1000,
                        operatorId,
                        numSourceLocalBuffers,
                        successors),
          latch(std::move(latch)) {}

    void runningRoutine() override {
        latch->wait();
        DataSource::runningRoutine();
    }

  private:
    ThreadBarrierPtr latch;
};
}// namespace detail

TEST_F(NetworkStackIntegrationTest, testQEPNetworkSinkSource) {

    auto numQueries = 10;
    auto numThreads = 8;
    SchemaPtr schema = Schema::create()
                           ->addField("test$id", DataTypeFactory::createInt64())
                           ->addField("test$one", DataTypeFactory::createInt64())
                           ->addField("test$value", DataTypeFactory::createInt64());

    auto defaultSourceType = DefaultSourceType::create();
    std::vector<PhysicalSourcePtr> physicalSources;
    for (auto i = 0; i < numQueries; ++i) {
        auto str = std::to_string(i);
        auto physicalSource = PhysicalSource::create("default_logical"s + str, "default"s + str, defaultSourceType);
        physicalSources.emplace_back(physicalSource);
    }

    auto latch = std::make_shared<ThreadBarrier>(numQueries);

    auto nodeEngineSender = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                         31337,
                                                                         physicalSources,
                                                                         numThreads,
                                                                         bufferSize,
                                                                         buffersManaged,
                                                                         64,
                                                                         12,
                                                                         Configurations::QueryCompilerConfiguration());
    auto netManagerSender = nodeEngineSender->getNetworkManager();
    NodeLocation nodeLocationSender = netManagerSender->getServerLocation();
    auto nodeEngineReceiver = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                           31338,
                                                                           physicalSources,
                                                                           numThreads,
                                                                           bufferSize,
                                                                           buffersManaged,
                                                                           64,
                                                                           12,
                                                                           Configurations::QueryCompilerConfiguration());
    auto netManagerReceiver = nodeEngineReceiver->getNetworkManager();
    NodeLocation nodeLocationReceiver = netManagerReceiver->getServerLocation();

    std::vector<std::shared_ptr<TestSink>> finalSinks;

    uint32_t subPlanId = 0;
    for (auto i = 0; i < numQueries; ++i) {
        NesPartition nesPartition{Network::QueryId(i),
                                  Network::OperatorId(i * 22),
                                  Network::PartitionId(i * 33),
                                  Network::SubpartitionId(i * 44)};
        // create NetworkSink
        auto networkSourceDescriptor1 = std::make_shared<TestUtils::TestSourceDescriptor>(
            schema,
            [&](OperatorId,
                const SourceDescriptorPtr&,
                const Runtime::NodeEnginePtr&,
                size_t numSourceLocalBuffers,
                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) -> DataSourcePtr {
                return std::make_shared<NetworkSource>(schema,
                                                       nodeEngineReceiver->getBufferManager(),
                                                       nodeEngineReceiver->getQueryManager(),
                                                       netManagerReceiver,
                                                       nesPartition,
                                                       nodeLocationSender,
                                                       numSourceLocalBuffers,
                                                       NSOURCE_RETRY_WAIT,
                                                       NSOURCE_RETRIES,
                                                       successors);
            });

        auto testSink =
            std::make_shared<TestSink>(schema, nodeEngineReceiver, nodeEngineReceiver->getBufferManager());
        finalSinks.emplace_back(testSink);
        auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

        auto query = TestQuery::from(networkSourceDescriptor1).sink(testSinkDescriptor);

        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
        queryPlan->setQueryId(i);
        queryPlan->setQuerySubPlanId(subPlanId++);
        auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngineReceiver);
        auto queryCompiler = TestUtils::createTestQueryCompiler();
        auto result = queryCompiler->compileQuery(request);
        auto builderReceiverQEP = result->getExecutableQueryPlan();

        // creating query plan
        auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
            schema,
            [&](OperatorId,
                const SourceDescriptorPtr&,
                const Runtime::NodeEnginePtr&,
                size_t numSourceLocalBuffers,
                std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
                return std::make_shared<detail::TestSourceWithLatch>(schema,
                                                                     nodeEngineSender->getBufferManager(),
                                                                     nodeEngineSender->getQueryManager(),
                                                                     OperatorId(1 + i),
                                                                     numSourceLocalBuffers,
                                                                     std::move(successors),
                                                                     latch);
            });

    auto networkSink = std::make_shared<NetworkSink>(schema,
                                                     i,
                                                     subPlanId,
                                                     nodeLocationReceiver,
                                                     nesPartition,
                                                     nodeEngineSender,
                                                     1,
                                                     NSOURCE_RETRY_WAIT,
                                                     NSOURCE_RETRIES);
    auto networkSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(networkSink);
    auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(networkSinkDescriptor);

        auto queryPlan2 = typeInferencePhase->execute(query2.getQueryPlan());
        queryPlan2->setQueryId(i);
        queryPlan2->setQuerySubPlanId(subPlanId++);
        auto request2 = QueryCompilation::QueryCompilationRequest::create(queryPlan2, nodeEngineSender);
        auto result2 = queryCompiler->compileQuery(request2);
        auto builderGeneratorQEP = result2->getExecutableQueryPlan();
        ASSERT_TRUE(nodeEngineSender->registerQueryInNodeEngine(builderGeneratorQEP));
        ASSERT_TRUE(nodeEngineReceiver->registerQueryInNodeEngine(builderReceiverQEP));
        ASSERT_TRUE(nodeEngineSender->startQuery(builderGeneratorQEP->getQueryId()));
        ASSERT_TRUE(nodeEngineReceiver->startQuery(builderReceiverQEP->getQueryId()));
    }

    ASSERT_EQ(numQueries, finalSinks.size());

    //    for (auto i = 0; i < numQueries; ++i) {
    //        ASSERT_TRUE(nodeEngineSender->startQuery(i));
    //        ASSERT_TRUE(nodeEngineReceiver->startQuery(i));
    //        usleep(10 * 1000); // 10ms
    //    }

    for (const auto& testSink : finalSinks) {
        ASSERT_EQ(10ULL, testSink->completed.get_future().get());
    }

    for (auto i = 0; i < numQueries; ++i) {
        ASSERT_TRUE(nodeEngineSender->undeployQuery(i));
        ASSERT_TRUE(nodeEngineReceiver->undeployQuery(i));
    }
    ASSERT_TRUE(nodeEngineSender->stop());
    ASSERT_TRUE(nodeEngineReceiver->stop());
}


namespace detail {
struct TestEvent {
    explicit TestEvent(Runtime::EventType ev, uint32_t test) : ev(ev), test(test) {}

    Runtime::EventType getEventType() const { return ev; }

    uint32_t testValue() const { return test; }

    Runtime::EventType ev;
    uint32_t test;
};

}// namespace detail

TEST_F(NetworkStackIntegrationTest, testSendEvent) {
    std::promise<bool> completedProm;

    std::atomic<bool> eventReceived = false;
    auto nesPartition = NesPartition(1, 22, 333, 444);

    try {

        class ExchangeListener : public ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<bool>& eventReceived;

            ExchangeListener(std::atomic<bool>& bufferReceived, std::promise<bool>& completedProm)
                : completedProm(completedProm), eventReceived(bufferReceived) {}

            void onDataBuffer(NesPartition, TupleBuffer&) override {}

            void onEvent(NesPartition, Runtime::BaseEvent& event) override {
                eventReceived = event.getEventType() == Runtime::EventType::kCustomEvent
                    && dynamic_cast<Runtime::CustomEventWrapper&>(event).data<detail::TestEvent>()->testValue() == 123;
                ASSERT_TRUE(eventReceived);
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}

            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   *dataPort2,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(eventReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, this] {
            // register the incoming channel
            sleep(3);// intended stalling to simulate latency
            auto nodeLocation = NodeLocation(0, "127.0.0.1", *dataPort2);
            netManager->registerSubpartitionConsumer(nesPartition, nodeLocation, std::make_shared<DataEmitterImpl>());
            auto future = completedProm.get_future();
            if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
                ASSERT_TRUE(future.get());
            } else {
                NES_ERROR("NetworkStackIntegrationTest: Receiving thread timed out!");
            }
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", *dataPort2);
        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackIntegrationTest: Error in registering DataChannel!");
            completedProm.set_value(false);
        } else {
            senderChannel->sendEvent<detail::TestEvent>(Runtime::EventType::kCustomEvent, 123);
            senderChannel.reset();
        }

        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_TRUE(eventReceived.load());
}

TEST_F(NetworkStackIntegrationTest, testSendEventBackward) {

    NodeLocation nodeLocationSender{0, "127.0.0.1", *dataPort1};
    NodeLocation nodeLocationReceiver{0, "127.0.0.1", *dataPort2};
    NesPartition nesPartition{1, 22, 33, 44};
    SchemaPtr schema = Schema::create()
                           ->addField("test$id", DataTypeFactory::createInt64())
                           ->addField("test$one", DataTypeFactory::createInt64())
                           ->addField("test$value", DataTypeFactory::createInt64());
    auto queryCompilerConfiguration = Configurations::QueryCompilerConfiguration();

    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
    auto nodeEngineSender = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                         *dataPort1,
                                                                         {physicalSource},
                                                                         1,
                                                                         bufferSize,
                                                                         buffersManaged,
                                                                         64,
                                                                         12,
                                                                         queryCompilerConfiguration);
    auto nodeEngineReceiver = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                           *dataPort2,
                                                                           {physicalSource},
                                                                           1,
                                                                           bufferSize,
                                                                           buffersManaged,
                                                                           64,
                                                                           12,
                                                                           queryCompilerConfiguration);
    // create NetworkSink

    class TestNetworkSink : public NetworkSink {
      public:
        using NetworkSink::NetworkSink;

      protected:
        void onEvent(Runtime::BaseEvent& event) override {
            NetworkSink::onEvent(event);
            bool eventReceived = event.getEventType() == Runtime::EventType::kCustomEvent
                && dynamic_cast<Runtime::CustomEventWrapper&>(event).data<detail::TestEvent>()->testValue() == 123;
            completed.set_value(eventReceived);
        }

      public:
        std::promise<bool> completed;
    };

    auto networkSourceDescriptor1 = std::make_shared<TestUtils::TestSourceDescriptor>(
        schema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) -> DataSourcePtr {
            return std::make_shared<NetworkSource>(schema,
                                                   nodeEngineReceiver->getBufferManager(),
                                                   nodeEngineReceiver->getQueryManager(),
                                                   nodeEngineReceiver->getNetworkManager(),
                                                   nesPartition,
                                                   nodeLocationSender,
                                                   numSourceLocalBuffers,
                                                   NSOURCE_RETRY_WAIT,
                                                   NSOURCE_RETRIES,
                                                   successors);
        });

    class TestSinkEvent : public SinkMedium {
      public:
        SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

        explicit TestSinkEvent(const SchemaPtr& schema,
                               Runtime::NodeEnginePtr nodeEngine,
                               const Runtime::BufferManagerPtr& bufferManager)
            : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), nodeEngine, 0) {
            // nop
        }

        bool writeData(Runtime::TupleBuffer&, Runtime::WorkerContextRef context) override {
            auto parentPlan = nodeEngine->getQueryManager()->getQueryExecutionPlan(querySubPlanId);
            for (auto& dataSources : parentPlan->getSources()) {
                auto senderChannel = context.getEventOnlyNetworkChannel(dataSources->getOperatorId());
                senderChannel->sendEvent<detail::TestEvent>(Runtime::EventType::kCustomEvent, 123);
            }
            return true;
        }

        void setup() override {}
        void shutdown() override {}
        string toString() const override { return std::string(); }
    };

    class TestSourceEvent : public DefaultSource {
      public:
        explicit TestSourceEvent(const SchemaPtr& schema,
                                 const Runtime::BufferManagerPtr& bufferManager,
                                 const Runtime::QueryManagerPtr& queryManager,
                                 OperatorId operatorId,
                                 size_t numSourceLocalBuffers,
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
            : DefaultSource(schema,
                            bufferManager,
                            queryManager,
                            /*bufferCnt*/ 1,
                            /*frequency*/ 1000,
                            operatorId,
                            numSourceLocalBuffers,
                            successors) {}
    };

    auto testSink =
        std::make_shared<TestSinkEvent>(schema, nodeEngineReceiver, nodeEngineReceiver->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(networkSourceDescriptor1).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    queryPlan->setQueryId(0);
    queryPlan->setQuerySubPlanId(0);
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngineReceiver);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto builderReceiverQEP = result->getExecutableQueryPlan();

    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        schema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return std::make_shared<TestSourceEvent>(schema,
                                                     nodeEngineSender->getBufferManager(),
                                                     nodeEngineSender->getQueryManager(),
                                                     1,
                                                     numSourceLocalBuffers,
                                                     std::move(successors));
        });

    auto networkSink = std::make_shared<TestNetworkSink>(schema,
                                                         0,
                                                         1,
                                                         nodeLocationReceiver,
                                                         nesPartition,
                                                         nodeEngineSender,
                                                         1,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES);
    auto networkSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(networkSink);
    auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(networkSinkDescriptor);

    auto queryPlan2 = typeInferencePhase->execute(query2.getQueryPlan());
    queryPlan2->setQueryId(0);
    queryPlan2->setQuerySubPlanId(1);
    auto request2 = QueryCompilation::QueryCompilationRequest::create(queryPlan2, nodeEngineSender);
    queryCompiler = TestUtils::createTestQueryCompiler();
    auto result2 = queryCompiler->compileQuery(request2);
    auto builderGeneratorQEP = result2->getExecutableQueryPlan();
    ASSERT_TRUE(nodeEngineSender->registerQueryInNodeEngine(builderGeneratorQEP));
    ASSERT_TRUE(nodeEngineReceiver->registerQueryInNodeEngine(builderReceiverQEP));

    ASSERT_TRUE(nodeEngineSender->startQuery(builderGeneratorQEP->getQueryId()));
    ASSERT_TRUE(nodeEngineReceiver->startQuery(builderReceiverQEP->getQueryId()));

    auto future = networkSink->completed.get_future();
    ASSERT_TRUE(future.get());

    ASSERT_TRUE(nodeEngineSender->undeployQuery(builderGeneratorQEP->getQueryId()));
    ASSERT_TRUE(nodeEngineReceiver->undeployQuery(builderReceiverQEP->getQueryId()));
    ASSERT_TRUE(nodeEngineSender->stop());
    ASSERT_TRUE(nodeEngineReceiver->stop());
}

}// namespace Network
}// namespace NES