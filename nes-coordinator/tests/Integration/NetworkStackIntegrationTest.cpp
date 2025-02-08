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
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sources/DefaultSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtils.hpp>
#include <Util/ThreadBarrier.hpp>
#include <gtest/gtest.h>
#include <random>
#include <utility>

using namespace std;

namespace NES {
using Runtime::TupleBuffer;
class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;
const uint64_t buffersManaged = 8 * 1024;
const uint64_t bufferSize = 32 * 1024;

struct TestStruct {
    int64_t id;
    int64_t one;
    int64_t value;
};

static constexpr auto NSOURCE_RETRIES = 100;
static constexpr auto NSOURCE_RETRY_WAIT = std::chrono::milliseconds(5);
static constexpr auto INITIAL_VERSION = 0;
static constexpr auto DEFAULT_NUMBER_OF_ORIGINS = 1;

namespace Network {
class NetworkStackIntegrationTest : public Testing::BaseIntegrationTest, public testing::WithParamInterface<bool> {
  public:
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NetworkStackIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("SetUpTestCase NetworkStackIntegrationTest");
    }

    void SetUp() {
        Testing::BaseIntegrationTest::SetUp();
        dataPort1 = Testing::BaseIntegrationTest::getAvailablePort();
        dataPort2 = Testing::BaseIntegrationTest::getAvailablePort();
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void TearDown() {
        dataPort1.reset();
        dataPort2.reset();
        Testing::BaseIntegrationTest::TearDown();
    }

    static void TearDownTestCase() { NES_INFO("TearDownTestCase NetworkStackIntegrationTest."); }

  protected:
    Testing::BorrowedPortPtr dataPort1;
    Testing::BorrowedPortPtr dataPort2;
    static constexpr DecomposedQueryId defaultDecomposedQueryPlanId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    static constexpr SharedQueryId defaultSharedQueryId = INVALID_SHARED_QUERY_ID;
};

class TestSink : public SinkMedium {
  public:
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    TestSink(const SchemaPtr& schema,
             Runtime::NodeEnginePtr nodeEngine,
             const Runtime::BufferManagerPtr& bufferManager,
             uint32_t numOfProducers = 1,
             SharedQueryId sharedQueryId = INVALID_SHARED_QUERY_ID,
             DecomposedQueryId decomposedQueryId = INVALID_DECOMPOSED_QUERY_PLAN_ID,
             DecomposedQueryPlanVersion decomposedQueryVersion = INVALID_DECOMPOSED_QUERY_PLAN_VERSION)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager),
                     nodeEngine,
                     numOfProducers,
                     sharedQueryId,
                     decomposedQueryId,
                     decomposedQueryVersion) {
        // nop
    }

    bool writeData(Runtime::TupleBuffer& input_buffer, Runtime::WorkerContextRef) override {
        std::unique_lock lock(m);

        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(getSchemaPtr(), input_buffer.getBufferSize());
        auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer(rowLayout, input_buffer);
        std::stringstream testTupleBufferAsString;
        testTupleBufferAsString << testTupleBuffer;
        NES_TRACE("TestSink:\n{}", testTupleBufferAsString.str());

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

template<typename MockedNodeEngine, typename... ExtraParameters>
std::shared_ptr<MockedNodeEngine> createMockedEngine(const std::string& hostname,
                                                     uint16_t port,
                                                     uint64_t bufferSize,
                                                     uint64_t numBuffers,
                                                     ExtraParameters&&... extraParams) {
    try {
        class DummyQueryListener : public AbstractQueryStatusListener {
          public:
            virtual ~DummyQueryListener() {}

            bool canTriggerEndOfStream(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
                return true;
            }
            bool notifySourceTermination(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
                return true;
            }
            bool notifyQueryFailure(SharedQueryId, DecomposedQueryId, std::string) override { return true; }
            bool
            notifyQueryStatusChange(SharedQueryId, DecomposedQueryId, Runtime::Execution::ExecutableQueryPlanStatus) override {
                return true;
            }
            bool propagateNeighbourInformation(std::vector<std::pair<WorkerId, std::string>>) override {
                return true;
            }
            void offloadCheckpoint(uint64_t, std::vector<char>) override {
                return;
            }
            void rpcTrimCheckpoint(uint64_t, uint64_t) override {
                return;
            }
            bool requestSubQueryRemoval(SharedQueryId, DecomposedQueryId) override {
                return true;
            }
        };
        auto defaultSourceType = DefaultSourceType::create("default_logical", "default");
        std::vector<PhysicalSourceTypePtr> physicalSources{defaultSourceType};
        auto partitionManager = std::make_shared<Network::PartitionManager>();

        std::vector<Runtime::BufferManagerPtr> bufferManagers = {
            std::make_shared<Runtime::BufferManager>(bufferSize, numBuffers)};
        auto hwManager = std::make_shared<Runtime::HardwareManager>();
        auto queryManager = std::make_shared<Runtime::DynamicQueryManager>(std::make_shared<DummyQueryListener>(),
                                                                           bufferManagers,
                                                                           INVALID_WORKER_NODE_ID,
                                                                           1,
                                                                           hwManager,
                                                                           100);
        auto networkManagerCreator = [=](const Runtime::NodeEnginePtr& engine) {
            return Network::NetworkManager::create(INVALID_WORKER_NODE_ID,
                                                   hostname,
                                                   port,
                                                   Network::ExchangeProtocol(partitionManager, engine),
                                                   bufferManagers[0]);
        };
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto operatorHandlerStore = OperatorHandlerStore::create();
        auto options = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        options->setNumSourceLocalBuffers(12);

        auto compiler = QueryCompilation::NautilusQueryCompiler::create(options, phaseFactory, operatorHandlerStore);

        return std::make_shared<MockedNodeEngine>(std::move(physicalSources),
                                                  std::move(hwManager),
                                                  std::move(bufferManagers),
                                                  std::move(queryManager),
                                                  std::move(networkManagerCreator),
                                                  std::move(partitionManager),
                                                  std::move(operatorHandlerStore),
                                                  std::move(compiler),
                                                  std::forward<ExtraParameters>(extraParams)...);

    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine {}", err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
        return nullptr;
    }
}

TEST_P(NetworkStackIntegrationTest, testNetworkSourceSink) {
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    uint64_t totalNumBuffer = 100;

    static constexpr int numSendingThreads = 4;
    auto sendingThreads = std::vector<std::thread>();
    auto schema = TestSchemas::getSchemaTemplate("id_u64");

    NodeLocation nodeLocationSource{INVALID_WORKER_NODE_ID, "127.0.0.1", *dataPort1};
    NodeLocation nodeLocationSink{INVALID_WORKER_NODE_ID, "127.0.0.1", *dataPort2};

    NesPartition nesPartition{SharedQueryId(1), OperatorId(22), PartitionId(33), SubpartitionId(44)};
    ThreadBarrierPtr sinkShutdownBarrier = std::make_shared<ThreadBarrier>(numSendingThreads + 1);

    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        NesPartition nesPartition;
        std::promise<bool>& completed;
        atomic<int> eosCnt = 0;
        atomic<int>& bufferCnt;

        explicit MockedNodeEngine(std::vector<PhysicalSourceTypePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  NES::Runtime::QueryManagerPtr&& queryManager,
                                  std::function<Network::NetworkManagerPtr(NES::Runtime::NodeEnginePtr)>&& networkManagerCreator,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  OperatorHandlerStorePtr operatorHandlerStore,
                                  QueryCompilation::QueryCompilerPtr&& queryCompiler,
                                  std::promise<bool>& completed,
                                  NesPartition nesPartition,
                                  std::atomic<int>& bufferCnt)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryManager),
                         std::move(networkManagerCreator),
                         std::move(partitionManager),
                         std::move(operatorHandlerStore),
                         std::move(queryCompiler),
                         std::make_shared<DummyQueryListener>(),
                         std::make_shared<NES::Runtime::OpenCLManager>(),
                         INVALID_WORKER_NODE_ID,
                         64,
                         64,
                         12,
                         false),
              nesPartition(nesPartition), completed(completed), bufferCnt(bufferCnt) {}

        ~MockedNodeEngine() = default;

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

    try {
        std::thread receivingThread([&]() {
            auto recvEngine = createMockedEngine<MockedNodeEngine>("127.0.0.1",
                                                                   *dataPort1,
                                                                   bufferSize,
                                                                   buffersManaged,
                                                                   completed,
                                                                   nesPartition,
                                                                   bufferCnt);
            // register the incoming channel
            auto sink = std::make_shared<NullOutputSink>(recvEngine,
                                                         1,
                                                         INVALID_SHARED_QUERY_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION);
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> succ = {sink};
            auto uniqueId = OperatorId(1);
            auto source = std::make_shared<NetworkSource>(schema,
                                                          recvEngine->getBufferManager(),
                                                          recvEngine->getQueryManager(),
                                                          recvEngine->getNetworkManager(),
                                                          nesPartition,
                                                          nodeLocationSink,
                                                          64,
                                                          NSOURCE_RETRY_WAIT,
                                                          NSOURCE_RETRIES,
                                                          std::move(succ),
                                                          INITIAL_VERSION,
                                                          uniqueId);
            auto qep = Runtime::Execution::ExecutableQueryPlan::create(INVALID_SHARED_QUERY_ID,
                                                                       INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                                       INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                                       {source},
                                                                       {sink},
                                                                       {},
                                                                       recvEngine->getQueryManager(),
                                                                       recvEngine->getBufferManager());
            recvEngine->getQueryManager()->registerExecutableQueryPlan(qep);
            ASSERT_EQ(recvEngine->getPartitionManager()->getConsumerRegistrationStatus(nesPartition),
                      PartitionRegistrationStatus::Registered);
            completed.get_future().get();
            sinkShutdownBarrier->wait();
            while (!(qep->getStatus() == Runtime::Execution::ExecutableQueryPlanStatus::Stopped
                     || qep->getStatus() == Runtime::Execution::ExecutableQueryPlanStatus::Finished)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            ASSERT_TRUE(recvEngine->stop());
        });

        auto defaultSourceType = DefaultSourceType::create("default_logical", "default");
        auto workerConfig2 = WorkerConfiguration::create();
        workerConfig2->dataPort = *dataPort2;
        workerConfig2->bufferSizeInBytes = bufferSize;
        workerConfig2->physicalSourceTypes.add(defaultSourceType);
        auto connectAsync = GetParam();
        workerConfig2->connectSinksAsync.setValue(connectAsync);
        auto nodeEngineBuilder2 =
            Runtime::NodeEngineBuilder::create(workerConfig2).setQueryStatusListener(std::make_shared<DummyQueryListener>());
        auto sendEngine = nodeEngineBuilder2.build();

        //when async connecting is used, the sink will not finish the normal connecting process
        //this is because the sending threads do not mimic the behaviour of the actual worker thread pool so far that
        //the the threads which write the data receive the reconfiguration messages. The test passes because the sinks
        //shutdown sequence makes a connection attempt in order to flush the data, not because it connects normally
        auto networkSink = std::make_shared<NetworkSink>(schema,
                                                         INVALID_OPERATOR_ID,
                                                         INVALID_SHARED_QUERY_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                         nodeLocationSource,
                                                         nesPartition,
                                                         sendEngine,
                                                         1,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES,
                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                         INITIAL_VERSION);
        networkSink->preSetup();
        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
                Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), sendEngine->getBufferManager(), 64);
                auto rt = Runtime::ReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                          INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                          INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                          Runtime::ReconfigurationType::Initialize,
                                                          networkSink,
                                                          std::make_any<uint32_t>(1));
                networkSink->reconfigure(rt, workerContext);
                for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                    auto buffer = sendEngine->getBufferManager()->getBufferBlocking();
                    for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                        buffer.getBuffer<uint64_t>()[j] = j;
                    }
                    buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                    networkSink->writeData(buffer, workerContext);
                    usleep(rand() % 10000 + 1000);
                }
                auto rt2 = Runtime::ReconfigurationMessage(INITIAL_SHARED_QUERY_ID,
                                                           INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                           INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                           Runtime::ReconfigurationType::SoftEndOfStream,
                                                           networkSink);
                networkSink->reconfigure(rt2, workerContext);
                sinkShutdownBarrier->wait();
            });
            sendingThreads.emplace_back(std::move(sendingThread));
        }

        for (std::thread& t : sendingThreads) {
            if (t.joinable()) {
                t.join();
            }
        }
        networkSink->shutdown();
        receivingThread.join();
        ASSERT_TRUE(sendEngine->stop());
    } catch (...) {
        FAIL();
    }
    auto const bf = bufferCnt.load();
    ASSERT_TRUE(bf > 0);
    ASSERT_EQ(static_cast<std::size_t>(bf), numSendingThreads * totalNumBuffer);
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
                        /*frequency*/ 0,
                        operatorId,
                        /*oridingid*/ INVALID_ORIGIN_ID,
                        INVALID_STATISTIC_ID,
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
    SchemaPtr schema = TestSchemas::getSchemaTemplate("id_one_val_64")->updateSourceName("test");

    std::vector<PhysicalSourceTypePtr> physicalSourceTypes;
    for (auto i = 0; i < numQueries; ++i) {
        auto str = std::to_string(i);
        auto defaultSourceType = DefaultSourceType::create("default_logical"s + str, "default"s + str);
        physicalSourceTypes.emplace_back(defaultSourceType);
    }

    auto latch = std::make_shared<ThreadBarrier>(numQueries);

    auto workerConfiguration1 = WorkerConfiguration::create();
    workerConfiguration1->dataPort.setValue(*dataPort1);
    for (auto physicalSourceType : physicalSourceTypes) {
        workerConfiguration1->physicalSourceTypes.add(physicalSourceType);
    }
    workerConfiguration1->numWorkerThreads.setValue(numThreads);
    workerConfiguration1->bufferSizeInBytes.setValue(bufferSize);
    workerConfiguration1->numberOfBuffersInGlobalBufferManager.setValue(buffersManaged);
    workerConfiguration1->numberOfBuffersInSourceLocalBufferPool.setValue(64);
    workerConfiguration1->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngineSender = Runtime::NodeEngineBuilder::create(workerConfiguration1)
                                .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                                .build();
    auto netManagerSender = nodeEngineSender->getNetworkManager();
    NodeLocation nodeLocationSender = netManagerSender->getServerLocation();
    auto workerConfiguration2 = WorkerConfiguration::create();
    workerConfiguration2->dataPort.setValue(*dataPort2);
    for (auto source : physicalSourceTypes) {
        workerConfiguration2->physicalSourceTypes.add(source);
    }
    workerConfiguration2->numWorkerThreads.setValue(numThreads);
    workerConfiguration2->bufferSizeInBytes.setValue(bufferSize);
    workerConfiguration2->numberOfBuffersInGlobalBufferManager.setValue(buffersManaged);
    workerConfiguration2->numberOfBuffersInSourceLocalBufferPool.setValue(64);
    workerConfiguration2->numberOfBuffersPerWorker.setValue(12);
    auto nodeEngineReceiver = Runtime::NodeEngineBuilder::create(workerConfiguration2)
                                  .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                                  .build();

    auto netManagerReceiver = nodeEngineReceiver->getNetworkManager();
    NodeLocation nodeLocationReceiver = netManagerReceiver->getServerLocation();

    std::vector<std::shared_ptr<TestSink>> finalSinks;

    uint32_t subPlanId = 0;
    uint32_t subPlanVersion = 0;
    for (auto i = 1; i <= numQueries; ++i) {
        NesPartition nesPartition{SharedQueryId(i), OperatorId(i * 22), PartitionId(i * 33), SubpartitionId(i * 44)};
        // create NetworkSink
        auto networkSourceDescriptor1 = std::make_shared<TestUtils::TestSourceDescriptor>(
            schema,
            [&](SchemaPtr schema,
                OperatorId,
                OriginId,
                StatisticId,
                const SourceDescriptorPtr&,
                const Runtime::NodeEnginePtr&,
                size_t numSourceLocalBuffers,
                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) -> DataSourcePtr {
                auto uniqueId = OperatorId(1);
                return std::make_shared<NetworkSource>(schema,
                                                       nodeEngineReceiver->getBufferManager(),
                                                       nodeEngineReceiver->getQueryManager(),
                                                       netManagerReceiver,
                                                       nesPartition,
                                                       nodeLocationSender,
                                                       numSourceLocalBuffers,
                                                       NSOURCE_RETRY_WAIT,
                                                       NSOURCE_RETRIES,
                                                       successors,
                                                       0,
                                                       uniqueId);
            });

        auto testSink = std::make_shared<TestSink>(schema,
                                                   nodeEngineReceiver,
                                                   nodeEngineReceiver->getBufferManager(),
                                                   1,
                                                   SharedQueryId(i),
                                                   DecomposedQueryId(subPlanId),
                                                   DecomposedQueryPlanVersion(subPlanVersion));
        finalSinks.emplace_back(testSink);
        auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

        auto query = TestQuery::from(networkSourceDescriptor1).sink(testSinkDescriptor);
        auto decomposedQueryPlan = DecomposedQueryPlan::create(DecomposedQueryId(subPlanId++),
                                                               SharedQueryId(i),
                                                               INVALID_WORKER_NODE_ID,
                                                               query.getQueryPlan()->getRootOperators());
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
        queryPlan->setQueryId(QueryId(i));
        auto request = QueryCompilation::QueryCompilationRequest::create(decomposedQueryPlan, nodeEngineReceiver);
        auto queryCompiler = TestUtils::createTestQueryCompiler();
        auto result = queryCompiler->compileQuery(request);
        auto builderReceiverQEP = result->getExecutableQueryPlan();

        // creating query plan
        auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
            schema,
            [&](SchemaPtr schema,
                OperatorId,
                OriginId,
                StatisticId,
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
                                                         OperatorId(i),
                                                         SharedQueryId(i),
                                                         DecomposedQueryId(subPlanId),
                                                         DecomposedQueryPlanVersion(subPlanVersion),
                                                         nodeLocationReceiver,
                                                         nesPartition,
                                                         nodeEngineSender,
                                                         1,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES,
                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                         INITIAL_VERSION);
        auto networkSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(networkSink);
        auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(networkSinkDescriptor);
        auto decomposedQueryPlan2 = DecomposedQueryPlan::create(DecomposedQueryId(subPlanId++),
                                                                SharedQueryId(i),
                                                                INVALID_WORKER_NODE_ID,
                                                                query2.getQueryPlan()->getRootOperators());
        auto queryPlan2 = typeInferencePhase->execute(query2.getQueryPlan());
        queryPlan2->setQueryId(QueryId(i));
        auto request2 = QueryCompilation::QueryCompilationRequest::create(decomposedQueryPlan2, nodeEngineSender);
        auto result2 = queryCompiler->compileQuery(request2);
        auto builderGeneratorQEP = result2->getExecutableQueryPlan();
        //        ASSERT_TRUE(nodeEngineSender->registerQueryInNodeEngine(builderGeneratorQEP));
        //        ASSERT_TRUE(nodeEngineReceiver->registerQueryInNodeEngine(builderReceiverQEP));
        //        ASSERT_TRUE(nodeEngineSender->startQuery(builderGeneratorQEP->getQueryId()));
        //        ASSERT_TRUE(nodeEngineReceiver->startQuery(builderReceiverQEP->getQueryId()));

        auto func = [](auto engine, auto qep) {
            return engine->registerExecutableQueryPlan(qep);
        };

        auto f1 = std::async(std::launch::async, func, nodeEngineSender, builderGeneratorQEP);
        auto f2 = std::async(std::launch::async, func, nodeEngineReceiver, builderReceiverQEP);

        ASSERT_TRUE(f1.get());
        ASSERT_TRUE(f2.get());
        ASSERT_TRUE(nodeEngineSender->startDecomposedQueryPlan(builderGeneratorQEP->getSharedQueryId(),
                                                               builderGeneratorQEP->getDecomposedQueryId()));
        ASSERT_TRUE(nodeEngineReceiver->startDecomposedQueryPlan(builderReceiverQEP->getSharedQueryId(),
                                                                 builderReceiverQEP->getDecomposedQueryId()));
    }

    ASSERT_EQ(numQueries, finalSinks.size());

    for (const auto& testSink : finalSinks) {
        ASSERT_EQ(10ULL, testSink->completed.get_future().get());
    }

    NES_DEBUG("All network sinks are completed");

    while (true) {
        auto completedSubQueries = 0u;
        for (auto i = 1; i <= numQueries; ++i) {
            for (auto engine : {nodeEngineReceiver, nodeEngineSender}) {
                auto qepStatus = engine->getQueryStatus(SharedQueryId(i));
                if (qepStatus == Runtime::Execution::ExecutableQueryPlanStatus::Stopped
                    || qepStatus == Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
                    completedSubQueries++;
                }
            }
        }
        if (completedSubQueries == subPlanId) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    NES_DEBUG("All qeps are completed");
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
    auto nesPartition = NesPartition(SharedQueryId(1), OperatorId(22), PartitionId(333), SubpartitionId(444));

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
            NetworkManager::create(INVALID_WORKER_NODE_ID,
                                   "127.0.0.1",
                                   *dataPort2,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(eventReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&, bool) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, this] {
            // register the incoming channel
            sleep(3);// intended stalling to simulate latency
            auto nodeLocation = NodeLocation(INVALID_WORKER_NODE_ID, "127.0.0.1", *dataPort2);
            netManager->registerSubpartitionConsumer(nesPartition, nodeLocation, std::make_shared<DataEmitterImpl>());
            auto future = completedProm.get_future();
            if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
                ASSERT_TRUE(future.get());
            } else {
                NES_ERROR("NetworkStackIntegrationTest: Receiving thread timed out!");
            }
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        NodeLocation nodeLocation(INVALID_WORKER_NODE_ID, "127.0.0.1", *dataPort2);
        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackIntegrationTest: Error in registering DataChannel!");
            completedProm.set_value(false);
        } else {
            senderChannel->sendEvent<detail::TestEvent>(Runtime::EventType::kCustomEvent, 123);
            senderChannel->close(Runtime::QueryTerminationType::Graceful);
            senderChannel.reset();
            netManager->unregisterSubpartitionProducer(nesPartition);
        }

        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_TRUE(eventReceived.load());
}

TEST_F(NetworkStackIntegrationTest, DISABLED_testSendEventBackward) {

    NodeLocation nodeLocationSender{INVALID_WORKER_NODE_ID, "127.0.0.1", *dataPort1};
    NodeLocation nodeLocationReceiver{INVALID_WORKER_NODE_ID, "127.0.0.1", *dataPort2};
    NesPartition nesPartition{SharedQueryId(1), OperatorId(22), PartitionId(33), SubpartitionId(44)};
    SchemaPtr schema = TestSchemas::getSchemaTemplate("id_one_val_64")->updateSourceName("test");
    auto queryCompilerConfiguration = Configurations::QueryCompilerConfiguration();

    auto defaultSourceType = DefaultSourceType::create("default_logical", "default");
    auto workerConfiguration1 = WorkerConfiguration::create();
    workerConfiguration1->dataPort.setValue(*dataPort1);
    workerConfiguration1->physicalSourceTypes.add(defaultSourceType);
    workerConfiguration1->bufferSizeInBytes.setValue(bufferSize);
    workerConfiguration1->numberOfBuffersInGlobalBufferManager.setValue(buffersManaged);
    workerConfiguration1->numberOfBuffersInSourceLocalBufferPool.setValue(64);
    workerConfiguration1->numberOfBuffersPerWorker.setValue(12);

    auto nodeEngineSender = Runtime::NodeEngineBuilder::create(workerConfiguration1)
                                .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                                .build();
    auto workerConfiguration2 = WorkerConfiguration::create();
    workerConfiguration2->dataPort.setValue(*dataPort2);
    workerConfiguration2->physicalSourceTypes.add(defaultSourceType);
    workerConfiguration2->bufferSizeInBytes.setValue(bufferSize);
    workerConfiguration2->numberOfBuffersInGlobalBufferManager.setValue(buffersManaged);
    workerConfiguration2->numberOfBuffersInSourceLocalBufferPool.setValue(64);
    workerConfiguration2->numberOfBuffersPerWorker.setValue(12);

    std::promise<bool> queryCompleted;

    class TestQueryListener : public DummyQueryListener {
      public:
        explicit TestQueryListener(std::promise<bool>& queryCompleted) : queryCompleted(queryCompleted) {}

        bool notifyQueryStatusChange(SharedQueryId id,
                                     DecomposedQueryId planId,
                                     Runtime::Execution::ExecutableQueryPlanStatus status) override {
            queryCompleted.set_value(true);
            return DummyQueryListener::notifyQueryStatusChange(id, planId, status);
        }

      private:
        std::promise<bool>& queryCompleted;
    };

    auto nodeEngineReceiver = Runtime::NodeEngineBuilder::create(workerConfiguration2)
                                  .setQueryStatusListener(std::make_shared<TestQueryListener>(queryCompleted))
                                  .build();
    // create NetworkSink

    class TestNetworkSink : public NetworkSink {
      public:
        using NetworkSink::NetworkSink;

      protected:
        void onEvent(Runtime::BaseEvent& event) override {
            // NetworkSink::onEvent(event);
            bool eventReceived = event.getEventType() == Runtime::EventType::kCustomEvent
                && dynamic_cast<Runtime::CustomEventWrapper&>(event).data<detail::TestEvent>()->testValue() == 123;
            bool expected = false;
            if (sourceNotifier->compare_exchange_strong(expected, true)) {
                completed.set_value(eventReceived);
            }
        }

      public:
        std::promise<bool> completed;
        std::atomic<bool>* sourceNotifier;
    };

    auto networkSourceDescriptor1 = std::make_shared<TestUtils::TestSourceDescriptor>(
        schema,
        [&](SchemaPtr schema,
            OperatorId,
            OriginId,
            StatisticId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) -> DataSourcePtr {
            auto uniqueId = OperatorId(1);
            return std::make_shared<NetworkSource>(schema,
                                                   nodeEngineReceiver->getBufferManager(),
                                                   nodeEngineReceiver->getQueryManager(),
                                                   nodeEngineReceiver->getNetworkManager(),
                                                   nesPartition,
                                                   nodeLocationSender,
                                                   numSourceLocalBuffers,
                                                   NSOURCE_RETRY_WAIT,
                                                   NSOURCE_RETRIES,
                                                   successors,
                                                   0,
                                                   uniqueId);
        });

    class TestSourceEvent : public GeneratorSource {
      public:
        explicit TestSourceEvent(const SchemaPtr& schema,
                                 const Runtime::BufferManagerPtr& bufferManager,
                                 const Runtime::QueryManagerPtr& queryManager,
                                 OperatorId operatorId,
                                 size_t numSourceLocalBuffers,
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors)
            : GeneratorSource(schema,
                              bufferManager,
                              queryManager,
                              1000,
                              operatorId,
                              INVALID_ORIGIN_ID,
                              INVALID_STATISTIC_ID,
                              numSourceLocalBuffers,
                              GatheringMode::INTERVAL_MODE,
                              successors) {}

        std::optional<Runtime::TupleBuffer> receiveData() override {
            if (!canStop) {
                auto buffer = bufferManager->getBufferBlocking();
                auto* writer = buffer.getBuffer<int64_t>();
                writer[0] = 1;
                writer[1] = 2;
                writer[2] = 3;
                buffer.setNumberOfTuples(1);
                return buffer;
            } else {
                return {};
            }
        }

      public:
        std::atomic<bool> canStop{false};
    };

    class TestSinkEvent : public SinkMedium {
      public:
        SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

        explicit TestSinkEvent(const SchemaPtr& schema,
                               Runtime::NodeEnginePtr nodeEngine,
                               const Runtime::BufferManagerPtr& bufferManager,
                               uint32_t numOfProducers = 1,
                               SharedQueryId sharedQueryId = INVALID_SHARED_QUERY_ID,
                               DecomposedQueryId decomposedQueryId = INVALID_DECOMPOSED_QUERY_PLAN_ID,
                               DecomposedQueryPlanVersion decomposedQueryVersion = INVALID_DECOMPOSED_QUERY_PLAN_VERSION)
            : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager),
                         nodeEngine,
                         numOfProducers,
                         sharedQueryId,
                         decomposedQueryId,
                         decomposedQueryVersion) {
            // nop
        }

        bool writeData(Runtime::TupleBuffer&, Runtime::WorkerContextRef context) override {
            auto parentPlan = nodeEngine->getQueryManager()->getQueryExecutionPlan(
                DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion));
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

    auto testSink = std::make_shared<TestSinkEvent>(schema, nodeEngineReceiver, nodeEngineReceiver->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(networkSourceDescriptor1).sink(testSinkDescriptor);
    auto decomposedQueryPlan = DecomposedQueryPlan::create(defaultDecomposedQueryPlanId,
                                                           defaultSharedQueryId,
                                                           INVALID_WORKER_NODE_ID,
                                                           query.getQueryPlan()->getRootOperators());
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    queryPlan->setQueryId(QueryId(0));
    auto request = QueryCompilation::QueryCompilationRequest::create(decomposedQueryPlan, nodeEngineReceiver);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto builderReceiverQEP = result->getExecutableQueryPlan();

    auto networkSink = std::make_shared<TestNetworkSink>(schema,
                                                         INVALID_OPERATOR_ID,
                                                         INVALID_SHARED_QUERY_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                         INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                         nodeLocationReceiver,
                                                         nesPartition,
                                                         nodeEngineSender,
                                                         1,
                                                         NSOURCE_RETRY_WAIT,
                                                         NSOURCE_RETRIES,
                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                         INITIAL_VERSION);

    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        schema,
        [&](SchemaPtr schema,
            OperatorId,
            OriginId,
            StatisticId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            auto source = std::make_shared<TestSourceEvent>(schema,
                                                            nodeEngineSender->getBufferManager(),
                                                            nodeEngineSender->getQueryManager(),
                                                            OperatorId(1),
                                                            numSourceLocalBuffers,
                                                            std::move(successors));
            networkSink->sourceNotifier = &source->canStop;
            return source;
        });

    auto networkSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(networkSink);
    auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(networkSinkDescriptor);
    auto decomposedQueryPlan2 = DecomposedQueryPlan::create(DecomposedQueryId(1),
                                                            SharedQueryId(0),
                                                            INVALID_WORKER_NODE_ID,
                                                            query2.getQueryPlan()->getRootOperators());
    auto queryPlan2 = typeInferencePhase->execute(query2.getQueryPlan());
    queryPlan2->setQueryId(QueryId(0));
    auto request2 = QueryCompilation::QueryCompilationRequest::create(decomposedQueryPlan2, nodeEngineSender);
    queryCompiler = TestUtils::createTestQueryCompiler();
    auto result2 = queryCompiler->compileQuery(request2);
    auto builderGeneratorQEP = result2->getExecutableQueryPlan();

    auto func = [](auto engine, auto qep) {
        return engine->registerExecutableQueryPlan(qep);
    };

    auto f1 = std::async(std::launch::async, func, nodeEngineSender, builderGeneratorQEP);
    auto f2 = std::async(std::launch::async, func, nodeEngineReceiver, builderReceiverQEP);

    ASSERT_TRUE(f1.get());
    ASSERT_TRUE(f2.get());

    auto future = networkSink->completed.get_future();

    ASSERT_TRUE(nodeEngineSender->startDecomposedQueryPlan(builderGeneratorQEP->getSharedQueryId(),
                                                           builderGeneratorQEP->getDecomposedQueryId()));
    ASSERT_TRUE(nodeEngineReceiver->startDecomposedQueryPlan(builderReceiverQEP->getSharedQueryId(),
                                                             builderReceiverQEP->getDecomposedQueryId()));

    ASSERT_TRUE(future.get());

    ASSERT_TRUE(queryCompleted.get_future().get());

    ASSERT_TRUE(nodeEngineSender->stop());
    ASSERT_TRUE(nodeEngineReceiver->stop());
}

INSTANTIATE_TEST_CASE_P(NetworkStackIntegrationTestParam, NetworkStackIntegrationTest, ::testing::Values(false, true));

}// namespace Network
}// namespace NES
