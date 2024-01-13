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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
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
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestUtils.hpp>
#include <Util/ThreadBarrier.hpp>
#include <gtest/gtest.h>
#include <random>
#include <utility>
using namespace std;

namespace NES {
using Runtime::TupleBuffer;

const uint64_t buffersManaged = 8 * 1024;
const uint64_t bufferSize = 32 * 1024;
static constexpr auto NSOURCE_RETRIES = 100;
static constexpr auto NSOURCE_RETRY_WAIT = std::chrono::milliseconds(5);
struct TestStruct {
    int64_t id;
    int64_t one;
    int64_t value;
};

namespace Network {
class NetworkStackTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NetworkStackTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("SetUpTestCase NetworkStackTest");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        freeDataPort = getAvailablePort();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        freeDataPort.reset();
        Testing::BaseIntegrationTest::TearDown();
    }

  protected:
    Testing::BorrowedPortPtr freeDataPort;
};

class TestSink : public SinkMedium {
  public:
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    TestSink(const SchemaPtr& schema,
             Runtime::NodeEnginePtr nodeEngine,
             const Runtime::BufferManagerPtr& bufferManager,
             uint32_t numOfProducers = 1,
             QueryId queryId = 0,
             QuerySubPlanId querySubPlanId = 0)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), nodeEngine, numOfProducers, queryId, querySubPlanId){};

    bool writeData(Runtime::TupleBuffer& input_buffer, Runtime::WorkerContextRef) override {
        std::unique_lock lock(m);
        auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(getSchemaPtr(), input_buffer.getBufferSize());
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, input_buffer);
        std::stringstream dynamicTupleBufferAsString;
        dynamicTupleBufferAsString << dynamicTupleBuffer;
        NES_DEBUG("TestSink:\n{}", dynamicTupleBufferAsString.str());

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

class DummyExchangeProtocolListener : public ExchangeProtocolListener {
  public:
    ~DummyExchangeProtocolListener() override = default;
    void onDataBuffer(NesPartition, TupleBuffer&) override {}
    void onEndOfStream(Messages::EndOfStreamMessage) override {}
    void onServerError(Messages::ErrorMessage) override {}
    void onEvent(NesPartition, Runtime::BaseEvent&) override {}
    void onChannelError(Messages::ErrorMessage) override {}
};

TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto exchangeProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        ZmqServer server("127.0.0.1", *freeDataPort, 4, exchangeProtocol, buffMgr);
        server.start();
        ASSERT_EQ(server.isServerRunning(), true);
    } catch (...) {
        // shutdown failed
        FAIL();
    }
}

TEST_F(NetworkStackTest, serverMustStartAndStopRandomPort) {
    try {
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto exchangeProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        ZmqServer server("127.0.0.1", *freeDataPort, 4, exchangeProtocol, buffMgr);
        server.start();
        ASSERT_EQ(server.isServerRunning(), true);
        ASSERT_EQ(server.getServerPort(), *freeDataPort);
    } catch (...) {
        // shutdown failed
        FAIL();
    }
}

TEST_F(NetworkStackTest, dispatcherMustStartAndStop) {
    try {
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto exchangeProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        auto netManager = NetworkManager::create(0, "127.0.0.1", *freeDataPort, std::move(exchangeProtocol), buffMgr);
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, startCloseChannel) {
    try {
        // start zmqServer
        std::promise<bool> completed;

        class InternalListener : public Network::ExchangeProtocolListener {
          public:
            explicit InternalListener(std::promise<bool>& p) : completed(p) {}

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override { completed.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}

          private:
            std::promise<bool>& completed;
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto ep = ExchangeProtocol(partMgr, std::make_shared<InternalListener>(completed));
        auto netManager = NetworkManager::create(0, "127.0.0.1", *freeDataPort, std::move(ep), buffMgr);

        auto nesPartition = NesPartition(0, 0, 0, 0);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };

        std::thread t([&netManager, &completed, &nesPartition] {
            // register the incoming channel
            auto cnt = netManager->registerSubpartitionConsumer(nesPartition,
                                                                netManager->getServerLocation(),
                                                                std::make_shared<DataEmitterImpl>());
            NES_INFO("NetworkStackTest: SubpartitionConsumer registered with cnt {}", cnt);
            auto v = completed.get_future().get();
            netManager->unregisterSubpartitionConsumer(nesPartition);
            ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 3);
        senderChannel->close(Runtime::QueryTerminationType::Graceful);
        senderChannel.reset();
        netManager->unregisterSubpartitionProducer(nesPartition);

        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, startCloseChannelAsyncIndefiniteRetries) {
    try {
        // start zmqServer
        std::promise<bool> completed;

        class InternalListener : public Network::ExchangeProtocolListener {
          public:
            explicit InternalListener(std::promise<bool>& p) : completed(p) {}

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override { completed.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}

          private:
            std::promise<bool>& completed;
        };

        class DummyQueryManager : public Runtime::AbstractQueryManager {
          public:
            //explicit DummyQueryManager() : Runtime::AbstractQueryManager({}, {}, 1, 1, std::make_shared<Runtime::HardwareManager>(), {}, 1, {}) {};
            explicit DummyQueryManager()
                : Runtime::AbstractQueryManager({}, {}, 1, 1, std::make_shared<Runtime::HardwareManager>(), 1){};
            ExecutionResult processNextTask(bool, Runtime::WorkerContext&) override { return ExecutionResult::Error; };
            void addWorkForNextPipeline(TupleBuffer&, Runtime::Execution::SuccessorExecutablePipeline, uint32_t) override{};
            void poisonWorkers() override{};
            bool addReconfigurationMessage(QueryId, QuerySubPlanId, const Runtime::ReconfigurationMessage&, bool) override {
                receivedCallback = true;
                return true;
            };
            bool addReconfigurationMessage(QueryId, QuerySubPlanId, TupleBuffer&&, bool) override { return false; };

            uint64_t getNumberOfTasksInWorkerQueues() const override { return 0; };
            bool startThreadPool(uint64_t) override { return false; };
            ExecutionResult terminateLoop(Runtime::WorkerContext&) override { return ExecutionResult::Error; };
            bool receivedCallback = false;
        };

        auto partMgrRecv = std::make_shared<PartitionManager>();
        auto buffMgrRecv = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto receiveListener = std::make_shared<InternalListener>(completed);
        auto ep = ExchangeProtocol(partMgrRecv, receiveListener);
        auto netManagerReceiver = NetworkManager::create(0, "127.0.0.1", *freeDataPort, std::move(ep), buffMgrRecv, 4, 8);

        auto partMgrSend = std::make_shared<PartitionManager>();
        auto buffMgrSend = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto dummyProtocol = ExchangeProtocol(partMgrSend, std::make_shared<DummyExchangeProtocolListener>());
        auto senderPort = getAvailablePort();
        auto netManagerSender = NetworkManager::create(0, "127.0.0.1", *senderPort, std::move(dummyProtocol), buffMgrSend, 4, 2);

        auto nesPartition = NesPartition(0, 0, 0, 0);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };

        std::thread t([&netManagerReceiver, &completed, &nesPartition] {
            // register the incoming channel
            auto cnt = netManagerReceiver->registerSubpartitionConsumer(nesPartition,
                                                                        netManagerReceiver->getServerLocation(),
                                                                        std::make_shared<DataEmitterImpl>());
            NES_INFO("NetworkStackTest: SubpartitionConsumer registered with cnt {}", cnt);
            auto v = completed.get_future().get();
            netManagerReceiver->unregisterSubpartitionConsumer(nesPartition);
            ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto queryManager = std::make_shared<DummyQueryManager>();
        auto reconf = Runtime::ReconfigurationMessage(1,
                                                      1,
                                                      Runtime::ReconfigurationType::ConnectionEstablished,
                                                      {},
                                                      std::make_any<uint32_t>(1));
        //setting retry times to zero will let the channel keep attempting to connect indefinitely
        auto senderChannelFuture = netManagerSender->registerSubpartitionProducerAsync(nodeLocation,
                                                                                       nesPartition,
                                                                                       buffMgrSend,
                                                                                       std::chrono::seconds(1),
                                                                                       0,
                                                                                       reconf,
                                                                                       queryManager);
        EXPECT_FALSE(queryManager->receivedCallback);
        auto start_timestamp = std::chrono::system_clock::now();
        auto timeOut = std::chrono::seconds(10);
        while (senderChannelFuture.first.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            if (std::chrono::system_clock::now() > start_timestamp + timeOut) {
                NES_DEBUG("Channel not created within timeout")
                FAIL();
            }
        }
        auto senderChannel = senderChannelFuture.first.get();

        start_timestamp = std::chrono::system_clock::now();
        while (!queryManager->receivedCallback) {
            if (std::chrono::system_clock::now() > start_timestamp + timeOut) {
                NES_DEBUG("Callback not recevied within timeout")
                FAIL();
            }
        }
        ASSERT_TRUE(queryManager->receivedCallback);

        senderChannel->close(Runtime::QueryTerminationType::Graceful);
        senderChannel.reset();
        netManagerSender->unregisterSubpartitionProducer(nesPartition);

        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, startCloseMaxChannel) {
    try {
        // start zmqServer
        std::promise<bool> completed;
        std::promise<bool> connectionsReady;

        class InternalListener : public Network::ExchangeProtocolListener {
          public:
            explicit InternalListener(std::promise<bool>& p, uint32_t maxExpectedConnections)
                : completed(p), numReceivedEoS(0), maxExpectedConnections(maxExpectedConnections) {}

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override {
                if (numReceivedEoS.fetch_add(1) == (maxExpectedConnections - 1)) {
                    completed.set_value(true);
                }
            }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}

          private:
            std::promise<bool>& completed;
            std::atomic<uint32_t> numReceivedEoS;
            const uint32_t maxExpectedConnections;
        };

        constexpr auto maxExpectedConnections = 1000;
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto ep = ExchangeProtocol(partMgr, std::make_shared<InternalListener>(completed, maxExpectedConnections));
        auto netManagerReceiver = NetworkManager::create(0, "127.0.0.1", *freeDataPort, std::move(ep), buffMgr, 4, 8);

        auto dummyProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        auto senderPort = getAvailablePort();
        auto netManagerSender = NetworkManager::create(0, "127.0.0.1", *senderPort, std::move(dummyProtocol), buffMgr, 4, 2);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };

        std::thread t([&netManagerReceiver, &netManagerSender, &completed, &connectionsReady] {
            // register the incoming channel

            for (auto i = 0; i < maxExpectedConnections; ++i) {
                auto nesPartition = NesPartition(i, 0, 0, 0);
                ASSERT_TRUE(netManagerReceiver->registerSubpartitionConsumer(nesPartition,
                                                                             netManagerSender->getServerLocation(),
                                                                             std::make_shared<DataEmitterImpl>()));
            }

            auto v1 = completed.get_future().get();
            auto v2 = connectionsReady.get_future().get();
            ASSERT_TRUE(v1 && v2);
            for (auto i = 0; i < maxExpectedConnections; ++i) {
                auto nesPartition = NesPartition(i, 0, 0, 0);
                (netManagerReceiver->unregisterSubpartitionConsumer(nesPartition));
            }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        std::vector<NetworkChannelPtr> channels;

        for (auto i = 0; i < maxExpectedConnections; ++i) {
            auto nesPartition = NesPartition(i, 0, 0, 0);
            channels.emplace_back(
                netManagerSender->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 3));
            if (!channels.back()) {
                break;
            }
        }
        ASSERT_EQ(maxExpectedConnections, channels.size());

        int i = 0;
        for (auto&& senderChannel : channels) {
            auto nesPartition = NesPartition(i++, 0, 0, 0);
            senderChannel->close(Runtime::QueryTerminationType::Graceful);
            netManagerSender->unregisterSubpartitionProducer(nesPartition);
        }
        connectionsReady.set_value(true);
        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, testSendData) {
    std::promise<bool> completedProm;

    std::atomic<bool> bufferReceived = false;
    bool completed = false;
    auto nesPartition = NesPartition(1, 22, 333, 444);

    try {

        class ExchangeListener : public ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<bool>& bufferReceived;

            ExchangeListener(std::atomic<bool>& bufferReceived, std::promise<bool>& completedProm)
                : completedProm(completedProm), bufferReceived(bufferReceived) {}

            void onDataBuffer(NesPartition id, TupleBuffer& buf) override {

                if (buf.getBufferSize() == bufferSize) {
                    (volatile void) id.getQueryId();
                    (volatile void) id.getOperatorId();
                    (volatile void) id.getPartitionId();
                    (volatile void) id.getSubpartitionId();
                    bufferReceived = true;
                } else {
                    bufferReceived = false;
                }
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   *freeDataPort,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, &completed] {
            // register the incoming channel
            sleep(3);// intended stalling to simulate latency
            netManager->registerSubpartitionConsumer(nesPartition,
                                                     netManager->getServerLocation(),
                                                     std::make_shared<DataEmitterImpl>());
            auto future = completedProm.get_future();
            if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
                completed = future.get();
            } else {
                NES_ERROR("NetworkStackTest: Receiving thread timed out!");
            }
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering DataChannel!");
            completedProm.set_value(false);
        } else {
            // create testbuffer
            constexpr auto sentBufferSequenceNumber = 1;
            auto buffer = buffMgr->getBufferBlocking();
            buffer.getBuffer<uint64_t>()[0] = 0;
            buffer.setNumberOfTuples(1);
            buffer.setSequenceNumber(1);
            senderChannel->sendBuffer(buffer, sizeof(uint64_t), sentBufferSequenceNumber);
            senderChannel->close(Runtime::QueryTerminationType::Graceful, 1, 1);
            senderChannel.reset();
            netManager->unregisterSubpartitionProducer(nesPartition);
        }

        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(bufferReceived, true);
    ASSERT_EQ(completed, true);
}

TEST_F(NetworkStackTest, testCorrectHandlingEOS) {
    std::promise<bool> completedProm;

    // With these parameters, we should have around 5 handling threads and 4 buffers.
    // Therefore, we guarantee that an EOS is being processed together with a data buffer
    constexpr uint64_t numNetworkReceivingThreads = 10;
    uint64_t totalNumBuffer = 4;

    std::atomic<std::uint64_t> bufferReceived = 0;
    auto nesPartition = NesPartition(1, 22, 333, 444);
    try {
        class ExchangeListener : public ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<std::uint64_t>& bufferReceived;

            ExchangeListener(std::atomic<std::uint64_t>& bufferReceived, std::promise<bool>& completedProm)
                : completedProm(completedProm), bufferReceived(bufferReceived) {}

            void onDataBuffer(NesPartition id, TupleBuffer& buffer) override {

                ASSERT_TRUE(buffer.getBufferSize() == bufferSize);
                (volatile void) id.getQueryId();
                (volatile void) id.getOperatorId();
                (volatile void) id.getPartitionId();
                (volatile void) id.getSubpartitionId();
                auto* bufferContent = buffer.getBuffer<uint64_t>();
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    ASSERT_EQ(bufferContent[j], j);
                }
                bufferReceived++;
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   *freeDataPort,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
                                   buffMgr,
                                   -1 /*senderHighWatermark*/,
                                   numNetworkReceivingThreads);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, totalNumBuffer, nodeLocation] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition, nodeLocation, std::make_shared<DataEmitterImpl>());
            auto startTime = std::chrono::steady_clock::now().time_since_epoch();
            EXPECT_TRUE(completedProm.get_future().get());
            auto stopTime = std::chrono::steady_clock::now().time_since_epoch();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime);
            double bytes = totalNumBuffer * bufferSize;
            double throughput = (bytes * 1'000'000'000) / (elapsed.count() * 1024.0 * 1024.0);
            NES_DEBUG("Sent {} bytes :: throughput {}", bytes, throughput);
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_DEBUG("NetworkStackTest: Error in registering DataChannel!");
            completedProm.set_value(false);
        } else {
            for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                auto buffer = buffMgr->getBufferBlocking();
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    buffer.getBuffer<uint64_t>()[j] = j;
                }
                buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                buffer.setSequenceNumber(i + 1);
                senderChannel->sendBuffer(buffer, sizeof(uint64_t), i + 1);
            }
            senderChannel->close(Runtime::QueryTerminationType::Graceful, 1, totalNumBuffer);
            senderChannel.reset();
            netManager->unregisterSubpartitionProducer(nesPartition);
        }
        netManager->unregisterSubpartitionProducer(nesPartition);
        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(bufferReceived, totalNumBuffer);
}

TEST_F(NetworkStackTest, testMassiveSending) {
    std::promise<bool> completedProm;

    uint64_t totalNumBuffer = 1'000;
    std::atomic<std::uint64_t> bufferReceived = 0;
    auto nesPartition = NesPartition(1, 22, 333, 444);
    try {
        class ExchangeListener : public ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<std::uint64_t>& bufferReceived;

            ExchangeListener(std::atomic<std::uint64_t>& bufferReceived, std::promise<bool>& completedProm)
                : completedProm(completedProm), bufferReceived(bufferReceived) {}

            void onDataBuffer(NesPartition id, TupleBuffer& buffer) override {

                ASSERT_TRUE(buffer.getBufferSize() == bufferSize);
                (volatile void) id.getQueryId();
                (volatile void) id.getOperatorId();
                (volatile void) id.getPartitionId();
                (volatile void) id.getSubpartitionId();
                auto* bufferContent = buffer.getBuffer<uint64_t>();
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    ASSERT_EQ(bufferContent[j], j);
                }
                bufferReceived++;
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   *freeDataPort,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, totalNumBuffer, nodeLocation] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition, nodeLocation, std::make_shared<DataEmitterImpl>());
            auto startTime = std::chrono::steady_clock::now().time_since_epoch();
            EXPECT_TRUE(completedProm.get_future().get());
            auto stopTime = std::chrono::steady_clock::now().time_since_epoch();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime);
            double bytes = totalNumBuffer * bufferSize;
            double throughput = (bytes * 1'000'000'000) / (elapsed.count() * 1024.0 * 1024.0);
            NES_DEBUG("Sent {} bytes :: throughput {}", bytes, throughput);
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_DEBUG("NetworkStackTest: Error in registering DataChannel!");
            completedProm.set_value(false);
        } else {
            for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                auto buffer = buffMgr->getBufferBlocking();
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    buffer.getBuffer<uint64_t>()[j] = j;
                }
                buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                buffer.setSequenceNumber(i + 1);
                senderChannel->sendBuffer(buffer, sizeof(uint64_t), i + 1);
            }
            senderChannel->close(Runtime::QueryTerminationType::Graceful, 1, totalNumBuffer);
            senderChannel.reset();
            netManager->unregisterSubpartitionProducer(nesPartition);
        }
        netManager->unregisterSubpartitionProducer(nesPartition);
        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(bufferReceived, totalNumBuffer);
}

TEST_F(NetworkStackTest, testMassiveSendingWithChildrenBuffer) {
    std::promise<bool> completedProm;
    static const char* ctrl = "nebula";
    struct Record {
        uint64_t val;
        uint32_t logical;
    };
    uint64_t totalNumBuffer = 10;
    std::atomic<std::uint64_t> bufferReceived = 0;
    auto nesPartition = NesPartition(1, 22, 333, 444);
    try {
        class ExchangeListener : public ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<std::uint64_t>& bufferReceived;

            ExchangeListener(std::atomic<std::uint64_t>& bufferReceived, std::promise<bool>& completedProm)
                : completedProm(completedProm), bufferReceived(bufferReceived) {}

            void onDataBuffer(NesPartition id, TupleBuffer& buffer) override {

                ASSERT_TRUE(buffer.getBufferSize() == bufferSize);
                (volatile void) id.getQueryId();
                (volatile void) id.getOperatorId();
                (volatile void) id.getPartitionId();
                (volatile void) id.getSubpartitionId();
                auto* bufferContent = buffer.getBuffer<Record>();
                for (uint64_t j = 0; j < buffer.getNumberOfTuples(); ++j) {
                    ASSERT_EQ(bufferContent[j].val, j);
                    auto idx = bufferContent[j].logical;
                    ASSERT_NE(idx, uint32_t(-1));
                    auto child = buffer.loadChildBuffer(idx);
                    ASSERT_EQ(0, strcmp(ctrl, child.getBuffer<char>()));
                }

                bufferReceived++;
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   *freeDataPort,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, totalNumBuffer, nodeLocation] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition, nodeLocation, std::make_shared<DataEmitterImpl>());
            auto startTime = std::chrono::steady_clock::now().time_since_epoch();
            EXPECT_TRUE(completedProm.get_future().get());
            auto stopTime = std::chrono::steady_clock::now().time_since_epoch();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime);
            double bytes = totalNumBuffer * bufferSize;
            double throughput = (bytes * 1'000'000'000) / (elapsed.count() * 1024.0 * 1024.0);
            NES_DEBUG("Sent {} bytes :: throughput {}\n", bytes, throughput);
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_DEBUG("NetworkStackTest: Error in registering DataChannel!");
            completedProm.set_value(false);
        } else {
            for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                auto buffer = buffMgr->getBufferBlocking();
                auto content = buffer.getBuffer<Record>();
                for (uint64_t j = 0; j < bufferSize / sizeof(Record); ++j) {
                    content[j].val = j;
                    auto optBuf = buffMgr->getUnpooledBuffer(7);
                    ASSERT_TRUE(!!optBuf);
                    memcpy((*optBuf).getBuffer<char>(), "nebula", 7);
                    optBuf->setNumberOfTuples(7);
                    auto idx = buffer.storeChildBuffer(*optBuf);
                    content[j].logical = idx;
                }
                buffer.setNumberOfTuples(bufferSize / sizeof(Record));
                buffer.setSequenceNumber(i + 1);
                ASSERT_EQ(buffer.getNumberOfChildrenBuffer(), bufferSize / sizeof(Record));
                senderChannel->sendBuffer(buffer, sizeof(Record), i + 1);
            }
            senderChannel->close(Runtime::QueryTerminationType::Graceful, 1, totalNumBuffer);
            senderChannel.reset();
            netManager->unregisterSubpartitionProducer(nesPartition);
        }
        netManager->unregisterSubpartitionProducer(nesPartition);
        t.join();
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(bufferReceived, totalNumBuffer);
}

TEST_F(NetworkStackTest, testHandleUnregisteredBuffer) {
    static constexpr int retryTimes = 3;
    try {
        // start zmqServer
        std::promise<bool> serverError;
        std::promise<bool> channelError;

        class ExchangeListener : public ExchangeProtocolListener {
            std::atomic<int> errorCallsServer = 0;
            std::atomic<int> errorCallsChannel = 0;

          public:
            std::promise<bool>& serverError;
            std::promise<bool>& channelError;

            ExchangeListener(std::promise<bool>& serverError, std::promise<bool>& channelError)
                : serverError(serverError), channelError(channelError) {}

            void onServerError(Messages::ErrorMessage errorMsg) override {
                errorCallsServer++;
                if (errorCallsServer == retryTimes) {
                    serverError.set_value(true);
                }
                NES_INFO("NetworkStackTest: Server error called!");
                ASSERT_EQ(errorMsg.getErrorType(), Messages::ErrorType::PartitionNotRegisteredError);
            }

            void onChannelError(Messages::ErrorMessage errorMsg) override {
                errorCallsChannel++;
                if (errorCallsChannel == retryTimes) {
                    channelError.set_value(true);
                }
                NES_INFO("NetworkStackTest: Channel error called!");
                ASSERT_EQ(errorMsg.getErrorType(), Messages::ErrorType::PartitionNotRegisteredError);
            }

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override {}
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   *freeDataPort,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(serverError, channelError)),
                                   buffMgr);

        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);
        auto channel = netManager->registerSubpartitionProducer(nodeLocation,
                                                                NesPartition(1, 22, 333, 4445),
                                                                buffMgr,
                                                                std::chrono::seconds(1),
                                                                retryTimes);

        ASSERT_EQ(channel, nullptr);
        ASSERT_EQ(serverError.get_future().get(), true);
        ASSERT_EQ(channelError.get_future().get(), true);
        netManager->unregisterSubpartitionProducer(NesPartition(1, 22, 333, 4445));
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, testMassiveMultiSending) {
    uint64_t totalNumBuffer = 1'000;
    constexpr uint64_t numSendingThreads = 16;

    // create a couple of NesPartitions
    auto sendingThreads = std::vector<std::thread>();
    auto nesPartitions = std::vector<NesPartition>();
    auto completedPromises = std::vector<std::promise<bool>>();
    std::array<std::atomic<std::uint64_t>, numSendingThreads> bufferCounter{};

    for (uint64_t i = 0ull; i < numSendingThreads; ++i) {
        nesPartitions.emplace_back(i, i + 10, i + 20, i + 30);
        completedPromises.emplace_back();
        bufferCounter[i].store(0);
    }

    try {
        class ExchangeListenerImpl : public ExchangeProtocolListener {
          private:
            std::array<std::atomic<std::uint64_t>, numSendingThreads>& bufferCounter;
            std::vector<std::promise<bool>>& completedPromises;

          public:
            ExchangeListenerImpl(std::array<std::atomic<std::uint64_t>, numSendingThreads>& bufferCounter,
                                 std::vector<std::promise<bool>>& completedPromises)
                : bufferCounter(bufferCounter), completedPromises(completedPromises) {}

            void onServerError(Messages::ErrorMessage) override {}

            void onChannelError(Messages::ErrorMessage) override {}

            void onDataBuffer(NesPartition id, TupleBuffer& buffer) override {
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    ASSERT_EQ(buffer.getBuffer<uint64_t>()[j], j);
                }
                bufferCounter[id.getQueryId()]++;
                usleep(1000);
            }
            void onEndOfStream(Messages::EndOfStreamMessage p) override {
                completedPromises[p.getChannelId().getNesPartition().getQueryId()].set_value(true);
            }
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            0,
            "127.0.0.1",
            *freeDataPort,
            ExchangeProtocol(partMgr, std::make_shared<ExchangeListenerImpl>(bufferCounter, completedPromises)),
            buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread receivingThread([&netManager, &nesPartitions, &completedPromises, this] {
            // register the incoming channel
            NodeLocation nodeLocation{0, "127.0.0.1", *freeDataPort};
            for (NesPartition p : nesPartitions) {
                //add random latency
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 300));
                netManager->registerSubpartitionConsumer(p, nodeLocation, std::make_shared<DataEmitterImpl>());
            }

            for (std::promise<bool>& p : completedPromises) {
                EXPECT_TRUE(p.get_future().get());
            }
            for (NesPartition p : nesPartitions) {
                netManager->unregisterSubpartitionConsumer(p);
            }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", *freeDataPort);

        for (auto i = 0ULL; i < numSendingThreads; ++i) {
            std::thread sendingThread([&, i] {
                // register the incoming channel
                NesPartition nesPartition(i, i + 10, i + 20, i + 30);
                auto senderChannel =
                    netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(2), 10);

                if (senderChannel == nullptr) {
                    NES_INFO("NetworkStackTest: Error in registering DataChannel!");
                    completedPromises[i].set_value(false);
                } else {
                    std::mt19937 rnd;
                    std::uniform_int_distribution gen(5'000, 75'000);
                    for (auto sentBuffers = 0ULL; sentBuffers < totalNumBuffer; ++sentBuffers) {
                        auto buffer = buffMgr->getBufferBlocking();
                        for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                            buffer.getBuffer<uint64_t>()[j] = j;
                        }
                        buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                        buffer.setSequenceNumber(sentBuffers + 1);
                        senderChannel->sendBuffer(buffer, sizeof(uint64_t), sentBuffers + 1);
                        usleep(gen(rnd));
                    }
                    senderChannel->close(Runtime::QueryTerminationType::Graceful, 0, totalNumBuffer);
                    senderChannel.reset();
                    netManager->unregisterSubpartitionProducer(nesPartition);
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
    } catch (...) {
        FAIL();
    }
    for (uint64_t cnt : bufferCounter) {
        ASSERT_EQ(cnt, totalNumBuffer);
    }
}

}// namespace Network
}// namespace NES