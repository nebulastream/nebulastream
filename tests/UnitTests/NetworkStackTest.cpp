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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/UtilityFunctions.hpp>

#include "../util/DummySink.hpp"
#include "../util/TestQuery.hpp"
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <gtest/gtest.h>
#include <random>

using namespace std;

namespace NES {
using NodeEngine::TupleBuffer;
using NodeEngine::MemoryLayoutPtr;

const uint64_t buffersManaged = 8 * 1024;
const uint64_t bufferSize = 4 * 1024;

struct TestStruct {
    int64_t id;
    int64_t one;
    int64_t value;
};

namespace Network {
class NetworkStackTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("NetworkStackTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup NetworkStackTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down NetworkStackTest class." << std::endl; }

    /* Will be called before a  test is executed. */
    void SetUp() override { NES_INFO("Setup NetworkStackTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down NetworkStackTest test case." << std::endl; }
};

class TestSink : public SinkMedium {
  public:
    SinkMediumTypes getSinkMediumType() { return SinkMediumTypes::PRINT_SINK; }

    TestSink(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0){};

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink:\n" << UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchemaPtr()));

        uint64_t sum = 0;
        for (uint64_t i = 0; i < input_buffer.getNumberOfTuples(); ++i) {
            sum += input_buffer.getBuffer<TestStruct>()[i].value;
        }

        completed.set_value(sum);
        return true;
    }

    const std::string toString() const override { return ""; }

    void setup() override{};

    void shutdown() override{};

    ~TestSink() override{};

    std::string toString() override { return "PRINT_SINK"; }

  public:
    std::mutex m;
    std::promise<uint64_t> completed;
};

void fillBuffer(TupleBuffer& buf, MemoryLayoutPtr memoryLayout) {
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->write(buf, recordIndex);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 1)->write(buf, 1);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 2)->write(buf, recordIndex % 2);
    }
    buf.setNumberOfTuples(10);
}

class DummyExchangeProtocolListener : public ExchangeProtocolListener {
  public:
    void onDataBuffer(NesPartition, TupleBuffer&) override {}
    void onEndOfStream(Messages::EndOfStreamMessage) override {}
    void onServerError(Messages::ErrorMessage) override {}

    void onChannelError(Messages::ErrorMessage) override {}
};

TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);
        auto exchangeProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        ZmqServer server("127.0.0.1", 31337, 4, exchangeProtocol, buffMgr);
        server.start();
        ASSERT_EQ(server.getIsRunning(), true);
    } catch (...) {
        // shutdown failed
        FAIL();
    }
}

TEST_F(NetworkStackTest, dispatcherMustStartAndStop) {
    try {
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);
        auto exchangeProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        auto netManager = NetworkManager::create("127.0.0.1", 31337, std::move(exchangeProtocol), buffMgr);
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, startCloseChannel) {
    try {
        // start zmqServer
        std::promise<bool> completed;

        class InternalListener : public Network::ExchangeProtocolListener {
          public:
            InternalListener(std::promise<bool>& p) : completed(p) {}

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override { completed.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}

            void onChannelError(Messages::ErrorMessage) override {}

          private:
            std::promise<bool>& completed;
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);
        auto ep = ExchangeProtocol(partMgr, std::make_shared<InternalListener>(completed));
        auto netManager = NetworkManager::create("127.0.0.1", 31337, std::move(ep), buffMgr);

        auto nesPartition = NesPartition(0, 0, 0, 0);

        std::thread t([&netManager, &completed, &nesPartition] {
            // register the incoming channel
            auto cnt = netManager->registerSubpartitionConsumer(nesPartition);
            NES_INFO("NetworkStackTest: SubpartitionConsumer registered with cnt" << cnt);
            auto v = completed.get_future().get();
            ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager->registerSubpartitionProducer(nodeLocation, nesPartition, std::chrono::seconds(1), 3);
        senderChannel->close();
        senderChannel.reset();

        t.join();
    } catch (...) {
        ASSERT_EQ(true, false);
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
                : bufferReceived(bufferReceived), completedProm(completedProm) {}

            void onDataBuffer(NesPartition id, TupleBuffer& buf) override {
                bufferReceived = (buf.getBufferSize() == bufferSize) && (id.getQueryId(), 1) && (id.getOperatorId(), 22)
                    && (id.getPartitionId(), 333) && (id.getSubpartitionId(), 444);
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}

            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1", 31337, ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
            buffMgr);

        std::thread t([&netManager, &nesPartition, &completedProm, &completed] {
            // register the incoming channel
            sleep(3);// intended stalling to simulate latency
            netManager->registerSubpartitionConsumer(nesPartition);
            auto future = completedProm.get_future();
            if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
                completed = future.get();
            } else {
                NES_ERROR("NetworkStackTest: Receiving thread timed out!");
            }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager->registerSubpartitionProducer(nodeLocation, nesPartition, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            // create testbuffer
            auto buffer = buffMgr->getBufferBlocking();
            buffer.getBuffer<uint64_t>()[0] = 0;
            buffer.setNumberOfTuples(1);
            senderChannel->sendBuffer(buffer, sizeof(uint64_t));
            senderChannel.reset();
        }

        t.join();
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(bufferReceived, true);
    ASSERT_EQ(completed, true);
}

TEST_F(NetworkStackTest, testMassiveSending) {
    std::promise<bool> completedProm;

    uint64_t totalNumBuffer = 1'000'000;
    std::atomic<std::uint64_t> bufferReceived = 0;
    auto nesPartition = NesPartition(1, 22, 333, 444);

    try {
        class ExchangeListener : public ExchangeProtocolListener {

          public:
            std::promise<bool>& completedProm;
            std::atomic<std::uint64_t>& bufferReceived;

            ExchangeListener(std::atomic<std::uint64_t>& bufferReceived, std::promise<bool>& completedProm)
                : bufferReceived(bufferReceived), completedProm(completedProm) {}

            void onDataBuffer(NesPartition id, TupleBuffer& buffer) override {
                ASSERT_EQ((buffer.getBufferSize() == bufferSize) && (id.getQueryId(), 1) && (id.getOperatorId(), 22)
                              && (id.getPartitionId(), 333) && (id.getSubpartitionId(), 444),
                          true);
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    ASSERT_EQ(buffer.getBuffer<uint64_t>()[j], j);
                }
                bufferReceived++;
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override { completedProm.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}

            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1", 31337, ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
            buffMgr);

        std::thread t([&netManager, &nesPartition, &completedProm] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition);
            ASSERT_TRUE(completedProm.get_future().get());
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager->registerSubpartitionProducer(nodeLocation, nesPartition, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                auto buffer = buffMgr->getBufferBlocking();
                for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                    buffer.getBuffer<uint64_t>()[j] = j;
                }
                buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                senderChannel->sendBuffer(buffer, sizeof(uint64_t));
            }
            senderChannel.reset();
        }

        t.join();
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(bufferReceived, totalNumBuffer);
}

TEST_F(NetworkStackTest, testPartitionManager) {
    auto partition1 = NesPartition(1, 2, 3, 4);
    auto partition1Copy = NesPartition(1, 2, 3, 4);
    auto partitionManager = std::make_shared<PartitionManager>();
    partitionManager->registerSubpartition(partition1);
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 0);
    partitionManager->registerSubpartition(partition1);
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 1);

    partitionManager->unregisterSubpartition(partition1Copy);
    ASSERT_EQ(partitionManager->isRegistered(partition1), true);
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 0);

    partitionManager->unregisterSubpartition(partition1Copy);
    ASSERT_EQ(partitionManager->isRegistered(partition1), false);
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
                ASSERT_EQ(errorMsg.getErrorType(), Messages::kPartitionNotRegisteredError);
            }

            void onChannelError(Messages::ErrorMessage errorMsg) override {
                errorCallsChannel++;
                if (errorCallsChannel == retryTimes) {
                    channelError.set_value(true);
                }
                NES_INFO("NetworkStackTest: Channel error called!");
                ASSERT_EQ(errorMsg.getErrorType(), Messages::kPartitionNotRegisteredError);
            }

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1", 31337, ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(serverError, channelError)),
            buffMgr);

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto channel = netManager->registerSubpartitionProducer(nodeLocation, NesPartition(1, 22, 333, 4445),
                                                                std::chrono::seconds(1), retryTimes);

        ASSERT_EQ(channel, nullptr);
        ASSERT_EQ(serverError.get_future().get(), true);
        ASSERT_EQ(channelError.get_future().get(), true);
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, testMassiveMultiSending) {
    uint64_t totalNumBuffer = 1'000;
    constexpr uint64_t numSendingThreads = 4;

    // create a couple of NesPartitions
    auto sendingThreads = std::vector<std::thread>();
    auto nesPartitions = std::vector<NesPartition>();
    auto completedPromises = std::vector<std::promise<bool>>();
    std::array<std::atomic<std::uint64_t>, numSendingThreads> bufferCounter;

    for (int i = 0; i < numSendingThreads; i++) {
        nesPartitions.emplace_back(i, i + 10, i + 20, i + 30);
        completedPromises.emplace_back(std::promise<bool>());
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
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1", 31337,
            ExchangeProtocol(partMgr, std::make_shared<ExchangeListenerImpl>(bufferCounter, completedPromises)), buffMgr);

        std::thread receivingThread([&netManager, &nesPartitions, &completedPromises] {
            // register the incoming channel
            for (NesPartition p : nesPartitions) {
                //add random latency
                sleep(rand() % 3);
                netManager->registerSubpartitionConsumer(p);
            }

            for (std::promise<bool>& p : completedPromises) {
                ASSERT_TRUE(p.get_future().get());
            }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);

        for (int i = 0; i < numSendingThreads; i++) {
            std::thread sendingThread([&, i] {
                // register the incoming channel
                NesPartition nesPartition(i, i + 10, i + 20, i + 30);
                auto senderChannel =
                    netManager->registerSubpartitionProducer(nodeLocation, nesPartition, std::chrono::seconds(2), 10);

                if (senderChannel == nullptr) {
                    NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
                    completedPromises[i].set_value(false);
                } else {
                    std::mt19937 rnd;
                    std::uniform_int_distribution gen(5'000, 75'000);
                    for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                        auto buffer = buffMgr->getBufferBlocking();
                        for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                            buffer.getBuffer<uint64_t>()[j] = j;
                        }
                        buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                        senderChannel->sendBuffer(buffer, sizeof(uint64_t));
                        usleep(gen(rnd));
                    }
                    senderChannel.reset();
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
        ASSERT_EQ(true, false);
    }
    for (uint64_t cnt : bufferCounter) {
        ASSERT_EQ(cnt, totalNumBuffer);
    }
}

TEST_F(NetworkStackTest, testNetworkSink) {
    static constexpr auto numSendingThreads = 10;
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    uint64_t totalNumBuffer = 100;

    auto sendingThreads = std::vector<std::thread>();
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    try {
        class ExchangeListener : public ExchangeProtocolListener {
          public:
            NesPartition nesPartition;
            std::promise<bool>& completed;
            atomic<int> eosCnt;
            atomic<int>& bufferCnt;

            ExchangeListener(std::promise<bool>& completed, NesPartition nesPartition, std::atomic<int>& bufferCnt)
                : completed(completed), nesPartition(nesPartition), bufferCnt(bufferCnt), eosCnt(0) {}

            void onServerError(Messages::ErrorMessage ex) override {
                if (ex.getErrorType() != Messages::kPartitionNotRegisteredError) {
                    completed.set_exception(make_exception_ptr(runtime_error("Error")));
                }
            }

            void onChannelError(Messages::ErrorMessage) override {}

            void onDataBuffer(NesPartition id, TupleBuffer&) override {
                if (nesPartition == id) {
                    bufferCnt++;
                }
                ASSERT_EQ(id, nesPartition);
            }
            void onEndOfStream(Messages::EndOfStreamMessage) override {
                eosCnt++;
                if (eosCnt == numSendingThreads) {
                    completed.set_value(true);
                }
            }
        };

        auto pManager = std::make_shared<PartitionManager>();
        auto bMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1", 31337,
            ExchangeProtocol(pManager, std::make_shared<ExchangeListener>(completed, nesPartition, bufferCnt)), bMgr);

        std::thread receivingThread([this, &pManager, &netManager, &nesPartition, &completed] {
            // register the incoming channel
            //add latency
            netManager->registerSubpartitionConsumer(nesPartition);
            ASSERT_TRUE(completed.get_future().get());
            pManager->unregisterSubpartition(nesPartition);
            ASSERT_FALSE(pManager->isRegistered(nesPartition));
        });

        NetworkSink networkSink(schema, 0, netManager, nodeLocation, nesPartition, bMgr, nullptr);

        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
              NodeEngine::WorkerContext workerContext(NodeEngine::NesThread::getId());
                auto rt = NodeEngine::ReconfigurationTask(0, NodeEngine::Initialize, &networkSink);
                networkSink.reconfigure(rt, workerContext);
                std::mt19937 rnd;
                std::uniform_int_distribution gen(50'000, 100'000);
                auto buffMgr = bMgr;
                for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                    auto buffer = buffMgr->getBufferBlocking();
                    for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                        buffer.getBuffer<uint64_t>()[j] = j;
                    }
                    buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                    usleep(gen(rnd));
                    networkSink.writeData(buffer, workerContext);
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
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(bufferCnt, numSendingThreads * totalNumBuffer);
}

TEST_F(NetworkStackTest, testNetworkSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged);
    auto netManager = nodeEngine->getNetworkManager();

    NesPartition nesPartition{1, 22, 33, 44};

    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());
    auto networkSource = std::make_unique<NetworkSource>(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                         netManager, nesPartition);
    ASSERT_TRUE(networkSource->start());

    ASSERT_EQ(nodeEngine->getPartitionManager()->getSubpartitionCounter(nesPartition), 0);
    ASSERT_TRUE(networkSource->stop());
    ASSERT_FALSE(nodeEngine->getPartitionManager()->isRegistered(nesPartition));
}

TEST_F(NetworkStackTest, testStartStopNetworkSrcSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged);
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    auto networkSource = std::make_shared<NetworkSource>(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                         nodeEngine->getNetworkManager(), nesPartition);
    ASSERT_TRUE(networkSource->start());

    auto networkSink = std::make_shared<NetworkSink>(schema, 0, nodeEngine->getNetworkManager(), nodeLocation, nesPartition,
                                                     nodeEngine->getBufferManager(), nullptr);

    ASSERT_TRUE(networkSource->stop());
}

template<typename MockedNodeEngine, typename... ExtraParameters>
std::shared_ptr<MockedNodeEngine> createMockedEngine(const std::string& hostname, uint16_t port, uint64_t bufferSize,
                                                     uint64_t numBuffers, ExtraParameters&&... extraParams) {
    try {
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        auto bufferManager = std::make_shared<NodeEngine::BufferManager>(bufferSize, numBuffers);
        auto queryManager = std::make_shared<NodeEngine::QueryManager>(bufferManager, 0, 1);
        auto networkManagerCreator = [=](NodeEngine::NodeEnginePtr engine) {
            return Network::NetworkManager::create(hostname, port, Network::ExchangeProtocol(partitionManager, engine),
                                                   bufferManager);
        };
        auto compiler = createDefaultQueryCompiler();
        return std::make_shared<MockedNodeEngine>(std::move(streamConf), std::move(bufferManager), std::move(queryManager),
                                                  std::move(networkManagerCreator), std::move(partitionManager),
                                                  std::move(compiler), std::forward<ExtraParameters>(extraParams)...);
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
}

TEST_F(NetworkStackTest, testNetworkSourceSink) {
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    uint64_t totalNumBuffer = 100;

    static constexpr int numSendingThreads = 4;
    auto sendingThreads = std::vector<std::thread>();
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    class MockedNodeEngine : public NodeEngine::NodeEngine {
      public:
        NesPartition nesPartition;
        std::promise<bool>& completed;
        atomic<int> eosCnt = 0;
        atomic<int>& bufferCnt;

        explicit MockedNodeEngine(PhysicalStreamConfigPtr streamConf, NES::NodeEngine::BufferManagerPtr&& bufferManager,
                                  NES::NodeEngine::QueryManagerPtr&& queryManager,
                                  std::function<Network::NetworkManagerPtr(NES::NodeEngine::NodeEnginePtr)>&& networkManagerCreator,
                                  Network::PartitionManagerPtr&& partitionManager, QueryCompilerPtr&& queryCompiler,
                                  std::promise<bool>& completed, NesPartition nesPartition, std::atomic<int>& bufferCnt)
            : NodeEngine(std::move(streamConf), std::move(bufferManager), std::move(queryManager),
                         std::move(networkManagerCreator), std::move(partitionManager), std::move(queryCompiler), 0),
              completed(completed), nesPartition(nesPartition), bufferCnt(bufferCnt) {}
        void onDataBuffer(Network::NesPartition id, TupleBuffer&) override {
            if (nesPartition == id) {
                bufferCnt++;
            }
            ASSERT_EQ(id, nesPartition);
        }

        void onEndOfStream(Network::Messages::EndOfStreamMessage) override {
            eosCnt++;
            if (eosCnt == numSendingThreads) {
                completed.set_value(true);
            }
        }

        void onServerError(Network::Messages::ErrorMessage ex) override {
            if (ex.getErrorType() != Messages::kPartitionNotRegisteredError) {
                completed.set_exception(make_exception_ptr(runtime_error("Error")));
            }
        }

        void onChannelError(Network::Messages::ErrorMessage message) override { NodeEngine::onChannelError(message); }
    };

    auto nodeEngine =
        createMockedEngine<MockedNodeEngine>("127.0.0.1", 31337, bufferSize, buffersManaged, completed, nesPartition, bufferCnt);
    try {
        auto netManager = nodeEngine->getNetworkManager();

        std::thread receivingThread([&]() {
            // register the incoming channel
            NetworkSource source(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), netManager, nesPartition);
            ASSERT_TRUE(source.start());
            ASSERT_TRUE(nodeEngine->getPartitionManager()->isRegistered(nesPartition));
            completed.get_future().get();
            ASSERT_TRUE(source.stop());
        });

        NetworkSink networkSink(schema, 0, netManager, nodeLocation, nesPartition, nodeEngine->getBufferManager(), nullptr);
        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
                NodeEngine::WorkerContext workerContext(NodeEngine::NesThread::getId());
                auto rt = NodeEngine::ReconfigurationTask(0, NodeEngine::Initialize, &networkSink);
                networkSink.reconfigure(rt, workerContext);
                for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
                    for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                        buffer.getBuffer<uint64_t>()[j] = j;
                    }
                    buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                    networkSink.writeData(buffer, workerContext);
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
    } catch (...) {
        FAIL();
    }
    ASSERT_EQ(bufferCnt, numSendingThreads * totalNumBuffer);
    ASSERT_FALSE(nodeEngine->getPartitionManager()->isRegistered(nesPartition));
}

TEST_F(NetworkStackTest, testQEPNetworkSinkSource) {
    //    std::promise<bool> completed;
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};
    SchemaPtr schema = Schema::create()
                           ->addField("id", DataTypeFactory::createInt64())
                           ->addField("one", DataTypeFactory::createInt64())
                           ->addField("value", DataTypeFactory::createInt64());

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged);
    auto netManager = nodeEngine->getNetworkManager();
    // create NetworkSink
    auto networkSource1 = std::make_shared<NetworkSource>(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                          netManager, nesPartition);
    auto testSink = std::make_shared<TestSink>(schema, nodeEngine->getBufferManager());

    auto query = TestQuery::from(schema).sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0]);

    GeneratedQueryExecutionPlanBuilder builderReceiverQEP = GeneratedQueryExecutionPlanBuilder::create()
                                                                .setCompiler(nodeEngine->getCompiler())
                                                                .addOperatorQueryPlan(generatableOperators)
                                                                .addSink(testSink)
                                                                .addSource(networkSource1)
                                                                .setBufferManager(nodeEngine->getBufferManager())
                                                                .setQueryManager(nodeEngine->getQueryManager())
                                                                .setQueryId(1)
                                                                .setQuerySubPlanId(1);

    // creating query plan
    auto testSource =
        createDefaultDataSourceWithSchemaForOneBuffer(schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), 1);
    auto networkSink = std::make_shared<NetworkSink>(schema, 2, netManager, nodeLocation, nesPartition,
                                                     nodeEngine->getBufferManager(), nodeEngine->getQueryManager());

    auto query2 = TestQuery::from(schema).filter(Attribute("id") < 5).sink(DummySink::create());

    auto queryPlan2 = typeInferencePhase->execute(query2.getQueryPlan());
    auto generatableOperators2 = translatePhase->transform(queryPlan2->getRootOperators()[0]);

    GeneratedQueryExecutionPlanBuilder builderGeneratorQEP = GeneratedQueryExecutionPlanBuilder::create()
                                                                 .setCompiler(nodeEngine->getCompiler())
                                                                 .addSink(networkSink)
                                                                 .addOperatorQueryPlan(generatableOperators2)
                                                                 .addSource(testSource)
                                                                 .setBufferManager(nodeEngine->getBufferManager())
                                                                 .setQueryManager(nodeEngine->getQueryManager())
                                                                 .setQueryId(2)
                                                                 .setQuerySubPlanId(2);

    nodeEngine->registerQueryInNodeEngine(builderGeneratorQEP.build());
    nodeEngine->registerQueryInNodeEngine(builderReceiverQEP.build());

    nodeEngine->startQuery(2);
    nodeEngine->startQuery(1);

    ASSERT_EQ(10, testSink->completed.get_future().get());

    nodeEngine->undeployQuery(2);
    nodeEngine->undeployQuery(1);
}

}// namespace Network
}// namespace NES