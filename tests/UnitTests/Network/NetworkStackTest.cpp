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
#include <API/QueryAPI.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/SourceCreator.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/UtilityFunctions.hpp>

#include "../../util/TestQuery.hpp"
#include "../../util/TestQueryCompiler.hpp"
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
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

namespace Network {
class NetworkStackTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("NetworkStackTest.log", NES::LOG_DEBUG);
        NES_INFO("SetUpTestCase NetworkStackTest");
    }

    static void TearDownTestCase() { NES_INFO("TearDownTestCase NetworkStackTest."); }

    /* Will be called before a  test is executed. */
    void SetUp() override { NES_INFO("Setup NetworkStackTest"); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("TearDown NetworkStackTest"); }
};

class TestSink : public SinkMedium {
  public:
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::PRINT_SINK; }

    TestSink(const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), 0){};

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
    ;

    std::mutex m;
    std::promise<uint64_t> completed;
};

void fillBuffer(TupleBuffer& buf, const Runtime::DynamicMemoryLayout::DynamicRowLayoutPtr& memoryLayout) {

    auto bindedRowLayout = memoryLayout->bind(buf);
    auto recordIndexFields = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(0, bindedRowLayout);
    auto fields01 = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(1, bindedRowLayout);
    auto fields02 = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(2, bindedRowLayout);

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

    void onChannelError(Messages::ErrorMessage) override {}
};

TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto exchangeProtocol = ExchangeProtocol(partMgr, std::make_shared<DummyExchangeProtocolListener>());
        ZmqServer server("127.0.0.1", 31337, 4, exchangeProtocol, buffMgr);
        server.start();
        ASSERT_EQ(server.isServerRunning(), true);
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
            explicit InternalListener(std::promise<bool>& p) : completed(p) {}

            void onDataBuffer(NesPartition, TupleBuffer&) override {}
            void onEndOfStream(Messages::EndOfStreamMessage) override { completed.set_value(true); }
            void onServerError(Messages::ErrorMessage) override {}

            void onChannelError(Messages::ErrorMessage) override {}

          private:
            std::promise<bool>& completed;
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto ep = ExchangeProtocol(partMgr, std::make_shared<InternalListener>(completed));
        auto netManager = NetworkManager::create("127.0.0.1", 31337, std::move(ep), buffMgr);

        auto nesPartition = NesPartition(0, 0, 0, 0);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };

        std::thread t([&netManager, &completed, &nesPartition] {
            // register the incoming channel
            auto cnt = netManager->registerSubpartitionConsumer(nesPartition, std::make_shared<DataEmitterImpl>());
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

            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create("127.0.0.1",
                                   31337,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, &completed] {
            // register the incoming channel
            sleep(3);// intended stalling to simulate latency
            netManager->registerSubpartitionConsumer(nesPartition, std::make_shared<DataEmitterImpl>());
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

    uint64_t totalNumBuffer = 100000;
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

            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create("127.0.0.1",
                                   31337,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(bufferReceived, completedProm)),
                                   buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread t([&netManager, &nesPartition, &completedProm, totalNumBuffer] {
            // register the incoming channel
            netManager->registerSubpartitionConsumer(nesPartition, std::make_shared<DataEmitterImpl>());
            auto startTime = std::chrono::steady_clock::now().time_since_epoch();
            EXPECT_TRUE(completedProm.get_future().get());
            auto stopTime = std::chrono::steady_clock::now().time_since_epoch();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime);
            double bytes = totalNumBuffer * bufferSize;
            double throughput = (bytes * 1'000'000'000) / (elapsed.count() * 1024.0 * 1024.0);
            std::cout << "Sent " << bytes << " bytes :: throughput " << throughput << std::endl;
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager->registerSubpartitionProducer(nodeLocation, nesPartition, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_DEBUG("NetworkStackTest: Error in registering OutputChannel!");
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
    struct DataEmitterImpl : public DataEmitter {
        void emitWork(TupleBuffer&) override {}
    };
    partitionManager->registerSubpartition(partition1, std::make_shared<DataEmitterImpl>());
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 0UL);
    partitionManager->registerSubpartition(partition1, std::make_shared<DataEmitterImpl>());
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 1UL);

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
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create("127.0.0.1",
                                   31337,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(serverError, channelError)),
                                   buffMgr);

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto channel = netManager->registerSubpartitionProducer(nodeLocation,
                                                                NesPartition(1, 22, 333, 4445),
                                                                std::chrono::seconds(1),
                                                                retryTimes);

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
    std::array<std::atomic<std::uint64_t>, numSendingThreads> bufferCounter{};

    for (uint64_t i = 0ull; i < numSendingThreads; ++i) {
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
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1",
            31337,
            ExchangeProtocol(partMgr, std::make_shared<ExchangeListenerImpl>(bufferCounter, completedPromises)),
            buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread receivingThread([&netManager, &nesPartitions, &completedPromises] {
            // register the incoming channel
            for (NesPartition p : nesPartitions) {
                //add random latency
                sleep(rand() % 3);
                netManager->registerSubpartitionConsumer(p, std::make_shared<DataEmitterImpl>());
            }

            for (std::promise<bool>& p : completedPromises) {
                EXPECT_TRUE(p.get_future().get());
            }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);

        for (auto i = 0ULL; i < numSendingThreads; ++i) {
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
                    for (auto i = 0ULL; i < totalNumBuffer; ++i) {
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

    NodeLocation nodeLocation{0, "127.0.0.1", 31339};
    NesPartition nesPartition{1, 22, 33, 44};

    try {
        class ExchangeListener : public ExchangeProtocolListener {
          public:
            NesPartition nesPartition;
            std::promise<bool>& completed;
            atomic<int> eosCnt{0};
            atomic<int>& bufferCnt;

            ExchangeListener(std::promise<bool>& completed, NesPartition nesPartition, std::atomic<int>& bufferCnt)
                : nesPartition(nesPartition), completed(completed), bufferCnt(bufferCnt) {}

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
                auto prev = eosCnt.fetch_add(1);
                if (prev == 0) {
                    completed.set_value(true);
                }
            }
        };

        auto pManager = std::make_shared<PartitionManager>();
        auto bMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            "127.0.0.1",
            31339,
            ExchangeProtocol(pManager, std::make_shared<ExchangeListener>(completed, nesPartition, bufferCnt)),
            bMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread receivingThread([&pManager, &netManager, &nesPartition, &completed] {
            // register the incoming channel
            //add latency
            netManager->registerSubpartitionConsumer(nesPartition, std::make_shared<DataEmitterImpl>());
            EXPECT_TRUE(completed.get_future().get());
            //            pManager->unregisterSubpartition(nesPartition);
            ASSERT_FALSE(pManager->isRegistered(nesPartition));
        });

        auto networkSink = std::make_shared<NetworkSink>(schema, 0, netManager, nodeLocation, nesPartition, bMgr, nullptr);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        auto nodeEngine =
            Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged, 64, 64);

        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
                Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), nodeEngine->getBufferManager());
                auto rt = Runtime::ReconfigurationMessage(0, Runtime::Initialize, networkSink);
                networkSink->reconfigure(rt, workerContext);
                std::mt19937 rnd;
                std::uniform_int_distribution gen(50'000, 100'000);
                const auto& buffMgr = bMgr;
                for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                    auto buffer = buffMgr->getBufferBlocking();
                    for (uint64_t j = 0; j < bufferSize / sizeof(uint64_t); ++j) {
                        buffer.getBuffer<uint64_t>()[j] = j;
                    }
                    buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));
                    usleep(gen(rnd));
                    networkSink->writeData(buffer, workerContext);
                }
                auto rtEnd = Runtime::ReconfigurationMessage(0, Runtime::HardEndOfStream, networkSink);
                networkSink->reconfigure(rtEnd, workerContext);
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
    ASSERT_EQ(static_cast<uint32_t>(bufferCnt.load()), numSendingThreads * totalNumBuffer);
}

TEST_F(NetworkStackTest, testNetworkSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine =
        Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged, 64, 64);
    auto netManager = nodeEngine->getNetworkManager();

    NesPartition nesPartition{1, 22, 33, 44};

    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());
    auto networkSource = std::make_shared<NetworkSource>(schema,
                                                         nodeEngine->getBufferManager(),
                                                         nodeEngine->getQueryManager(),
                                                         netManager,
                                                         nesPartition,
                                                         64);
    EXPECT_TRUE(networkSource->start());

    ASSERT_EQ(nodeEngine->getPartitionManager()->getSubpartitionCounter(nesPartition), 0UL);
    EXPECT_TRUE(networkSource->stop());
    netManager->unregisterSubpartitionConsumer(nesPartition);
    ASSERT_FALSE(nodeEngine->getPartitionManager()->isRegistered(nesPartition));
    nodeEngine->stop();
}

TEST_F(NetworkStackTest, testStartStopNetworkSrcSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine =
        Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged, 64, 64);
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    auto networkSource = std::make_shared<NetworkSource>(schema,
                                                         nodeEngine->getBufferManager(),
                                                         nodeEngine->getQueryManager(),
                                                         nodeEngine->getNetworkManager(),
                                                         nesPartition,
                                                         64);
    EXPECT_TRUE(networkSource->start());

    auto networkSink = std::make_shared<NetworkSink>(schema,
                                                     0,
                                                     nodeEngine->getNetworkManager(),
                                                     nodeLocation,
                                                     nesPartition,
                                                     nodeEngine->getBufferManager(),
                                                     nullptr);

    EXPECT_TRUE(networkSource->stop());
    nodeEngine->stop();
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
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        std::vector<Runtime::BufferManagerPtr> bufferManagers = {
            std::make_shared<Runtime::BufferManager>(bufferSize, numBuffers)};
        auto queryManager = std::make_shared<Runtime::QueryManager>(bufferManagers, 0, 1);
        auto networkManagerCreator = [=](const Runtime::NodeEnginePtr& engine) {
            return Network::NetworkManager::create(hostname,
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

        return std::make_shared<MockedNodeEngine>(std::move(streamConf),
                                                  std::make_shared<Runtime::HardwareManager>(),
                                                  std::move(bufferManagers),
                                                  std::move(queryManager),
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

TEST_F(NetworkStackTest, testNetworkSourceSink) {
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    uint64_t totalNumBuffer = 100;

    static constexpr int numSendingThreads = 4;
    auto sendingThreads = std::vector<std::thread>();
    auto schema = Schema::create()->addField("id", DataTypeFactory::createInt64());

    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        NesPartition nesPartition;
        std::promise<bool>& completed;
        atomic<int> eosCnt = 0;
        atomic<int>& bufferCnt;

        explicit MockedNodeEngine(PhysicalStreamConfigPtr streamConf,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  NES::Runtime::QueryManagerPtr&& queryManager,
                                  std::function<Network::NetworkManagerPtr(NES::Runtime::NodeEnginePtr)>&& networkManagerCreator,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& queryCompiler,
                                  std::promise<bool>& completed,
                                  NesPartition nesPartition,
                                  std::atomic<int>& bufferCnt)
            : NodeEngine(std::move(streamConf),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryManager),
                         std::move(networkManagerCreator),
                         std::move(partitionManager),
                         std::move(queryCompiler),
                         std::make_shared<NES::Runtime::StateManager>(0),
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
            auto source = std::make_shared<NetworkSource>(schema,
                                                          nodeEngine->getBufferManager(),
                                                          nodeEngine->getQueryManager(),
                                                          netManager,
                                                          nesPartition,
                                                          64);
            EXPECT_TRUE(source->start());
            EXPECT_TRUE(nodeEngine->getPartitionManager()->isRegistered(nesPartition));
            completed.get_future().get();
            EXPECT_TRUE(source->stop());
        });

        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        auto nodeEngine =
            Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31338, streamConf, 1, bufferSize, buffersManaged, 64, 64);

        auto networkSink = std::make_shared<NetworkSink>(schema,
                                                         0,
                                                         netManager,
                                                         nodeLocation,
                                                         nesPartition,
                                                         nodeEngine->getBufferManager(),
                                                         nullptr);
        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
                Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), nodeEngine->getBufferManager());
                auto rt = Runtime::ReconfigurationMessage(0, Runtime::Initialize, networkSink);
                networkSink->reconfigure(rt, workerContext);
                for (uint64_t i = 0; i < totalNumBuffer; ++i) {
                    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
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
    } catch (...) {
        FAIL();
    }
    auto const bf = bufferCnt.load();
    ASSERT_TRUE(bf > 0);
    ASSERT_EQ(static_cast<std::size_t>(bf), numSendingThreads * totalNumBuffer);
    ASSERT_FALSE(nodeEngine->getPartitionManager()->isRegistered(nesPartition));
    nodeEngine->stop();
    nodeEngine = nullptr;
}

TEST_F(NetworkStackTest, testQEPNetworkSinkSource) {
    //    std::promise<bool> completed;
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};
    SchemaPtr schema = Schema::create()
                           ->addField("test$id", DataTypeFactory::createInt64())
                           ->addField("test$one", DataTypeFactory::createInt64())
                           ->addField("test$value", DataTypeFactory::createInt64());

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                                   31337,
                                                                   streamConf,
                                                                   1,
                                                                   bufferSize,
                                                                   buffersManaged,
                                                                   64,
                                                                   12,
                                                                   NES::Runtime::NumaAwarenessFlag::DISABLED);
    auto netManager = nodeEngine->getNetworkManager();
    // create NetworkSink

    auto networkSourceDescriptor1 = std::make_shared<TestUtils::TestSourceDescriptor>(
        schema,
        [&](OperatorId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) -> DataSourcePtr {
            return std::make_shared<NetworkSource>(schema,
                                                   nodeEngine->getBufferManager(),
                                                   nodeEngine->getQueryManager(),
                                                   netManager,
                                                   nesPartition,
                                                   numSourceLocalBuffers,
                                                   successors);
        });

    auto testSink = std::make_shared<TestSink>(schema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(networkSourceDescriptor1).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    queryPlan->setQueryId(0);
    queryPlan->setQuerySubPlanId(0);
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
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
            return createDefaultDataSourceWithSchemaForOneBuffer(schema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 1,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto networkSink = std::make_shared<NetworkSink>(schema,
                                                     1,
                                                     netManager,
                                                     nodeLocation,
                                                     nesPartition,
                                                     nodeEngine->getBufferManager(),
                                                     nodeEngine->getQueryManager());
    auto networkSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(networkSink);
    auto query2 = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(networkSinkDescriptor);

    auto queryPlan2 = typeInferencePhase->execute(query2.getQueryPlan());
    queryPlan2->setQueryId(0);
    queryPlan2->setQuerySubPlanId(1);
    auto request2 = QueryCompilation::QueryCompilationRequest::create(queryPlan2, nodeEngine);
    auto result2 = queryCompiler->compileQuery(request2);
    auto builderGeneratorQEP = result2->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(builderGeneratorQEP);
    nodeEngine->registerQueryInNodeEngine(builderReceiverQEP);

    nodeEngine->startQuery(1);
    nodeEngine->startQuery(0);

    ASSERT_EQ(10ULL, testSink->completed.get_future().get());

    nodeEngine->undeployQuery(1);
    nodeEngine->undeployQuery(0);
}

}// namespace Network
}// namespace NES