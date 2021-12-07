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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
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

    TestSink(const SchemaPtr& schema, Runtime::QueryManagerPtr queryManager, const Runtime::BufferManagerPtr& bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager), queryManager, 0){};

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
        auto netManager = NetworkManager::create(0, "127.0.0.1", 31337, std::move(exchangeProtocol), buffMgr);
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
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}

          private:
            std::promise<bool>& completed;
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto ep = ExchangeProtocol(partMgr, std::make_shared<InternalListener>(completed));
        auto netManager = NetworkManager::create(0, "127.0.0.1", 31337, std::move(ep), buffMgr);

        auto nesPartition = NesPartition(0, 0, 0, 0);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };

        std::thread t([&netManager, &completed, &nesPartition] {
            // register the incoming channel
            auto cnt = netManager->registerSubpartitionConsumer(nesPartition,
                                                                netManager->getServerLocation(),
                                                                std::make_shared<DataEmitterImpl>());
            NES_INFO("NetworkStackTest: SubpartitionConsumer registered with cnt" << cnt);
            auto v = completed.get_future().get();
            netManager->unregisterSubpartitionConsumer(nesPartition);
            ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 3);
        senderChannel->close();
        senderChannel.reset();
        netManager->unregisterSubpartitionProducer(nesPartition);

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
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
            void onChannelError(Messages::ErrorMessage) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   31337,
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

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel =
            netManager->registerSubpartitionProducer(nodeLocation, nesPartition, buffMgr, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering DataChannel!");
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

    uint64_t totalNumBuffer = 1000;
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
        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto netManager =
            NetworkManager::create(0,
                                   "127.0.0.1",
                                   31337,
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
            std::cout << "Sent " << bytes << " bytes :: throughput " << throughput << std::endl;
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
                senderChannel->sendBuffer(buffer, sizeof(uint64_t));
            }
            senderChannel.reset();
        }
        netManager->unregisterSubpartitionProducer(nesPartition);
        t.join();
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(bufferReceived, totalNumBuffer);
}

TEST_F(NetworkStackTest, testPartitionManager) {
    auto nodeLocation = NodeLocation(0, "*", 1);
    auto partition1 = NesPartition(1, 2, 3, 4);
    auto partition1Copy = NesPartition(1, 2, 3, 4);
    auto partitionManager = std::make_shared<PartitionManager>();
    struct DataEmitterImpl : public DataEmitter {
        void emitWork(TupleBuffer&) override {}
    };
    partitionManager->registerSubpartitionConsumer(partition1, nodeLocation, std::make_shared<DataEmitterImpl>());
    ASSERT_EQ(*partitionManager->getSubpartitionConsumerCounter(partition1Copy), 1UL);
    partitionManager->registerSubpartitionConsumer(partition1, nodeLocation, std::make_shared<DataEmitterImpl>());
    ASSERT_EQ(*partitionManager->getSubpartitionConsumerCounter(partition1Copy), 2UL);

    partitionManager->unregisterSubpartitionConsumer(partition1Copy);
    ASSERT_EQ(*partitionManager->getSubpartitionConsumerCounter(partition1Copy), 1UL);
    partitionManager->unregisterSubpartitionConsumer(partition1Copy);
    ASSERT_EQ(partitionManager->getConsumerRegistrationStatus(partition1), PartitionRegistrationStatus::Deleted);
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
                                   31337,
                                   ExchangeProtocol(partMgr, std::make_shared<ExchangeListener>(serverError, channelError)),
                                   buffMgr);

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto channel = netManager->registerSubpartitionProducer(nodeLocation,
                                                                NesPartition(1, 22, 333, 4445),
                                                                buffMgr,
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
            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
        };

        auto partMgr = std::make_shared<PartitionManager>();
        auto buffMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);

        auto netManager = NetworkManager::create(
            0,
            "127.0.0.1",
            31337,
            ExchangeProtocol(partMgr, std::make_shared<ExchangeListenerImpl>(bufferCounter, completedPromises)),
            buffMgr);

        struct DataEmitterImpl : public DataEmitter {
            void emitWork(TupleBuffer&) override {}
        };
        std::thread receivingThread([&netManager, &nesPartitions, &completedPromises] {
            // register the incoming channel
            NodeLocation nodeLocation{0, "127.0.0.1", 31337};
            for (NesPartition p : nesPartitions) {
                //add random latency
                sleep(rand() % 3);
                netManager->registerSubpartitionConsumer(p, nodeLocation, std::make_shared<DataEmitterImpl>());
            }

            for (std::promise<bool>& p : completedPromises) {
                EXPECT_TRUE(p.get_future().get());
            }
            for (NesPartition p : nesPartitions) {
                netManager->unregisterSubpartitionConsumer(p);
            }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);

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
                if (ex.getErrorType() != Messages::ErrorType::PartitionNotRegisteredError) {
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

            void onEvent(NesPartition, Runtime::BaseEvent&) override {}
        };

        auto pManager = std::make_shared<PartitionManager>();
        auto bMgr = std::make_shared<Runtime::BufferManager>(bufferSize, buffersManaged);
        auto bSt = std::make_shared<Runtime::BufferStorage>();
        auto netManager = NetworkManager::create(
            0,
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
            auto nodeLocation = NodeLocation(0, "127.0.0.1", 31337);
            netManager->registerSubpartitionConsumer(nesPartition, nodeLocation, std::make_shared<DataEmitterImpl>());
            EXPECT_TRUE(completed.get_future().get());
            ASSERT_FALSE(pManager->getConsumerRegistrationStatus(nesPartition) == PartitionRegistrationStatus::Deleted);
            netManager->unregisterSubpartitionConsumer(nesPartition);
        });

        auto networkSink =
            std::make_shared<NetworkSink>(schema, 0, netManager, nodeLocation, nesPartition, bMgr, nullptr, bSt, 1);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        auto nodeEngine =
            Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1", 31337, streamConf, 1, bufferSize, buffersManaged, 64, 64);

        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([&] {
                // register the incoming channel
                Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);
                auto rt = Runtime::ReconfigurationMessage(0, Runtime::Initialize, networkSink, std::make_any<uint32_t>(1));
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

}// namespace Network
}// namespace NES