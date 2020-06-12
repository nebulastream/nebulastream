#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Util/ThreadBarrier.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <Operators/Operator.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <gtest/gtest.h>
#include <random>

using namespace std;

namespace NES {
const size_t buffersManaged = 8*1024;
const size_t bufferSize = 4*1024;

struct TestStruct {
    int64_t id;
    int64_t one;
    int64_t value;
};

namespace Network {
class NetworkStackTest : public testing::Test {
  public:
    PartitionManagerPtr partitionManager;
    NodeEnginePtr nodeEngine;
    BufferManagerPtr bufferManager;
    SchemaPtr schema;

    static void SetUpTestCase() {
        NES::setupLogging("NetworkStackTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup NetworkStackTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down NetworkStackTest class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        std::cout << "Setup BufferManagerTest test case." << std::endl;
        nodeEngine = std::make_shared<NodeEngine>();
        nodeEngine->createBufferManager(bufferSize, buffersManaged);
        nodeEngine->startQueryManager();
        bufferManager = nodeEngine->getBufferManager();
        partitionManager = std::make_shared<PartitionManager>();

        schema = Schema::create()
            ->addField(createField("id", BasicType::INT64))
            ->addField(createField("one", BasicType::INT64))
            ->addField(createField("value", BasicType::INT64));
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        std::cout << "Tear down BufferManagerTest test case." << std::endl;
    }
};

class TestSink : public DataSink {
  public:

    bool writeData(TupleBuffer& input_buffer) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink:\n" <<UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchema()));

        uint64_t sum = 0;
        for (size_t i = 0; i < input_buffer.getNumberOfTuples(); ++i) {
            sum += input_buffer.getBuffer<TestStruct>()[i].value;
        }

        completed.set_value(sum);
        return true;
    }

    const std::string toString() const override {
        return "";
    }

    void setup() override {
    };

    void shutdown() override {
    };

    ~TestSink() override {
    };

    SinkType getType() const override {
        return SinkType::PRINT_SINK;
    }

  public:
    std::mutex m;
    std::promise<uint64_t> completed;
};

void fillBuffer(TupleBuffer& buf, MemoryLayoutPtr memoryLayout) {
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->write(
            buf, recordIndex);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/1)->write(
            buf, 1);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/2)->write(
            buf, recordIndex%2);
    }
    buf.setNumberOfTuples(10);
}

TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        auto exchangeProtocol = std::make_shared<ExchangeProtocol>(bufferManager, partitionManager,
                                                                   nodeEngine->getQueryManager());
        ZmqServer server("127.0.0.1", 31337, 4, exchangeProtocol);
        server.start();
        ASSERT_EQ(server.getIsRunning(), true);
    } catch (...) {
        // shutdown failed
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, dispatcherMustStartAndStop) {
    try {
        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager());
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);
    }
    catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, startCloseChannel) {
    try {
        // start zmqServer
        std::promise<bool> completed;
        auto onBuffer = [](NesPartition id, TupleBuffer& buf) {};
        auto onError = [&completed](Messages::ErroMessage ex) {
          if (ex.getErrorType() != Messages::PartitionNotRegisteredError) {
              completed.set_exception(make_exception_ptr(runtime_error("Error")));
          }
        };
        auto onEndOfStream = [&completed](Messages::EndOfStreamMessage p) {
          completed.set_value(true);
        };

        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                               onBuffer, onEndOfStream, onError);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        auto nesPartition = NesPartition(0, 0, 0, 0);

        std::thread t([&netManager, &completed, &nesPartition] {
          // register the incoming channel
          auto cnt = netManager.registerSubpartitionConsumer(nesPartition);
          NES_INFO("NetworkStackTest: SubpartitionConsumer registered with cnt" << cnt);
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel =
            netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, std::chrono::seconds(1), 3);
        senderChannel->close();
        delete senderChannel;

        t.join();
    }
    catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, testSendData) {
    std::promise<bool> completedProm;

    bool bufferReceived = false;
    bool completed = false;
    auto nesPartition = NesPartition(1, 22, 333, 444);

    try {
        // start zmqServer
        auto onBuffer = [&bufferReceived](NesPartition id, TupleBuffer& buf) {
          bufferReceived = (buf.getBufferSize() == bufferSize)
              && (id.getQueryId(), 1) && (id.getOperatorId(), 22) && (id.getPartitionId(), 333)
              && (id.getSubpartitionId(), 444);
        };

        auto onError = [](Messages::ErroMessage ex) {
          //can be empty for this test
        };

        auto onEndOfStream = [&completedProm](Messages::EndOfStreamMessage p) { completedProm.set_value(true); };

        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                               onBuffer, onEndOfStream, onError);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        std::thread t([&netManager, &nesPartition, &completedProm, &completed] {
          // register the incoming channel
          sleep(3); // intended stalling to simulate latency
          netManager.registerSubpartitionConsumer(nesPartition);
          auto future = completedProm.get_future();
          if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
              completed = future.get();
          } else {
              NES_ERROR("NetworkStackTest: Receiving thread timed out!");
          }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        OutputChannel* senderChannel;
        senderChannel =
            netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            // create testbuffer
            auto buffer = bufferManager->getBufferBlocking();
            buffer.getBuffer<uint64_t>()[0] = 0;
            buffer.setNumberOfTuples(1);
            senderChannel->sendBuffer(buffer, sizeof(uint64_t));
            delete senderChannel;
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

    size_t totalNumBuffer = 1'000'000;
    size_t bufferReceived = 0;
    auto nesPartition = NesPartition(1, 22, 333, 444);

    try {
        // start zmqServer
        auto onBuffer = [&bufferReceived](NesPartition id, TupleBuffer& buffer) {
          ASSERT_EQ((buffer.getBufferSize() == bufferSize)
                        && (id.getQueryId(), 1) && (id.getOperatorId(), 22) && (id.getPartitionId(), 333)
                        && (id.getSubpartitionId(), 444), true);
          for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
              ASSERT_EQ(buffer.getBuffer<uint64_t>()[j], j);
          }
          bufferReceived++;
        };

        auto onError = [](Messages::ErroMessage ex) {
          //can be empty for this test
        };

        auto onEndOfStream = [&completedProm](Messages::EndOfStreamMessage p) { completedProm.set_value(true); };

        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                               onBuffer, onEndOfStream, onError);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        std::thread t([&netManager, &nesPartition, &completedProm] {
          // register the incoming channel
          netManager.registerSubpartitionConsumer(nesPartition);
          ASSERT_TRUE(completedProm.get_future().get());
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel =
            netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, std::chrono::seconds(1), 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            for (size_t i = 0; i < totalNumBuffer; ++i) {
                auto buffer = bufferManager->getBufferBlocking();
                for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
                    buffer.getBuffer<uint64_t>()[j] = j;
                }
                buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
                senderChannel->sendBuffer(buffer, sizeof(uint64_t));
            }
            delete senderChannel;
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
    int retryTimes = 3;
    int errorCallsServer = 0;
    int errorCallsChannel = 0;

    try {
        // start zmqServer
        std::promise<bool> serverError;
        std::promise<bool> channelError;

        auto onBuffer = [](NesPartition id, TupleBuffer& buf) {};

        auto onErrorServer = [&serverError, &retryTimes, &errorCallsServer](Messages::ErroMessage errorMsg) {
          errorCallsServer++;
          if (errorCallsServer == retryTimes) {
              serverError.set_value(true);
          }
          NES_INFO("NetworkStackTest: Server error called!");
          ASSERT_EQ(errorMsg.getErrorType(), Messages::PartitionNotRegisteredError);
        };

        auto onErrorChannel = [&channelError, &retryTimes, &errorCallsChannel](Messages::ErroMessage errorMsg) {
          errorCallsChannel++;
          if (errorCallsChannel == retryTimes) {
              channelError.set_value(true);
          }
          NES_INFO("NetworkStackTest: Channel error called!");
          ASSERT_EQ(errorMsg.getErrorType(), Messages::PartitionNotRegisteredError);
        };
        auto onEndOfStream = [](Messages::EndOfStreamMessage) {};

        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                               onBuffer, onEndOfStream, onErrorServer);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto channel = netManager.registerSubpartitionProducer(nodeLocation,
                                                               NesPartition(1, 22, 333, 4445),
                                                               onErrorChannel,
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
    size_t totalNumBuffer = 1'000;
    constexpr size_t numSendingThreads = 4;

    // create a couple of NesPartitions
    auto sendingThreads = std::vector<std::thread>();
    auto nesPartitions = std::vector<NesPartition>();
    auto completedPromises = std::vector<std::promise<bool>>();
    std::array<std::atomic<std::size_t>, numSendingThreads> bufferCounter;


    for (int i = 0; i < numSendingThreads; i++) {
        nesPartitions.emplace_back(i, i + 10, i + 20, i + 30);
        completedPromises.emplace_back(std::promise<bool>());
        bufferCounter[i].store(0);
    }

    try {
        // start zmqServer
        auto onBuffer = [&bufferCounter](NesPartition id, TupleBuffer& buffer) {
          for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
              ASSERT_EQ(buffer.getBuffer<uint64_t>()[j], j);
          }
          bufferCounter[id.getQueryId()]++;
          usleep(1000);
        };

        auto onError = [](Messages::ErroMessage ex) {};

        auto onEndOfStream = [&completedPromises](Messages::EndOfStreamMessage p) {
          completedPromises[p.getChannelId().getNesPartition().getQueryId()].set_value(true);
        };

        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                               onBuffer, onEndOfStream, onError);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        std::thread receivingThread([&netManager, &nesPartitions, &completedPromises] {
          // register the incoming channel
          for (NesPartition p: nesPartitions) {
              //add random latency
              sleep(rand()%3);
              netManager.registerSubpartitionConsumer(p);
          }

          for (std::promise<bool>& p: completedPromises) {
              ASSERT_TRUE(p.get_future().get());
          }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);

        for (int i = 0; i < numSendingThreads; i++) {
            std::thread sendingThread([&, i] {
              // register the incoming channel
              NesPartition nesPartition(i, i + 10, i + 20, i + 30);
              auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError,
                                                                           std::chrono::seconds(2), 10);

              if (senderChannel == nullptr) {
                  NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
                  completedPromises[i].set_value(false);
              } else {
                  std::mt19937 rnd;
                  std::uniform_int_distribution gen(50'000, 100'000);
                  for (size_t i = 0; i < totalNumBuffer; ++i) {
                      auto buffer = bufferManager->getBufferBlocking();
                      for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
                          buffer.getBuffer<uint64_t>()[j] = j;
                      }
                      buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
                      senderChannel->sendBuffer(buffer, sizeof(uint64_t));
                      usleep(gen(rnd));
                  }
                  delete senderChannel;
              }
            });
            sendingThreads.emplace_back(std::move(sendingThread));
        }

        for (std::thread& t: sendingThreads) {
            if (t.joinable()) {
                t.join();
            }
        }

        receivingThread.join();
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    for (size_t cnt: bufferCounter) {
        ASSERT_EQ(cnt, totalNumBuffer);
    }
}

TEST_F(NetworkStackTest, testNetworkSink) {
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    atomic<int> eosCnt = 0;
    size_t totalNumBuffer = 100;

    int numSendingThreads = 10;
    auto sendingThreads = std::vector<std::thread>();
    auto schema = Schema::create()->addField(createField("id", BasicType::INT64));

    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    try {
        // create NetworkSink
        auto onError = [&completed](Messages::ErroMessage ex) {
          if (ex.getErrorType() != Messages::PartitionNotRegisteredError) {
              completed.set_exception(make_exception_ptr(runtime_error("Error")));
          }
        };
        auto onEndOfStream = [&eosCnt, &completed, &numSendingThreads](Messages::EndOfStreamMessage p) {
          eosCnt++;
          if (eosCnt == numSendingThreads) {
              completed.set_value(true);
          }
        };

        auto onBuffer = [&nesPartition, &bufferCnt](NesPartition id, TupleBuffer& buf) {
          NES_DEBUG("NetworkStackTest: Received buffer " << id.toString());
          if (nesPartition == id) {
              bufferCnt++;
          }
          ASSERT_EQ(id, nesPartition);
        };

        auto exchangeProtocol =
            std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                               onBuffer, onEndOfStream, onError);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        std::thread receivingThread([this, &netManager, &nesPartition, &completed] {
          // register the incoming channel
          //add latency
          netManager.registerSubpartitionConsumer(nesPartition);
          completed.get_future().get();
          this->partitionManager->unregisterSubpartition(nesPartition);
          ASSERT_FALSE(this->partitionManager->isRegistered(nesPartition));
        });

        NetworkSink networkSink{schema, netManager, nodeLocation, nesPartition};

        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([this, &networkSink, &totalNumBuffer, threadNr] {
              // register the incoming channel
              std::mt19937 rnd;
              std::uniform_int_distribution gen(50'000, 100'000);
              for (size_t i = 0; i < totalNumBuffer; ++i) {
                  auto buffer = bufferManager->getBufferBlocking();
                  for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
                      buffer.getBuffer<uint64_t>()[j] = j;
                  }
                  buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
                  usleep(gen(rnd));
                  networkSink.writeData(buffer);
              }
            });
            sendingThreads.emplace_back(std::move(sendingThread));
        }

        for (std::thread& t: sendingThreads) {
            if (t.joinable()) {
                t.join();
            }
        }
        receivingThread.join();
    }
    catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(bufferCnt, numSendingThreads * totalNumBuffer);
}

TEST_F(NetworkStackTest, testNetworkSource) {
    auto exchangeProtocol = std::make_shared<ExchangeProtocol>(bufferManager, partitionManager,
                                                               nodeEngine->getQueryManager());
    NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

    NesPartition nesPartition{1, 22, 33, 44};

    auto networkSource = new NetworkSource{schema, bufferManager, nodeEngine->getQueryManager(),
                                           netManager, nesPartition};
    ASSERT_TRUE(networkSource->start());

    ASSERT_EQ(partitionManager->getSubpartitionCounter(nesPartition), 0);
    ASSERT_TRUE(networkSource->stop());
    delete networkSource;
    ASSERT_FALSE(partitionManager->isRegistered(nesPartition));
}

TEST_F(NetworkStackTest, testStartStopNetworkSrcSink) {
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    // create NetworkSink
    auto exchangeProtocol = std::make_shared<ExchangeProtocol>(bufferManager, partitionManager,
                                                               nodeEngine->getQueryManager());
    NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

    auto networkSource = std::make_shared<NetworkSource>(schema, bufferManager, nodeEngine->getQueryManager(),
                                                         netManager, nesPartition);
    ASSERT_TRUE(networkSource->start());

    auto networkSink = std::make_shared<NetworkSink>(schema, netManager, nodeLocation, nesPartition);

    ASSERT_TRUE(networkSource->stop());
}

TEST_F(NetworkStackTest, testNetworkSourceSink) {
    std::promise<bool> completed;
    atomic<int> bufferCnt = 0;
    atomic<int> eosCnt = 0;
    size_t totalNumBuffer = 100;

    int numSendingThreads = 10;
    auto sendingThreads = std::vector<std::thread>();
    auto schema = Schema::create()->addField(createField("id", BasicType::INT64));

    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    try {
        // create NetworkSink
        auto onError = [&completed](Messages::ErroMessage ex) {
          if (ex.getErrorType() != Messages::PartitionNotRegisteredError) {
              completed.set_exception(make_exception_ptr(runtime_error("Error")));
          }
        };
        auto onEndOfStream = [&eosCnt, &completed, &numSendingThreads](Messages::EndOfStreamMessage p) {
          eosCnt++;
          if (eosCnt == numSendingThreads) {
              completed.set_value(true);
          }
        };

        auto onBuffer = [&nesPartition, &bufferCnt](NesPartition id, TupleBuffer& buf) {
          NES_DEBUG("NetworkStackTest: Received buffer " << id.toString());
          if (nesPartition == id) {
              bufferCnt++;
          }
          ASSERT_EQ(id, nesPartition);
        };

        auto exchangeProtocol = std::make_shared<ExchangeProtocol>(bufferManager, partitionManager,
                                                                   nodeEngine->getQueryManager(), onBuffer, onEndOfStream, onError);
        NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

        std::thread receivingThread([=, &netManager, &nesPartition, &completed] {
          // register the incoming channel
          NetworkSource source{schema, bufferManager, nodeEngine->getQueryManager(),
                               netManager, nesPartition};
          ASSERT_TRUE(source.start());
          ASSERT_TRUE(this->partitionManager->isRegistered(nesPartition));
          completed.get_future().get();
          ASSERT_TRUE(source.stop());
        });

        NetworkSink networkSink{schema, netManager, nodeLocation, nesPartition};
        for (int threadNr = 0; threadNr < numSendingThreads; threadNr++) {
            std::thread sendingThread([this, &networkSink, &totalNumBuffer, threadNr] {
              // register the incoming channel
              for (size_t i = 0; i < totalNumBuffer; ++i) {
                  auto buffer = bufferManager->getBufferBlocking();
                  for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
                      buffer.getBuffer<uint64_t>()[j] = j;
                  }
                  buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
                  networkSink.writeData(buffer);
                  usleep(rand()%10000 + 1000);
              }
            });
            sendingThreads.emplace_back(std::move(sendingThread));
        }

        for (std::thread& t: sendingThreads) {
            if (t.joinable()) {
                t.join();
            }
        }
        receivingThread.join();
    }
    catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(bufferCnt, numSendingThreads * totalNumBuffer);
    ASSERT_FALSE(this->partitionManager->isRegistered(nesPartition));
}

TEST_F(NetworkStackTest, testQEPNetworkSink) {
    std::promise<bool> bufferReceived;
    std::promise<bool> completed;

    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    // create NetworkSink
    auto onError = [](Messages::ErroMessage ex) {};
    auto onEndOfStream = [&completed](Messages::EndOfStreamMessage p) {completed.set_value(true);};
    auto onBuffer = [this, &bufferReceived](NesPartition id, TupleBuffer& buf) {
      NES_DEBUG("NetworkStackTest: Received buffer \n" << UtilityFunctions::prettyPrintTupleBuffer(buf, schema));
      ASSERT_EQ(buf.getNumberOfTuples(), 10);
      uint64_t sum = 0;
      for (size_t i = 0; i < buf.getNumberOfTuples(); ++i) {
          sum += buf.getBuffer<TestStruct>()[i].value;
      }
      ASSERT_EQ(sum, 10);
      bufferReceived.set_value(true);
    };

    auto exchangeProtocol = std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                                               onBuffer, onEndOfStream, onError);
    NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

    auto networkSource = std::make_shared<NetworkSource>(schema, bufferManager, nodeEngine->getQueryManager(),
                                                         netManager, nesPartition);
    ASSERT_TRUE(networkSource->start());

    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(schema,
                                                                    nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager());
    auto networkSink = std::make_shared<NetworkSink>(schema, netManager, nodeLocation, nesPartition);

    auto source = createSourceOperator(testSource);
    auto sink = createSinkOperator(networkSink);

    source->setParent(sink);
    sink->addChild(source);

    auto compiler = createDefaultQueryCompiler(nodeEngine->getQueryManager());
    auto plan = compiler->compile(sink);
    plan->addDataSink(networkSink);
    plan->addDataSource(testSource);
    plan->setBufferManager(nodeEngine->getBufferManager());
    plan->setQueryManager(nodeEngine->getQueryManager());
    plan->setQueryId("1");

    nodeEngine->start();
    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery("1");
    ASSERT_TRUE(bufferReceived.get_future().get());
    nodeEngine->undeployQuery("1");
    nodeEngine->stop(false);
    networkSource->stop();
    ASSERT_TRUE(completed.get_future().get());
}

TEST_F(NetworkStackTest, testQEPNetworkSource) {
    std::promise<bool> completed;
    NodeLocation nodeLocation{0, "127.0.0.1", 31337};
    NesPartition nesPartition{1, 22, 33, 44};

    auto nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->createBufferManager(bufferSize, buffersManaged);
    nodeEngine->startQueryManager();
    auto bufferManager = nodeEngine->getBufferManager();
    auto partitionManager = std::make_shared<PartitionManager>();
    auto onError = [](Messages::ErroMessage ex) {};
    auto onEndOfStream = [](Messages::EndOfStreamMessage p) {};
    auto onBuffer = [this, &completed](NesPartition id, TupleBuffer& buf) {
      NES_DEBUG("NetworkStackTest: Received buffer \n" << UtilityFunctions::prettyPrintTupleBuffer(buf, schema));
      ASSERT_EQ(buf.getNumberOfTuples(), 10);
      uint64_t sum = 0;
      for (size_t i = 0; i < buf.getNumberOfTuples(); ++i) {
          sum += buf.getBuffer<TestStruct>()[i].value;
      }
      ASSERT_EQ(sum, 10);
      completed.set_value(true);
    };

    auto exchangeProtocol = std::make_shared<ExchangeProtocol>(bufferManager, partitionManager, nodeEngine->getQueryManager(),
                                                               onBuffer, onEndOfStream, onError);
    NetworkManager netManager("127.0.0.1", 31337, exchangeProtocol);

    auto compiler = createDefaultQueryCompiler(nodeEngine->getQueryManager());

    // create NetworkSink
    auto networkSource1 = std::make_shared<NetworkSource>(schema, bufferManager, nodeEngine->getQueryManager(),
                                                          netManager, nesPartition);
    auto testSink = std::make_shared<TestSink>();

    auto networkSourceOp = createSourceOperator(networkSource1);
    auto testSinkOp = createSinkOperator(testSink);
    networkSourceOp->setParent(testSinkOp);
    testSinkOp->addChild(networkSourceOp);
    auto receiveQep = compiler->compile(testSinkOp);
    receiveQep->addDataSink(testSink);
    receiveQep->addDataSource(networkSource1);
    receiveQep->setBufferManager(nodeEngine->getBufferManager());
    receiveQep->setQueryManager(nodeEngine->getQueryManager());
    receiveQep->setQueryId("1");

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(schema, nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager());
    auto networkSink = std::make_shared<NetworkSink>(schema, netManager, nodeLocation, nesPartition);

    auto source = createSourceOperator(testSource);
    auto filter = createFilterOperator(createPredicate(Field(schema->get("id")) < 5));
    auto sink = createSinkOperator(networkSink);

    filter->addChild(source);
    source->setParent(filter);
    sink->addChild(filter);
    filter->setParent(sink);

    auto generatorQep = compiler->compile(sink);
    generatorQep->addDataSink(networkSink);
    generatorQep->addDataSource(testSource);
    generatorQep->setBufferManager(nodeEngine->getBufferManager());
    generatorQep->setQueryManager(nodeEngine->getQueryManager());
    generatorQep->setQueryId("2");

    nodeEngine->start();
    nodeEngine->registerQueryInNodeEngine(generatorQep);
    nodeEngine->registerQueryInNodeEngine(receiveQep);
    nodeEngine->startQuery("1");
    nodeEngine->startQuery("2");

    ASSERT_TRUE(completed.get_future().get());
    nodeEngine->undeployQuery("1");
    nodeEngine->undeployQuery("2");
//    networkSource1->stop();
    nodeEngine->stop(true);
    ASSERT_EQ(10, testSink->completed.get_future().get());
}

} // namespace Network
} // namespace NES