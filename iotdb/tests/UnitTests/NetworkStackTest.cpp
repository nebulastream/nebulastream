#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

#include <gtest/gtest.h>
#include <NodeEngine/BufferManager.hpp>

using namespace std;

namespace NES {
const size_t buffersManaged = 1024;
const size_t bufferSize = 4*1024;

namespace Network {
class NetworkStackTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;
    PartitionManagerPtr partitionManager;

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
        bufferManager = std::make_shared<BufferManager>(bufferSize, buffersManaged);
        partitionManager = std::make_shared<PartitionManager>();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        std::cout << "Tear down BufferManagerTest test case." << std::endl;
    }
};

TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        ExchangeProtocol
            exchangeProtocol
            ([](NesPartition id, TupleBuffer buf) {}, [](Messages::EndOfStreamMessage p) {}, [](Messages::ErroMessage ex) {});
        ZmqServer server("127.0.0.1", 31337, 4, exchangeProtocol, bufferManager, partitionManager);
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
        NetworkManager netManager(
            "127.0.0.1",
            31337,
            [](NesPartition id, TupleBuffer buf) {},
            [](Messages::EndOfStreamMessage p) {},
            [](Messages::ErroMessage ex) {},
            bufferManager, partitionManager);
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
        auto onBuffer = [](NesPartition id, TupleBuffer buf) {};
        auto onError = [&completed](Messages::ErroMessage ex) {
          completed.set_exception(make_exception_ptr(runtime_error("Error")));
        };
        auto onEndOfStream = [&completed](Messages::EndOfStreamMessage p) {
          completed.set_value(true);
        };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError,
                                  bufferManager, partitionManager);
        auto nesPartition = NesPartition(0, 0, 0, 0);

        std::thread t([&netManager, &completed, &nesPartition] {
          // register the incoming channel
          netManager.registerSubpartitionConsumer(nesPartition);
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, 1, 3);

        senderChannel->close();

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
        auto onBuffer = [&bufferReceived](NesPartition id, TupleBuffer buf) {
          bufferReceived = (buf.getBufferSize() == bufferSize)
              && (id.getQueryId(), 1) && (id.getOperatorId(), 22) && (id.getPartitionId(), 333)
              && (id.getSubpartitionId(), 444);
        };

        auto onError = [](Messages::ErroMessage ex) {
          //can be empty for this test
        };

        auto onEndOfStream = [&completedProm](Messages::EndOfStreamMessage p) { completedProm.set_value(true); };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError, bufferManager,
                                  partitionManager);

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
        senderChannel = netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, 1, 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            // create testbuffer
            auto buffer = bufferManager->getBufferBlocking();
            buffer.getBuffer<uint64_t>()[0] = 0;
            buffer.setNumberOfTuples(1);
            buffer.setTupleSizeInBytes(sizeof(uint64_t));
            senderChannel->sendBuffer(buffer);
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

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError, bufferManager,
                                  partitionManager);

        std::thread t([&netManager, &nesPartition, &completedProm] {
          // register the incoming channel
          netManager.registerSubpartitionConsumer(nesPartition);
          completedProm.get_future().get();
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, 1, 5);

        if (senderChannel == nullptr) {
            NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
            completedProm.set_value(false);
        } else {
            auto buffer = bufferManager->getBufferBlocking();
            for (size_t i = 0; i < totalNumBuffer; ++i) {
                for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
                    buffer.getBuffer<uint64_t>()[j] = j;
                }
                buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
                buffer.setTupleSizeInBytes(sizeof(uint64_t));
                senderChannel->sendBuffer(buffer);
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
    auto partition2 = NesPartition(1, 2, 3, 5);

    partitionManager->registerSubpartition(partition1);
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 0);
    partitionManager->registerSubpartition(partition1);
    ASSERT_EQ(partitionManager->getSubpartitionCounter(partition1Copy), 1);

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

        auto onBuffer = [](NesPartition id, TupleBuffer buf) {};

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

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream,
                                  onErrorServer, bufferManager, partitionManager);

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto channel = netManager.registerSubpartitionProducer(nodeLocation,
                                                               NesPartition(1, 22, 333, 4445),
                                                               onErrorChannel,
                                                               1,
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
    size_t totalNumBuffer = 100;
    size_t numSendingThreads = 10;

    // create a couple of NesPartitions
    auto sendingThreads = std::vector<std::thread>();
    auto nesPartitions = std::vector<NesPartition>();
    auto completedPromises = std::vector<std::promise<bool>>();
    auto bufferCounter = std::vector<std::size_t>();

    for (int i = 0; i < numSendingThreads; i++) {
        nesPartitions.emplace_back(i, i + 10, i + 20, i + 30);
        completedPromises.emplace_back(std::promise<bool>());
        bufferCounter.emplace_back(0);
    }

    try {
        // start zmqServer
        auto onBuffer = [&bufferCounter](NesPartition id, TupleBuffer& buffer) {
          for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
              ASSERT_EQ(buffer.getBuffer<uint64_t>()[j], j);
          }
          bufferCounter[id.getQueryId()]++;
          usleep(rand()%10000+1000);
        };

        auto onError = [](Messages::ErroMessage ex) {
          //can be empty for this test
        };

        auto onEndOfStream = [&completedPromises](Messages::EndOfStreamMessage p) {
            completedPromises[p.getNesPartition().getQueryId()].set_value(true);
        };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError, bufferManager,
                                  partitionManager);

        std::thread receivingThread([&netManager, &nesPartitions, &completedPromises] {
          // register the incoming channel
          for (NesPartition p: nesPartitions) {
              //add random latency
              sleep(rand()%3);
              netManager.registerSubpartitionConsumer(p);
          }

          for (std::promise<bool>& p: completedPromises) {
              p.get_future().get();
          }
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);

        for (int i = 0; i < numSendingThreads; i++) {
            std::thread sendingThread([&, i] {
              // register the incoming channel
              NesPartition nesPartition(i, i + 10, i + 20, i + 30);
              auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, nesPartition, onError, 5, 5);

              if (senderChannel == nullptr) {
                  NES_INFO("NetworkStackTest: Error in registering OutputChannel!");
                  completedPromises[i].set_value(false);
              } else {
                  auto buffer = bufferManager->getBufferBlocking();
                  for (size_t i = 0; i < totalNumBuffer; ++i) {
                      for (size_t j = 0; j < bufferSize/sizeof(uint64_t); ++j) {
                          buffer.getBuffer<uint64_t>()[j] = j;
                      }
                      buffer.setNumberOfTuples(bufferSize/sizeof(uint64_t));
                      buffer.setTupleSizeInBytes(sizeof(uint64_t));
                      senderChannel->sendBuffer(buffer);
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

} // namespace Network
} // namespace NES