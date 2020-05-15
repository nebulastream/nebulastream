#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

#include <gtest/gtest.h>
#include <NodeEngine/BufferManager.hpp>

using namespace std;

namespace NES {
const size_t buffersManaged = 10;
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
            exchangeProtocol([](uint64_t* id, TupleBuffer buf) {}, [](NesPartition p) {}, [](Messages::ErroMessage ex) {});
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
            [](uint64_t* id, TupleBuffer buf) {},
            [](NesPartition p) {},
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
        auto onBuffer = [](uint64_t* id, TupleBuffer buf) {};
        auto onError = [&completed](Messages::ErroMessage ex) {
          completed.set_exception(make_exception_ptr(runtime_error("Error")));
        };
        auto onEndOfStream = [&completed](NesPartition p) {
          completed.set_value(true);
        };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError,
                                  bufferManager, partitionManager);

        std::thread t([&netManager, &completed] {
          // register the incoming channel
          netManager.registerSubpartitionConsumer(0, 0, 0, 0);
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, 0, 0, 0, 0, onError);

        senderChannel->close();

        t.join();
    }
    catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, testSendData) {
    try {
        // start zmqServer
        std::promise<bool> completed;
        auto onBuffer = [](uint64_t* id, TupleBuffer buf) {
          ASSERT_EQ(buf.getBufferSize(), bufferSize);
          ASSERT_EQ(id[0], 1);
          ASSERT_EQ(id[1], 22);
          ASSERT_EQ(id[2], 333);
          ASSERT_EQ(id[3], 444);
        };

        auto onError = [&completed](Messages::ErroMessage ex) {
          completed.set_exception(make_exception_ptr(runtime_error("Error")));
        };

        auto onEndOfStream = [&completed](NesPartition p) { completed.set_value(true); };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError, bufferManager,
                                  partitionManager);
        netManager.registerSubpartitionConsumer(1, 22, 333, 444);

        std::thread t([&netManager, &completed] {
          // register the incoming channel
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, 1, 22, 333, 444, onError);

        // create testbuffer
        auto buffer = bufferManager->getBufferBlocking();
        senderChannel->sendBuffer(buffer);

        senderChannel->close();
        t.join();

    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
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
    //TODO: enable asynchronous registration of OutputChannel
    try {
        // start zmqServer
        std::promise<bool> completed;
        bool serverError = false;
        bool channelError = false;

        auto onBuffer = [](uint64_t* id, TupleBuffer buf) {
        };

        auto onErrorServer = [&serverError](Messages::ErroMessage errorMsg) {
          ASSERT_EQ(errorMsg.getErrorType(), Messages::PartitionNotRegisteredError);
          serverError = true;
        };

        auto onErrorChannel = [&channelError](Messages::ErroMessage errorMsg) {
          ASSERT_EQ(errorMsg.getErrorType(), Messages::PartitionNotRegisteredError);
          channelError = true;
        };
        auto onEndOfStream = [&completed](NesPartition) { completed.set_value(true); };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream,
                                  onErrorServer, bufferManager, partitionManager);
        // register the incoming channel
        netManager.registerSubpartitionConsumer(1, 22, 333, 4444);

        std::thread t([&completed] {
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, 1, 22, 333, 4445, onErrorChannel);

        // create testbuffer
        auto buffer = bufferManager->getBufferBlocking();
        senderChannel->sendBuffer(buffer);

        senderChannel->close();
        t.join();

        ASSERT_EQ(channelError, true);
        ASSERT_EQ(serverError, true);

    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

} // namespace Network
} // namespace NES