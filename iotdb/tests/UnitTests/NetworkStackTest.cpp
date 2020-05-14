#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <NodeEngine/BufferManager.hpp>

using namespace std;

namespace NES {
const size_t buffersManaged = 10;
const size_t bufferSize = 4*1024;

namespace Network {
class NetworkStackTest : public testing::Test {
  public:
    BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::setupLogging("NetworkStackTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup NetworkStackTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down NetworkStackTest class." << std::endl;
    }

    /* Will be called before a  test is executed. */
    void SetUp() {
        std::cout << "Setup BufferManagerTest test case." << std::endl;
        bufferManager = std::make_shared<BufferManager>(bufferSize, buffersManaged);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down BufferManagerTest test case." << std::endl;
    }
};

TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        ExchangeProtocol exchangeProtocol([](uint64_t* id, TupleBuffer buf) {}, []() {}, [](std::exception_ptr ex) {});
        ZmqServer server("127.0.0.1", 31337, 4, exchangeProtocol, bufferManager);
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
            []() {},
            [](std::exception_ptr ex) {},
            bufferManager);
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
        auto onError = [&completed](std::exception_ptr ex) {
          completed.set_exception(ex);
        };
        auto onEndOfStream = [&completed]() {
          completed.set_value(true);
        };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError, bufferManager);

        std::thread t([&netManager, &completed] {
          // register the incoming channel
          netManager.registerSubpartitionConsumer(0, 0, 0, 0);
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, 0, 0, 0, 0);

        senderChannel.close();

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

        auto onError = [&completed](std::exception_ptr ex) { completed.set_exception(ex); };
        auto onEndOfStream = [&completed]() { completed.set_value(true); };

        NetworkManager netManager("127.0.0.1", 31337, onBuffer, onEndOfStream, onError, bufferManager);

        std::thread t([&netManager, &completed] {
          // register the incoming channel
          netManager.registerSubpartitionConsumer(1, 22, 333, 444);
          auto v = completed.get_future().get();
          ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netManager.registerSubpartitionProducer(nodeLocation, 1, 22, 333, 444);

        // create testbuffer
        auto buffer = bufferManager->getBufferBlocking();
        senderChannel.sendBuffer(buffer);

        senderChannel.close();
        t.join();

    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, registerChannelWithActors) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port+10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    string query = "InputQuery::from(default_logical).print(std::cout);";

    std::string queryId = crd->addQuery(query, "BottomUp");
    sleep(2);
    crd->removeQuery(queryId);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

} // namespace Network
} // namespace NES