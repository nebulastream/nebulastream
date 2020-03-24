#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <Network/ZmqServer.hpp>
#include <Network/NetworkDispatcher.hpp>
#include <Network/OutputChannel.hpp>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {
namespace Network {
class NetworkStackTest : public testing::Test {
public:
    static void SetUpTestCase() {
        NES::setupLogging("NetworkStackTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup NetworkStackTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down NetworkStackTest class." << std::endl;
    }
};


TEST_F(NetworkStackTest, serverMustStartAndStop) {
    try {
        ZmqServer server("127.0.0.1", 31337, 4);
        server.start();
        ASSERT_EQ(server.isRunning(), true);
    } catch (...) {
        // shutdown failed
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, dispatcherMustStartAndStop) {
    try {
        NetworkDispatcher netDispatcher("127.0.0.1", 31337);
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, singleDispatcherTest) {
    try {
        NetworkDispatcher netDispatcher("127.0.0.1", 31337);

        std::thread t ([&netDispatcher] {
            netDispatcher.registerConsumer(0, 0, 0, 0, []() {

            });
        });

        auto senderChannel = netDispatcher.registerProducer(0, 0, 0, 0);

        senderChannel.sendBuffer();

    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}


}
}