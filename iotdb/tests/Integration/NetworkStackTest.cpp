#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <Network/ZmqServer.hpp>
#include <Network/NetworkDispatcher.hpp>

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
        auto in = netDispatcher.getInputChannel(0, 0, 0, 0);
        auto out = netDispatcher.getOutputChannel(0, 0, 0, 0);


    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}


}
}