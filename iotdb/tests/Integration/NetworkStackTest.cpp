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

TEST_F(NetworkStackTest, startCloseChannel) {
    try {
        NetworkDispatcher netDispatcher("127.0.0.1", 31337);

        std::thread t ([&netDispatcher] {
            std::promise<bool> completed;
            netDispatcher.registerSubpartitionConsumer(0, 0, 0, 0, [&completed]() {
                // check that buffer content is correct
                try {

                } catch (...) {
                    completed.set_exception(std::current_exception());
                }
                completed.set_value(true);
            });
            auto v = completed.get_future().get();
            ASSERT_EQ(v, true);
        });

        auto senderChannel = netDispatcher.registerSubpartitionProducer(0, 0, 0, 0);

        senderChannel.close();

        t.join();

    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}


}
}