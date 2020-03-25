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
        ExchangeProtocol exchangeProtocol([](){}, [](){}, [](std::exception_ptr ex){});
        ZmqServer server("127.0.0.1", 31337, 4, exchangeProtocol);
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
        NetworkDispatcher netDispatcher("127.0.0.1", 31337, [](){}, [](){}, [](std::exception_ptr ex){});
    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}

TEST_F(NetworkStackTest, startCloseChannel) {
    try {
        // start zmqServer
        std::promise<bool> completed;
        auto onBuffer = [](){};
        auto onError = [&completed](std::exception_ptr ex){
           completed.set_exception(ex);
        };
        auto onEndOfStream = [&completed](){
          completed.set_value(true);
        };

        NetworkDispatcher netDispatcher("127.0.0.1", 31337, onBuffer, onEndOfStream, onError);

        std::thread t ([&netDispatcher, &completed] {
            // register the incoming channel
            netDispatcher.registerSubpartitionConsumer(0, 0, 0, 0);
            auto v = completed.get_future().get();
            ASSERT_EQ(v, true);
        });

        NodeLocation nodeLocation(0, "127.0.0.1", 31337);
        auto senderChannel = netDispatcher.registerSubpartitionProducer(nodeLocation, 0, 0, 0, 0);

        senderChannel.close();

        t.join();

    } catch (...) {
        ASSERT_EQ(true, false);
    }
    ASSERT_EQ(true, true);
}


}
}