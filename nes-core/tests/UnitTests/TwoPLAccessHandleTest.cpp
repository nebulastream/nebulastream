#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <WorkQueues/TwoPhaseLockingStorageAccessHandle.hpp>
#include <Topology/TopologyNode.hpp>

namespace NES {
class TwoPLAccessHandleTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("2PLAccessHandleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TwoPhaseLockingAccessHandle test class.");
    }
};

TEST_F(TwoPLAccessHandleTest, TestResourceAccess) {
    auto topology = Topology::create();
    auto twoPLAccessHandle = TwoPhaseLockingStorageAccessHandle::create(topology);
    ASSERT_EQ(topology.get(), twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_EQ(topology->getRoot(), twoPLAccessHandle->getTopologyHandle()->getRoot());
}

TEST_F(TwoPLAccessHandleTest, TestLocking) {
    auto topology = Topology::create();
    auto twoPLAccessHandle = TwoPhaseLockingStorageAccessHandle::create(topology);
    {
        //constructor acquires lock
        auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
        auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
          ASSERT_THROW((twoPLAccessHandle->getTopologyHandle()), std::exception);
        });
        thread->join();
        //destructor releases lock
    }
    auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
      ASSERT_NO_THROW((twoPLAccessHandle->getTopologyHandle()));
    });
    thread->join();
}

TEST_F(TwoPLAccessHandleTest, TestConcurrentAccess) {
    size_t numThreads = 20;
    size_t acquiringMax = 10;
    size_t acquired = 0;

    auto topology = Topology::create();
    topology->setAsRoot(TopologyNode::create(1, "127.0.0.1", 1, 0, 0, {}));
    auto twoPLAccessHandle = TwoPhaseLockingStorageAccessHandle::create(topology);
    std::vector<std::thread> threadList;
    for (size_t i = 0; i < numThreads; ++i) {
        threadList.emplace_back([&acquired, &acquiringMax, &twoPLAccessHandle]() {
            while (true) {
                try {
                    (void) twoPLAccessHandle;
                    auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
                    if (acquired >= acquiringMax) {
                        break;
                    }
                    sleep(1);
                    ++acquired;
                } catch (std::exception& ex) {
                }
            }
        });
    }

    for (auto& thread : threadList) {
        thread.join();
    }

    ASSERT_EQ(acquired, acquiringMax);
}

}
