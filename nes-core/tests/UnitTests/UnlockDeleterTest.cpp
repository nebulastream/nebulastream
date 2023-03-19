#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <WorkQueues/TwoPhaseLockingStorageAccessHandle.hpp>
#include <Topology/TopologyNode.hpp>
#include <gtest/gtest.h>

namespace NES {
class UnlockDeleterTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("UnlockDeleterTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UnlockDeleter test class.");
    }
};

TEST_F(UnlockDeleterTest, TestLocking) {
    std::mutex mtx;
    {
        //constructor acquires lock
        std::unique_ptr<std::vector<int>, UnlockDeleter> handle({}, UnlockDeleter(mtx));
        auto thread = std::make_shared<std::thread>([&mtx]() {
          ASSERT_THROW((std::unique_ptr<std::vector<int>, UnlockDeleter>({}, UnlockDeleter(mtx, std::try_to_lock))), std::exception);
        });
        thread->join();
        //destructor releases lock
    }
    auto thread = std::make_shared<std::thread>([&mtx]() {
      ASSERT_NO_THROW((std::unique_ptr<std::vector<int>, UnlockDeleter>({}, UnlockDeleter(mtx, std::try_to_lock))));
    });
    thread->join();
}

}
