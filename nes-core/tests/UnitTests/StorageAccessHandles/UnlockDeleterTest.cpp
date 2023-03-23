/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandle.hpp>
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
