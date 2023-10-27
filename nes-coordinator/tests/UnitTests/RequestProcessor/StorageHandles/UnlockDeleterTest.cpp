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
#include <BaseUnitTest.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <RequestProcessor/StorageHandles/UnlockDeleter.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::RequestProcessor::Experimental {
class UnlockDeleterTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("UnlockDeleterTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UnlockDeleter test class.");
    }
};

TEST_F(UnlockDeleterTest, TestNoLocking) {
    UnlockDeleter unlockDeleter;
    EXPECT_NO_THROW(unlockDeleter = UnlockDeleter());
}

TEST_F(UnlockDeleterTest, TestBlocking) {
    std::mutex mtx;
    std::shared_ptr<std::thread> thread;

    //check that mutex gets locked by unlock deleter
    {
        //constructor acquires lock
        std::unique_ptr<std::vector<int>, UnlockDeleter> handle({}, UnlockDeleter(mtx));
        thread = std::make_shared<std::thread>([&mtx]() {
            ASSERT_FALSE((mtx.try_lock()));
        });
        thread->join();
        //destructor releases lock
    }

    //check that mutex gets unlocked on deletion of unlock deleter
    ASSERT_TRUE(mtx.try_lock());
    mtx.unlock();

    //check that thread can acquire lock after blocking wait
    {
        //constructor acquires lock
        std::unique_ptr<std::vector<int>, UnlockDeleter> handle({}, UnlockDeleter(mtx));
        thread = std::make_shared<std::thread>([&mtx]() {
            ASSERT_NO_THROW((std::unique_ptr<std::vector<int>, UnlockDeleter>({}, UnlockDeleter(mtx))));
        });
        //destructor releases lock
    }
    thread->join();
}

TEST_F(UnlockDeleterTest, TestLocking) {
    std::mutex mtx;

    //check that try lock will throw an exception if mutex is already locked
    {
        //constructor acquires lock
        std::unique_ptr<std::vector<int>, UnlockDeleter> handle({}, UnlockDeleter(mtx));
        auto thread = std::make_shared<std::thread>([&mtx]() {
            ASSERT_THROW((std::unique_ptr<std::vector<int>, UnlockDeleter>({}, UnlockDeleter(mtx, std::try_to_lock))),
                         std::exception);
        });
        thread->join();
        //destructor releases lock
    }

    //check that try lock will not throw an exception if mutex is not locked
    auto thread = std::make_shared<std::thread>([&mtx]() {
        ASSERT_NO_THROW((std::unique_ptr<std::vector<int>, UnlockDeleter>({}, UnlockDeleter(mtx, std::try_to_lock))));
    });
    thread->join();
}

TEST_F(UnlockDeleterTest, TestTakingOwnershipOfLock) {
    std::mutex mtx;
    auto lock = std::unique_lock(mtx);
    UnlockDeleter deleter;
    ASSERT_NO_THROW(deleter = UnlockDeleter(std::move(lock)));
    ASSERT_FALSE(mtx.try_lock());
}

TEST_F(UnlockDeleterTest, TestTakingOwnershipNotLocked) {
    std::mutex mtx;
    auto lock = std::unique_lock(mtx, std::defer_lock);
    UnlockDeleter deleter;
    ASSERT_THROW(deleter = UnlockDeleter(std::move(lock)), std::exception);
}
}// namespace NES::RequestProcessor::Experimental
