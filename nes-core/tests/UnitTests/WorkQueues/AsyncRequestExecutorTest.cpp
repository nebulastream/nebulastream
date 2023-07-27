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
#include <WorkQueues/AsyncRequestExecutor.hpp>
#include <WorkQueues/StorageHandles/StorageDataStructures.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES {

class AsyncRequestExecutorTest : public Testing::TestWithErrorHandling, public testing::WithParamInterface<int> {
    using Base = Testing::TestWithErrorHandling;
};

TEST_F(AsyncRequestExecutorTest, startAndDestroy) {
    StorageDataStructures storageDataStructures = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
    auto storageHandler = std::make_shared<TwoPhaseLockingStorageHandler>(storageDataStructures);
    //auto executor = std::make_shared<Experimental::AsyncRequestExecutor<TwoPhaseLockingStorageHandler>>();
    //ASSERT_TRUE(executor->destroy());
}

}// namespace NES