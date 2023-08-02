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

class DummyResponse : public AbstractRequestResponse {
  public:
    explicit DummyResponse(uint32_t number) : number(number){};
    uint32_t number;
};

class DummyRequest : public AbstractRequest {
  public:
    DummyRequest(RequestId requestId,
                 const std::vector<ResourceType>& requiredResources,
                 uint8_t maxRetries,
                 std::promise<AbstractRequestResponsePtr> responsePromise, uint32_t responseValue)
        : AbstractRequest(requestId, requiredResources, maxRetries, std::move(responsePromise)), responseValue(responseValue) {};

    std::vector<AbstractRequestPtr> executeRequestLogic(NES::StorageHandler&) override {
        responsePromise.set_value(std::make_shared<DummyResponse>(responseValue));
        return {};
    }

    std::vector<AbstractRequestPtr> rollBack(const RequestExecutionException&, StorageHandler&) override { return {}; }

  protected:
    void preRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
    void postRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
    void postExecution(StorageHandler&) override {}
  private:
    uint32_t responseValue;
};

class AsyncRequestExecutorTest : public Testing::TestWithErrorHandling, public testing::WithParamInterface<int> {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG); }
    //todo: check if we need base setup
    using Base = Testing::TestWithErrorHandling;
};

TEST_F(AsyncRequestExecutorTest, startAndDestroy) {
    StorageDataStructures storageDataStructures = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
    auto executor = std::make_shared<Experimental::AsyncRequestExecutor<TwoPhaseLockingStorageHandler>>(0, storageDataStructures);
    ASSERT_TRUE(executor->destroy());
}

TEST_F(AsyncRequestExecutorTest, submitRequest) {
//    constexpr uint32_t responseValue = 20;
//    constexpr uint32_t requestId = 1;
//    try {
//        StorageDataStructures storageDataStructures = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
//        auto executor = std::make_shared<Experimental::AsyncRequestExecutor<TwoPhaseLockingStorageHandler>>(0, storageDataStructures);
//        auto request = std::make_shared<DummyRequest>();
//        std::promise<
//        auto future = executor->runAsync(requestId, {}, 0, )
//    } catch (std::exception const& ex) {
//        NES_DEBUG("{}", ex.what());
//        FAIL();
//    }
}

}// namespace NES