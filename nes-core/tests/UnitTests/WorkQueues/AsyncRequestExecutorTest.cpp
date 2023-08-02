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
                 uint32_t responseValue)
        : AbstractRequest(requestId, requiredResources, maxRetries), responseValue(responseValue) {};

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

//    class DummyConcatResponse : public AbstractRequestResponse {
//    public:
//        explicit DummyConcatResponse(uint32_t number) : number(number){};
//        uint32_t number;
//        std::optional<
//    };
//
//    class DummyRequest : public AbstractRequest {
//    public:
//        DummyRequest(RequestId requestId,
//                     const std::vector<ResourceType>& requiredResources,
//                     uint8_t maxRetries,
//                     uint32_t responseValue)
//                : AbstractRequest(requestId, requiredResources, maxRetries), responseValue(responseValue) {};
//
//        std::vector<AbstractRequestPtr> executeRequestLogic(NES::StorageHandler&) override {
//            responsePromise.set_value(std::make_shared<DummyResponse>(responseValue));
//            return {};
//        }
//
//        std::vector<AbstractRequestPtr> rollBack(const RequestExecutionException&, StorageHandler&) override { return {}; }
//
//    protected:
//        void preRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
//        void postRollbackHandle(const RequestExecutionException&, StorageHandler&) override {}
//        void postExecution(StorageHandler&) override {}
//    private:
//        uint32_t responseValue;
//    };

class AsyncRequestExecutorTest : public Testing::TestWithErrorHandling, public testing::WithParamInterface<int> {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("AsyncRequestExecutorTest.log", NES::LogLevel::LOG_DEBUG); }
    using Base = Testing::TestWithErrorHandling;
  protected:
    Experimental::AsyncRequestExecutorPtr<TwoPhaseLockingStorageHandler> executor{nullptr};
public:
    void SetUp() override {
        Base::SetUp();
        StorageDataStructures storageDataStructures = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
        executor = std::make_shared<Experimental::AsyncRequestExecutor<TwoPhaseLockingStorageHandler>>(GetParam(), storageDataStructures);
    }

    void TearDown() override {
        executor.reset();
        Base::TearDown();
    }
};

TEST_P(AsyncRequestExecutorTest, startAndDestroy) {
    ASSERT_TRUE(executor->destroy());
}

TEST_P(AsyncRequestExecutorTest, submitRequest) {
    constexpr uint32_t responseValue = 20;
    constexpr uint32_t requestId = 1;
    try {
        auto request = std::make_shared<DummyRequest>(requestId, std::vector<ResourceType>{}, 0, responseValue);
        auto future = executor->runAsync(request);
        ASSERT_EQ(std::static_pointer_cast<DummyResponse>(future.get())->number, responseValue);
        ASSERT_TRUE(executor->destroy());
    } catch (std::exception const& ex) {
        NES_DEBUG("{}", ex.what());
        FAIL();
    }
}

INSTANTIATE_TEST_CASE_P(AsyncRequestExecutorMTTest, AsyncRequestExecutorTest, ::testing::Values(1, 4, 8));

}// namespace NES