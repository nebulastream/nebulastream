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
#include <BaseIntegrationTest.hpp>
#include <RequestProcessor/RequestTypes/AbstractMultiRequest.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <RequestProcessor/RequestTypes/AbstractSubRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <gtest/gtest.h>

namespace NES::RequestProcessor::Experimental {

class DummyResponse : public AbstractRequestResponse {
  public:
    explicit DummyResponse(uint32_t number) : number(number){};
    uint32_t number;
};

class ThrowAwayRequest : public AbstractRequest {
  public:
    ThrowAwayRequest() : AbstractRequest({}, 0){};
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postExecution(const StorageHandlerPtr&) override {}
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr&) override { return {}; }
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }
};

class DummySubRequest : public AbstractSubRequest {
  public:
    DummySubRequest(uint32_t& additionTarget, uint32_t returnNewRequestFrequency) : additionTarget(additionTarget), returnNewRequestFrequency(returnNewRequestFrequency) {}
    std::vector<AbstractRequestPtr> execute(const StorageHandlerPtr&) override {
        auto lastValue = ++additionTarget;
        responsePromise.set_value(true);
        if (lastValue % returnNewRequestFrequency == 0) {
            auto throwAwayPtr = std::make_shared<ThrowAwayRequest>();
            return {throwAwayPtr};
        }
        return {};
    }
    uint32_t& additionTarget;
    uint32_t returnNewRequestFrequency;
};
class DummyRequest : public AbstractMultiRequest {
  public:
    DummyRequest(const std::vector<ResourceType>& requiredResources,
                 uint8_t maxRetries,
                 uint32_t initialValue,
                 uint32_t additionValue, uint32_t returnNewRequestFrequency)
        : AbstractMultiRequest(requiredResources, maxRetries), responseValue(initialValue), additionValue(additionValue), returnNewRequestFrequency(returnNewRequestFrequency) {};

    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr&) override {
        std::vector<std::future<std::any>> futures;
        for (uint32_t i = 0; i < additionValue; ++i) {
            futures.push_back(scheduleSubRequest(std::make_shared<DummySubRequest>(responseValue, returnNewRequestFrequency)));
        }
        for (auto& f : futures) {
            f.wait();
        }
        responsePromise.set_value(std::make_shared<DummyResponse>(responseValue));
        return {};
    }

    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }
    uint32_t responseValue;

  protected:
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postExecution(const StorageHandlerPtr&) override {}

  private:
    uint32_t additionValue;
    uint32_t returnNewRequestFrequency;
};

class DummyStorageHandler : public StorageHandler {
  public:
    explicit DummyStorageHandler() = default;
    GlobalExecutionPlanHandle getGlobalExecutionPlanHandle(RequestId) override { return nullptr; };

    TopologyHandle getTopologyHandle(RequestId) override { return nullptr; };

    QueryCatalogServiceHandle getQueryCatalogServiceHandle(RequestId) override { return nullptr; };

    GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId) override { return nullptr; };

    SourceCatalogHandle getSourceCatalogHandle(RequestId) override { return nullptr; };

    UDFCatalogHandle getUDFCatalogHandle(RequestId) override { return nullptr; };

    CoordinatorConfigurationHandle getCoordinatorConfiguration(RequestId) override { return nullptr; }
};

class AbstractMultiRequestTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("AbstractRequestTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(AbstractMultiRequestTest, testOneMainThreadOneExecutor) {
    constexpr uint32_t iterations = 20;
    EXPECT_EQ(iterations % 2, 0);
    constexpr uint32_t additionsPerIteration = 3;
    constexpr uint32_t responseValue = iterations * additionsPerIteration;
    RequestId requestId = 1;
    std::vector<ResourceType> requiredResources;
    uint8_t maxRetries = 1;
    DummyRequest request(requiredResources, maxRetries, 0, responseValue, additionsPerIteration);
    request.setId(requestId);
    auto future = request.getFuture();
    auto storageHandler = std::make_shared<DummyStorageHandler>();
    auto thread = std::make_shared<std::thread>([&request, &storageHandler]() {
        request.execute(storageHandler);
    });
    for (uint32_t i = 0; i < iterations; ++i) {
        NES_DEBUG("Executing subrequest: {}", i);
        request.execute(storageHandler);
        EXPECT_EQ(request.responseValue, (i + 1) * additionsPerIteration);
    }
    thread->join();
    ASSERT_TRUE(request.isDone());
    ASSERT_EQ(std::static_pointer_cast<DummyResponse>(future.get())->number, responseValue);
}

TEST_F(AbstractMultiRequestTest, testOneMainThreadTwoExecutors) {
    constexpr uint32_t iterations = 20;
    EXPECT_EQ(iterations % 2, 0);
    constexpr uint32_t additionsPerIteration = 3;
    constexpr uint32_t responseValue = iterations * additionsPerIteration;
    RequestId requestId = 1;
    std::vector<ResourceType> requiredResources;
    uint8_t maxRetries = 1;
    DummyRequest request(requiredResources, maxRetries, 0, responseValue, additionsPerIteration);
    request.setId(requestId);
    auto future = request.getFuture();
    auto storageHandler = std::make_shared<DummyStorageHandler>();
    auto thread = std::make_shared<std::thread>([&request, &storageHandler]() {
        request.execute(storageHandler);
    });
    auto thread2 = std::make_shared<std::thread>([&request, &storageHandler]() {
        for (uint32_t i = 0; i < iterations / 2; ++i) {
            NES_DEBUG("Executing subrequest: {}", i);
            request.execute(storageHandler);
        }
    });
    for (uint32_t i = 0; i < iterations / 2; ++i) {
        NES_DEBUG("Executing subrequest: {}", i);
        request.execute(storageHandler);
    }
    thread2->join();
    thread->join();
    ASSERT_TRUE(request.isDone());
    ASSERT_EQ(std::static_pointer_cast<DummyResponse>(future.get())->number, responseValue);
}
}// namespace NES::RequestProcessor::Experimental
