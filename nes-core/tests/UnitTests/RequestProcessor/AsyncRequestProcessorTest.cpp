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

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <BaseUnitTest.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <gtest/gtest.h>

namespace NES::RequestProcessor::Experimental {

class DummyConcatResponse : public AbstractRequestResponse {
  public:
    explicit DummyConcatResponse(uint32_t number) : number(number){};
    explicit DummyConcatResponse(uint32_t number, std::vector<std::future<AbstractRequestResponsePtr>> futures)
        : number(number), futures(std::move(futures)){};
    uint32_t number;
    std::vector<std::future<AbstractRequestResponsePtr>> futures;
};

/**
 * The dummy concat request is used for testing the creation of follow up requests. It will create a new request for
 * all v with responseValue > v > min.
 * So executing a request with the parameters responseValue = 12 and min = 10 will cause the following request to be executed:
 *
 *              --- (10,10)
 *            /
 * (12,10) ---
 *            \
 *              --- (11,10) ---
 *                             \
 *                              --- (10,10)
 */
class DummyConcatRequest : public AbstractRequest {
  public:
    DummyConcatRequest(const std::vector<ResourceType>& requiredResources,
                       uint8_t maxRetries,
                       uint32_t responseValue,
                       uint32_t min)
        : AbstractRequest(requiredResources, maxRetries), responseValue(responseValue), min(min){};

    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr&) override {
        std::vector<std::shared_ptr<AbstractRequest>> newRequests;
        auto response = std::make_shared<DummyConcatResponse>(responseValue);
        for (uint32_t i = responseValue - 1; i >= min; --i) {
            auto newRequest = std::make_shared<DummyConcatRequest>(std::vector<ResourceType>{}, 0, i, min);
            response->futures.push_back(newRequest->getFuture());
            newRequests.push_back(newRequest);
        }
        responsePromise.set_value(response);
        return newRequests;
    }

    std::vector<AbstractRequestPtr> rollBack(RequestExecutionException&, const StorageHandlerPtr&) override { return {}; }

  protected:
    void preRollbackHandle(const RequestExecutionException&, const StorageHandlerPtr&) override {}
    void postRollbackHandle(const RequestExecutionException&, const StorageHandlerPtr&) override {}
    void postExecution(const StorageHandlerPtr&) override {}

  private:
    uint32_t responseValue;
    uint32_t min;
};

class AsyncRequestProcessorTest : public Testing::BaseUnitTest, public testing::WithParamInterface<int> {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("AsyncRequestProcessorTest.log", NES::LogLevel::LOG_DEBUG); }
    using Base = Testing::BaseUnitTest;

  protected:
    AsyncRequestProcessorPtr processor{nullptr};

  public:
    void SetUp() override {
        Base::SetUp();
        auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
        coordinatorConfig->requestExecutorThreads = GetParam();
        StorageDataStructures storageDataStructures = {coordinatorConfig, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
        processor = std::make_shared<Experimental::AsyncRequestProcessor>(storageDataStructures);
    }

    void TearDown() override {
        processor.reset();
        Base::TearDown();
    }
};

TEST_P(AsyncRequestProcessorTest, startAndDestroy) {
    EXPECT_TRUE(processor->stop());

    //it should not be possible to submit a request after destruction
    auto request = std::make_shared<DummyConcatRequest>(std::vector<ResourceType>{}, 0, 10, 10);
    EXPECT_FALSE(processor->runAsync(request));
}

TEST_P(AsyncRequestProcessorTest, submitRequest) {
    constexpr uint32_t responseValue = 20;
    try {
        //using the same value for response value and min value will create a single request with no follow up requests
        auto request = std::make_shared<DummyConcatRequest>(std::vector<ResourceType>{}, 0, responseValue, responseValue);

        auto future = request->getFuture();
        auto queryId = processor->runAsync(request);
        EXPECT_NE(queryId, INVALID_REQUEST_ID);
        EXPECT_EQ(std::static_pointer_cast<DummyConcatResponse>(future.get())->number, responseValue);
        EXPECT_TRUE(processor->stop());
    } catch (std::exception const& ex) {
        NES_DEBUG("{}", ex.what());
        FAIL();
    }
}

TEST_P(AsyncRequestProcessorTest, submitFollowUpRequest) {
    constexpr uint32_t responseValue = 12;
    constexpr uint32_t min = 10;
    try {
        auto request = std::make_shared<DummyConcatRequest>(std::vector<ResourceType>{}, 0, responseValue, min);
        auto future = request->getFuture();
        auto queryId = processor->runAsync(request);
        EXPECT_NE(queryId, INVALID_REQUEST_ID);
        auto responsePtr = std::static_pointer_cast<DummyConcatResponse>(future.get());
        EXPECT_EQ(responsePtr->number, responseValue);

        //check that all follow up requests have been executed and producted the expected result
        EXPECT_EQ(responsePtr->futures.size(), 2);
        for (auto& f : responsePtr->futures) {
            auto r = std::static_pointer_cast<DummyConcatResponse>(f.get());
            if (r->number == responseValue - 1) {
                EXPECT_EQ(r->futures.size(), 1);
                auto r2 = std::static_pointer_cast<DummyConcatResponse>(r->futures.front().get());
                EXPECT_EQ(r2->number, responseValue - 2);
                EXPECT_EQ(r2->futures.size(), 0);
            } else {
                EXPECT_EQ(r->number, responseValue - 2);
                EXPECT_EQ(r->futures.size(), 0);
            }
        }
    } catch (std::exception const& ex) {
        NES_DEBUG("{}", ex.what());
        FAIL();
    }
    EXPECT_TRUE(processor->stop());
}

INSTANTIATE_TEST_CASE_P(AsyncRequestExecutorMTTest, AsyncRequestProcessorTest, ::testing::Values(1, 4, 8));

}// namespace NES::RequestProcessor