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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/RequestTypes/AbstractMultiRequest.hpp>
#include <RequestProcessor/RequestTypes/AbstractSubRequest.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <RequestProcessor/RequestTypes/SubRequestFuture.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/TestUtils.hpp>
#include <algorithm>
#include <gtest/gtest.h>
#include <utility>

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
class DummyConcatRequest : public AbstractUniRequest {
  public:
    DummyConcatRequest(const std::vector<ResourceType>& requiredResources,
                       uint8_t maxRetries,
                       uint32_t responseValue,
                       uint32_t min)
        : AbstractUniRequest(requiredResources, maxRetries), responseValue(responseValue), min(min){};

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

    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }

  protected:
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postExecution(const StorageHandlerPtr&) override {}

  private:
    uint32_t responseValue;
    uint32_t min;
};

class DummyWaitOnFutureResponse : public AbstractRequestResponse {
  public:
    explicit DummyWaitOnFutureResponse(StorageHandlerPtr storageHandler) : storageHandler(std::move(storageHandler)) {}
    StorageHandlerPtr storageHandler;
};

class DummyWaitOnFutureRequest : public AbstractUniRequest {
  public:
    DummyWaitOnFutureRequest(const std::vector<ResourceType>& requiredResources, uint8_t maxRetries, std::future<bool> future)
        : AbstractUniRequest(requiredResources, maxRetries), future(std::move(future)){};

    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override {
        if (future.get()) {
            auto response = std::make_shared<DummyWaitOnFutureResponse>(storageHandler);

            responsePromise.set_value(response);
        }
        return {};
    }

    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }

  protected:
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postExecution(const StorageHandlerPtr& storageHandler) override { storageHandler->releaseResources(requestId); }

  private:
    std::future<bool> future;
};

class DummyWaitOnFutureSubRequest : public AbstractSubRequest {
  public:
    DummyWaitOnFutureSubRequest(const std::vector<ResourceType>& requiredResources, std::future<bool> future)
        : AbstractSubRequest(requiredResources), future(std::move(future)){};

    std::any executeSubRequestLogic(const StorageHandlerPtr& storageHandler) override {
        future.get();
        auto response = std::make_shared<DummyWaitOnFutureResponse>(storageHandler);

        return response;
    }

  private:
    std::future<bool> future;
};

class DummyWaitOnFutureMultiRequest : public AbstractMultiRequest {
  public:
    DummyWaitOnFutureMultiRequest(
        uint8_t maxRetries,
        std::vector<std::pair<std::future<bool>, std::shared_ptr<AbstractSubRequest>>>& listOfResourceLists)
        : AbstractMultiRequest(maxRetries), listOfResourceLists(listOfResourceLists){};

    std::vector<AbstractRequestPtr> executeRequestLogic() override {
        std::vector<SubRequestFuture> responseFutures;
        responseFutures.reserve(listOfResourceLists.size());
        for (auto& [future, subRequest] : listOfResourceLists) {
            future.get();
            responseFutures.push_back(scheduleSubRequest(subRequest));
        }

        std::shared_ptr<DummyWaitOnFutureResponse> response;
        for (auto& f : responseFutures) {
            response = std::any_cast<std::shared_ptr<DummyWaitOnFutureResponse>>(f.get());
        }
        responsePromise.set_value(response);
        return {};
    }

    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }

  protected:
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}

  private:
    std::vector<std::pair<std::future<bool>, std::shared_ptr<AbstractSubRequest>>>& listOfResourceLists;
};

class DummyMultiResponse : public AbstractRequestResponse {
  public:
    explicit DummyMultiResponse(uint32_t number) : number(number){};
    uint32_t number;
};

class DummySubRequest : public AbstractSubRequest {
  public:
    DummySubRequest(std::atomic<uint32_t>& additionTarget, uint32_t returnNewRequestFrequency)
        : AbstractSubRequest({}), additionTarget(additionTarget), returnNewRequestFrequency(returnNewRequestFrequency) {}
    std::any executeSubRequestLogic(const StorageHandlerPtr&) override {
        auto lastValue = ++additionTarget;
        return true;
    }
    std::atomic<uint32_t>& additionTarget;
    uint32_t returnNewRequestFrequency;
};

//todo: rename
class DummyMultiRequest : public AbstractMultiRequest {
  public:
    DummyMultiRequest(uint8_t maxRetries, uint32_t initialValue, uint32_t additionValue, uint32_t returnNewRequestFrequency)
        : AbstractMultiRequest(maxRetries), responseValue(initialValue), additionValue(additionValue),
          returnNewRequestFrequency(returnNewRequestFrequency){};

    std::vector<AbstractRequestPtr> executeRequestLogic() override {
        std::vector<SubRequestFuture> futures;
        for (uint32_t i = 0; i < additionValue; ++i) {
            futures.push_back(scheduleSubRequest(std::make_shared<DummySubRequest>(responseValue, returnNewRequestFrequency)));
        }

        for (auto& f : futures) {
            f.get();
        }
        responsePromise.set_value(std::make_shared<DummyMultiResponse>(responseValue));
        return {};
    }

    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }
    std::atomic<uint32_t> responseValue;

  protected:
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}

  private:
    uint32_t additionValue;
    uint32_t returnNewRequestFrequency;
};

class AsyncRequestProcessorTest : public Testing::BaseUnitTest, public testing::WithParamInterface<int> {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("AsyncRequestProcessorTest.log", NES::LogLevel::LOG_DEBUG); }
    using Base = Testing::BaseUnitTest;

  protected:
    Configurations::CoordinatorConfigurationPtr coordinatorConfig;

  public:
    AsyncRequestProcessorPtr getProcessor(uint16_t numThreads) {
        coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
        coordinatorConfig->requestExecutorThreads = numThreads;
        StorageDataStructures storageDataStructures =
            {coordinatorConfig, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
        return std::make_shared<AsyncRequestProcessor>(storageDataStructures);
    }
};

TEST_P(AsyncRequestProcessorTest, startAndDestroy) {
    auto processor = getProcessor(GetParam());
    EXPECT_TRUE(processor->stop());

    //it should not be possible to submit a request after destruction
    auto request = std::make_shared<DummyConcatRequest>(std::vector<ResourceType>{}, 0, 10, 10);
    EXPECT_EQ(processor->runAsync(request), INVALID_REQUEST_ID);
}

TEST_F(AsyncRequestProcessorTest, testWaitingForLock) {
    auto processor = getProcessor(8);
    try {
        //using the same value for response value and min value will create a single request with no follow up requests
        std::promise<bool> promise1;
        auto request1 = std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{ResourceType::Topology},
                                                                   0,
                                                                   promise1.get_future());

        std::promise<bool> promise2;
        auto request2 = std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{ResourceType::QueryCatalogService},
                                                                   0,
                                                                   promise2.get_future());

        std::promise<bool> promise3;
        auto request3 = std::make_shared<DummyWaitOnFutureRequest>(
            std::vector<ResourceType>{ResourceType::QueryCatalogService, ResourceType::Topology},
            0,
            promise3.get_future());

        std::promise<bool> promise4;
        auto request4 = std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{ResourceType::Topology},
                                                                   0,
                                                                   promise4.get_future());

        std::promise<bool> promise5;
        auto request5 = std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{ResourceType::QueryCatalogService},
                                                                   0,
                                                                   promise5.get_future());

        //get 2pl handler
        std::promise<bool> handlerGetterPromise;
        auto handlerGetterRequest =
            std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{}, 0, handlerGetterPromise.get_future());
        auto handlerGetterResponseFuture = handlerGetterRequest->getFuture();
        auto handlerGetterId = processor->runAsync(handlerGetterRequest);
        EXPECT_NE(handlerGetterId, INVALID_REQUEST_ID);
        handlerGetterPromise.set_value(true);
        auto twoplhandler = std::dynamic_pointer_cast<TwoPhaseLockingStorageHandler>(
            std::static_pointer_cast<DummyWaitOnFutureResponse>(handlerGetterResponseFuture.get())->storageHandler);

        auto future1 = request1->getFuture();
        auto future2 = request2->getFuture();
        auto future3 = request3->getFuture();
        auto future4 = request4->getFuture();
        auto future5 = request5->getFuture();

        auto queryId1 = processor->runAsync(request1);
        EXPECT_NE(queryId1, INVALID_REQUEST_ID);

        while (true) {
            try {
                twoplhandler->getTopologyHandle(queryId1);
                NES_DEBUG("Request {} has locked the topology", queryId1);
                break;
            } catch (std::exception& e) {
            }
        }
        auto currentTicketTopology = 0;
        auto currentTicketQueryCatalog = 0;
        auto nextAvailableTicketTopology = 1;
        auto nextAvailableTicketQueryCatalogService = 0;
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        auto queryId2 = processor->runAsync(request2);
        EXPECT_NE(queryId2, INVALID_REQUEST_ID);
        nextAvailableTicketQueryCatalogService++;
        while (true) {
            try {
                twoplhandler->getQueryCatalogHandle(queryId2);
                NES_DEBUG("Request {} has locked the query catalog", queryId2);
                break;
            } catch (std::exception& e) {
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        auto queryId3 = processor->runAsync(request3);
        EXPECT_NE(queryId3, INVALID_REQUEST_ID);
        //no additional waiting on query catalog because the request has to acquire topology before trying ot acquire the query catalog
        nextAvailableTicketTopology++;

        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (twoplhandler->getNextAvailableTicket(ResourceType::Topology) < nextAvailableTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        auto queryId4 = processor->runAsync(request4);
        EXPECT_NE(queryId4, INVALID_REQUEST_ID);
        nextAvailableTicketTopology++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::Topology) < nextAvailableTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        auto queryId5 = processor->runAsync(request5);
        EXPECT_NE(queryId5, INVALID_REQUEST_ID);
        nextAvailableTicketQueryCatalogService++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService)
               < nextAvailableTicketQueryCatalogService) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        promise1.set_value(true);
        future1.get();
        //because one lock on the topology gets released, request 3 can qcquire it and will then start waiting on the query catalog as well
        nextAvailableTicketQueryCatalogService++;
        //when request one gets executed, the current ticket for the topology will increase eventually
        currentTicketTopology++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getCurrentTicket(ResourceType::Topology) < currentTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService)
               < nextAvailableTicketQueryCatalogService) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }

        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);
        promise2.set_value(true);
        future2.get();
        //when request two gets executed, the current ticket for the query catalog will increase eventually
        currentTicketQueryCatalog++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService) < currentTicketQueryCatalog) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);
        promise3.set_value(true);
        //for threads >= 5 nothing changes, request 3 is still waiting on the query catalog

        //if there is just one thread, this will increase the ticket of the topology
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::Topology) < nextAvailableTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService)
               < nextAvailableTicketQueryCatalogService) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }

        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getCurrentTicket(ResourceType::Topology) < currentTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);
        promise4.set_value(true);
        //nothing changes, request 4 is waiting on toplogy which is still held by request 3 which is waiting on query catalog
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::Topology) < nextAvailableTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getCurrentTicket(ResourceType::Topology) < currentTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);
        promise5.set_value(true);
        future3.get();
        future4.get();
        future5.get();
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), 3);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), 3);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), 3);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService), 3);

        EXPECT_TRUE(processor->stop());
    } catch (std::exception const& ex) {
        NES_DEBUG("{}", ex.what());
        FAIL();
    }
}

TEST_F(AsyncRequestProcessorTest, testWaitingForLockMultiRequest) {
    auto processor = getProcessor(8);
    constexpr uint32_t responseValue = 20;
    try {
        //using the same value for response value and min value will create a single request with no follow up requests
        std::promise<bool> promise1;
        auto request1 = std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{ResourceType::Topology},
                                                                   0,
                                                                   promise1.get_future());

        std::promise<bool> promise2;
        auto request2 = std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{ResourceType::QueryCatalogService},
                                                                   0,
                                                                   promise2.get_future());

        std::promise<bool> promise3;
        std::promise<bool> runPromise3;
        auto request3 = std::make_shared<DummyWaitOnFutureSubRequest>(
            std::vector<ResourceType>{ResourceType::QueryCatalogService, ResourceType::Topology},
            promise3.get_future());
        std::pair pair3 = {runPromise3.get_future(), request3};

        std::promise<bool> promise4;
        std::promise<bool> runPromise4;
        auto request4 = std::make_shared<DummyWaitOnFutureSubRequest>(std::vector<ResourceType>{ResourceType::Topology},
                                                                      promise4.get_future());
        std::pair pair4 = {runPromise4.get_future(), request4};

        std::promise<bool> promise5;
        std::promise<bool> runPromise5;
        auto request5 =
            std::make_shared<DummyWaitOnFutureSubRequest>(std::vector<ResourceType>{ResourceType::QueryCatalogService},
                                                          promise5.get_future());
        std::pair pair5 = {runPromise5.get_future(), request5};

        std::vector<std::pair<std::future<bool>, std::shared_ptr<AbstractSubRequest>>> pairs;
        pairs.emplace_back(std::move(pair3));
        pairs.emplace_back(std::move(pair4));
        pairs.emplace_back(std::move(pair5));
        auto multiRequest = std::make_shared<DummyWaitOnFutureMultiRequest>(0, pairs);

        //get 2pl handler
        std::promise<bool> handlerGetterPromise;
        auto handlerGetterRequest =
            std::make_shared<DummyWaitOnFutureRequest>(std::vector<ResourceType>{}, 0, handlerGetterPromise.get_future());
        auto handlerGetterResponseFuture = handlerGetterRequest->getFuture();
        auto handlerGetterId = processor->runAsync(handlerGetterRequest);
        EXPECT_NE(handlerGetterId, INVALID_REQUEST_ID);
        handlerGetterPromise.set_value(true);
        auto twoplhandler = std::dynamic_pointer_cast<TwoPhaseLockingStorageHandler>(
            std::static_pointer_cast<DummyWaitOnFutureResponse>(handlerGetterResponseFuture.get())->storageHandler);

        auto future1 = request1->getFuture();
        auto future2 = request2->getFuture();
        auto multiRequestFuture = multiRequest->getFuture();

        auto queryId1 = processor->runAsync(request1);
        EXPECT_NE(queryId1, INVALID_REQUEST_ID);

        while (true) {
            try {
                twoplhandler->getTopologyHandle(queryId1);
                NES_DEBUG("Request {} has locked the topology", queryId1);
                break;
            } catch (std::exception& e) {
            }
        }
        auto currentTicketTopology = 0;
        auto currentTicketQueryCatalog = 0;
        auto nextAvailableTicketTopology = 1;
        auto nextAvailableTicketQueryCatalogService = 0;
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        auto queryId2 = processor->runAsync(request2);
        EXPECT_NE(queryId2, INVALID_REQUEST_ID);
        nextAvailableTicketQueryCatalogService++;
        while (true) {
            try {
                twoplhandler->getQueryCatalogHandle(queryId2);
                NES_DEBUG("Request {} has locked the query catalog", queryId2);
                break;
            } catch (std::exception& e) {
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        auto multiRequestId = processor->runAsync(multiRequest);
        runPromise3.set_value(true);

        //no additional waiting on query catalog because the request has to acquire topology before trying ot acquire the query catalog
        nextAvailableTicketTopology++;

        auto timeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);
        auto start_timestamp = std::chrono::system_clock::now();
        while (twoplhandler->getNextAvailableTicket(ResourceType::Topology) < nextAvailableTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        runPromise4.set_value(true);

        nextAvailableTicketTopology++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::Topology) < nextAvailableTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        runPromise5.set_value(true);

        nextAvailableTicketQueryCatalogService++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService)
               < nextAvailableTicketQueryCatalogService) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        promise1.set_value(true);
        future1.get();

        //because one lock on the topology gets released, request 3 can qcquire it and will then start waiting on the query catalog as well
        nextAvailableTicketQueryCatalogService++;
        //when request one gets executed, the current ticket for the topology will increase eventually
        currentTicketTopology++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getCurrentTicket(ResourceType::Topology) < currentTicketTopology) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService)
               < nextAvailableTicketQueryCatalogService) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }

        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        promise2.set_value(true);
        future2.get();

        //when request two gets executed, the current ticket for the query catalog will increase eventually
        currentTicketQueryCatalog++;
        start_timestamp = std::chrono::system_clock::now();
        while ((int) twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService) < currentTicketQueryCatalog) {
            if (std::chrono::system_clock::now() > start_timestamp + timeoutInSec) {
                FAIL();
            }
        }
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);

        promise3.set_value(true);
        //nothing changes, request 3 is still waiting on the query catalog
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);
        promise4.set_value(true);
        //nothing changes, request 4 is waiting on toplogy which is still held by request 3 which is waiting on query catalog
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), currentTicketTopology);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), currentTicketQueryCatalog);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), nextAvailableTicketTopology);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService),
                  nextAvailableTicketQueryCatalogService);
        promise5.set_value(true);
        multiRequestFuture.get();
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::Topology), 3);
        ASSERT_EQ(twoplhandler->getCurrentTicket(ResourceType::QueryCatalogService), 3);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::Topology), 3);
        ASSERT_EQ(twoplhandler->getNextAvailableTicket(ResourceType::QueryCatalogService), 3);

        EXPECT_TRUE(processor->stop());
    } catch (std::exception const& ex) {
        NES_DEBUG("{}", ex.what());
        FAIL();
    }
}

TEST_P(AsyncRequestProcessorTest, submitRequest) {
    auto processor = getProcessor(GetParam());
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
    auto processor = getProcessor(GetParam());
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

TEST_P(AsyncRequestProcessorTest, submitMultiRequest) {
    auto processor = getProcessor(GetParam());
    constexpr uint32_t iterations = 20;
    EXPECT_EQ(iterations % 2, 0);
    constexpr uint32_t additionsPerIteration = 3;
    constexpr uint32_t responseValue = iterations * additionsPerIteration;
    try {
        auto request = std::make_shared<DummyMultiRequest>(0, 0, responseValue, additionsPerIteration);
        auto future = request->getFuture();
        auto queryId = processor->runAsync(request);
        EXPECT_NE(queryId, INVALID_REQUEST_ID);
        auto responsePtr = std::static_pointer_cast<DummyMultiResponse>(future.get());
        EXPECT_EQ(responsePtr->number, responseValue);
    } catch (std::exception const& ex) {
        NES_DEBUG("{}", ex.what());
        FAIL();
    }
    EXPECT_TRUE(processor->stop());
}

INSTANTIATE_TEST_CASE_P(AsyncRequestExecutorMTTest, AsyncRequestProcessorTest, ::testing::Values(1, 4, 8));
}// namespace NES::RequestProcessor::Experimental