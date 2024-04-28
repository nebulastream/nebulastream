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
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <gtest/gtest.h>

namespace NES::RequestProcessor {

class DummyResponse : public AbstractRequestResponse {
  public:
    explicit DummyResponse(uint32_t number) : number(number){};
    uint32_t number;
};

class DummyRequest : public AbstractUniRequest {
  public:
    DummyRequest(const std::vector<ResourceType>& requiredResources, uint8_t maxRetries, uint32_t responseValue)
        : AbstractUniRequest(requiredResources, maxRetries), responseValue(responseValue){};

    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr&) override {
        responsePromise.set_value(std::make_shared<DummyResponse>(responseValue));
        return {};
    }

    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr, const StorageHandlerPtr&) override { return {}; }

  protected:
    void preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&) override {}
    void postExecution(const StorageHandlerPtr&) override {}

  private:
    uint32_t responseValue;
};

class DummyStorageHandler : public StorageHandler {
  public:
    explicit DummyStorageHandler() = default;
    Optimizer::GlobalExecutionPlanPtr getGlobalExecutionPlanHandle(RequestId) override { return nullptr; };

    TopologyHandle getTopologyHandle(RequestId) override { return nullptr; };

    QueryCatalogHandle getQueryCatalogHandle(RequestId) override { return nullptr; };

    GlobalQueryPlanHandle getGlobalQueryPlanHandle(RequestId) override { return nullptr; };

    Catalogs::Source::SourceCatalogPtr getSourceCatalogHandle(RequestId) override { return nullptr; };

    Catalogs::UDF::UDFCatalogPtr getUDFCatalogHandle(RequestId) override { return nullptr; };

    Configurations::CoordinatorConfigurationPtr getCoordinatorConfiguration(RequestId) override { return nullptr; }

    Optimizer::UMPMCAmendmentQueuePtr getAmendmentQueue() override { return nullptr; }
};

class AbstractRequestTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("AbstractRequestTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(AbstractRequestTest, testPromise) {
    constexpr uint32_t responseValue = 20;
    auto requestId = RequestId(1);
    std::vector<ResourceType> requiredResources;
    uint8_t maxRetries = 1;
    DummyRequest request(requiredResources, maxRetries, responseValue);
    request.setId(requestId);
    auto future = request.getFuture();
    auto thread = std::make_shared<std::thread>([&request]() {
        std::shared_ptr<DummyStorageHandler> storageHandler;
        request.executeRequestLogic(storageHandler);
    });
    thread->join();
    EXPECT_EQ(std::static_pointer_cast<DummyResponse>(future.get())->number, responseValue);
}
}// namespace NES::RequestProcessor
