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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <CoordinatorRPCService.pb.h>
#include <RequestProcessor/RequestTypes/SourceCatalog/GetSourceInformationRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemovePhysicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/UpdateLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/UpdateSourceCatalogRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::RequestProcessor {
class UpdateSourceCatalogRequestTest : public Testing::BaseUnitTest {
public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    StorageHandlerPtr storageHandler;
    uint8_t retries = 3;

    static void SetUpTestCase() { NES::Logger::setupLogging("UpdateSourceCatalogRequestTest.log", NES::LogLevel::LOG_DEBUG);}

    void SetUp() {
        Testing::BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        storageHandler = TwoPhaseLockingStorageHandler::create(
            StorageDataStructures{nullptr, nullptr, nullptr, nullptr, nullptr, sourceCatalog, nullptr});
    }
};
TEST_F(UpdateSourceCatalogRequestTest, AddLogicalSource) {
    std::string logicalSourceName1 = "logicalSource1";

    // create request
    SchemaPtr schema = Schema::create()->addField("$ID", BasicType::UINT64);
    auto updateLogicalSourceEvent = AddLogicalSourceEvent::create(logicalSourceName1, schema);
    RequestId requestId = 1;
    WorkerId workerId = 2;
    auto updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(updateLogicalSourceEvent, retries);
    auto future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    auto response = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_TRUE(response->success);
    //check if source exists in catalog
    auto source = sourceCatalog->getLogicalSource(logicalSourceName1);
    ASSERT_TRUE(source);
    ASSERT_TRUE(source->getSchema()->getField("ID"));
}

TEST_F(UpdateSourceCatalogRequestTest, RemoveLogicalSource) {
    std::string logicalSourceName1 = "logicalSource1";
    SchemaPtr schema = Schema::create()->addField("$ID", BasicType::UINT64);

    //add source to catalog
    sourceCatalog->addLogicalSource(logicalSourceName1, schema);
    //check if source exists in catalog
    auto source = sourceCatalog->getLogicalSource(logicalSourceName1);
    ASSERT_TRUE(source);
    ASSERT_TRUE(source->getSchema()->getField("ID"));
    ASSERT_FALSE(source->getSchema()->getField("value"));

    //create request
    RemoveLogicalSourceEventPtr removeLogicalSourceEvent = RemoveLogicalSourceEvent::create(logicalSourceName1);
    RequestId requestId = 1;
    WorkerId workerId = 2;
    auto updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removeLogicalSourceEvent, retries);
    auto future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    auto response = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_TRUE(response->success);

    // check if source was removed
    source = sourceCatalog->getLogicalSource(logicalSourceName1);
    ASSERT_FALSE(source);

    // removing the same source again should fail
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removeLogicalSourceEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    response = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_FALSE(response->success);
}

TEST_F(UpdateSourceCatalogRequestTest, UpdateLogicalSource) {
    std::string logicalSourceName1 = "logicalSource1";
    SchemaPtr schema = Schema::create()->addField("$ID", BasicType::UINT64);

    //add source to catalog
    sourceCatalog->addLogicalSource(logicalSourceName1, schema);
    //check if source exists in catalog
    auto source = sourceCatalog->getLogicalSource(logicalSourceName1);
    ASSERT_TRUE(source);
    ASSERT_TRUE(source->getSchema()->getField("ID"));
    ASSERT_FALSE(source->getSchema()->getField("value"));

    // create request
    schema = schema->addField("$value", BasicType::UINT64);
    UpdateLogicalSourceEventPtr updateLogicalSourceEvent = UpdateLogicalSourceEvent::create(logicalSourceName1, schema);
    RequestId requestId = 1;
    WorkerId workerId = 2;
    auto updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(updateLogicalSourceEvent, retries);
    auto future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    auto response = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_TRUE(response->success);

    //check if source was updated
    source = sourceCatalog->getLogicalSource(logicalSourceName1);
    ASSERT_TRUE(source);
    ASSERT_TRUE(source->getSchema()->getField("ID"));
    ASSERT_TRUE(source->getSchema()->getField("value"));
}

TEST_F(UpdateSourceCatalogRequestTest, AddPhysicalSources) {
    std::string logicalSourceName1 = "logicalSource1";
    std::string physicalSourceName1 = "physicalSource1";
    std::string logicalSourceName2 = "logicalSource2";
    std::string physicalSourceName2 = "physicalSource2";
    RequestId requestId = 1;

    sourceCatalog->addLogicalSource(logicalSourceName1, Schema::create()->addField("ID", BasicType::UINT64));
    std::vector<PhysicalSourceDefinition> physicalSources = {{logicalSourceName1, physicalSourceName1}};
    WorkerId workerId = 2;
    AddPhysicalSourcesEventPtr addPhysicalSourcesEvent = AddPhysicalSourcesEvent::create(physicalSources, workerId);
    ASSERT_EQ(addPhysicalSourcesEvent->getPhysicalSources(), physicalSources);
    ASSERT_EQ(addPhysicalSourcesEvent->getWorkerId(), workerId);

    //create and execute the request
    auto updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(addPhysicalSourcesEvent, retries);
    auto future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    auto response = std::static_pointer_cast<AddPhysicalSourcesResponse>(future.get());
    ASSERT_TRUE(response->success);
    ASSERT_FALSE(response->getFailedAddition());
    ASSERT_EQ(response->getSuccesfulAdditions(), std::vector{physicalSourceName1});

    //adding the same source again should fail
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(addPhysicalSourcesEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    response = std::static_pointer_cast<AddPhysicalSourcesResponse>(future.get());
    ASSERT_FALSE(response->success);
    ASSERT_TRUE(response->getFailedAddition());
    ASSERT_EQ(response->getFailedAddition().value(), physicalSourceName1);
    ASSERT_EQ(response->getSuccesfulAdditions(), std::vector<std::string>{});

    //adding a source with a non-existing logical source should fail
    physicalSources = {{logicalSourceName2, physicalSourceName2}};
    addPhysicalSourcesEvent = AddPhysicalSourcesEvent::create(physicalSources, workerId);
    //create and execute the request
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(addPhysicalSourcesEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    response = std::static_pointer_cast<AddPhysicalSourcesResponse>(future.get());
    ASSERT_FALSE(response->success);
    ASSERT_TRUE(response->getFailedAddition());
    ASSERT_EQ(response->getFailedAddition().value(), physicalSourceName2);
    ASSERT_EQ(response->getSuccesfulAdditions(), std::vector<std::string>{});
}

TEST_F(UpdateSourceCatalogRequestTest, RemovePhysicalSource) {
    std::string logicalSourceName1 = "logicalSource1";
    std::string physicalSourceName1 = "physicalSource1";
    RequestId requestId = 1;

    sourceCatalog->addLogicalSource(logicalSourceName1, Schema::create()->addField("ID", BasicType::UINT64));
    //add source
    std::vector<PhysicalSourceDefinition> physicalSources = {{logicalSourceName1, physicalSourceName1}};
    WorkerId workerId = 2;
    AddPhysicalSourcesEventPtr addPhysicalSourcesEvent = AddPhysicalSourcesEvent::create(physicalSources, workerId);
    auto updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(addPhysicalSourcesEvent, retries);
    auto future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    auto response = std::static_pointer_cast<AddPhysicalSourcesResponse>(future.get());
    ASSERT_TRUE(response->success);
    ASSERT_FALSE(response->getFailedAddition());
    ASSERT_EQ(response->getSuccesfulAdditions(), std::vector{physicalSourceName1});

    //create removal request with wrong worker id
    auto removePhysicalSourceEvent = RemovePhysicalSourceEvent::create(logicalSourceName1, physicalSourceName1, 8);
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removePhysicalSourceEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    auto removalResponse = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_FALSE(removalResponse->success);

    //create removal request with wrong logical source name
    removePhysicalSourceEvent = RemovePhysicalSourceEvent::create("abc", physicalSourceName1, workerId);
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removePhysicalSourceEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    removalResponse = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_FALSE(removalResponse->success);

    //create removal request with wrong physical source name
    removePhysicalSourceEvent = RemovePhysicalSourceEvent::create(logicalSourceName1, "abc", workerId);
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removePhysicalSourceEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    removalResponse = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_FALSE(removalResponse->success);

    //create correct  removal request
    removePhysicalSourceEvent = RemovePhysicalSourceEvent::create(logicalSourceName1, physicalSourceName1, workerId);
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removePhysicalSourceEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    removalResponse = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_TRUE(removalResponse->success);

    //removeing the same source again should fail
    //create removal request
    removePhysicalSourceEvent = RemovePhysicalSourceEvent::create(logicalSourceName1, physicalSourceName1, workerId);
    updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(removePhysicalSourceEvent, retries);
    future = updateSourceCatalogRequest->getFuture();
    updateSourceCatalogRequest->setId(requestId);
    updateSourceCatalogRequest->execute(storageHandler);
    removalResponse = std::static_pointer_cast<SourceCatalogResponse>(future.get());
    ASSERT_FALSE(removalResponse->success);
}
}// namespace NES::RequestProcessor
