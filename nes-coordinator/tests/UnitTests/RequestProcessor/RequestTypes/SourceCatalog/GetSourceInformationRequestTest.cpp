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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/GetSourceInformationRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetAllLogicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSchemaEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/UpdateSourceCatalogRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::RequestProcessor {
class GetSourceInformationRequestTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    StorageHandlerPtr storageHandler;
    uint8_t retries = 3;
    std::string logicalSourceName1 = "logicalSource1";
    std::string physicalSourceName1 = "physicalSource1";
    std::string logicalSourceName2 = "logicalSource2";
    std::string physicalSourceName2 = "physicalSource2";
    std::string physicalSourceName3 = "physicalSource3";
    std::string field1 = "$ID1";
    std::string field2 = "$ID2";
    WorkerId workerId1 = 1;
    WorkerId workerId2 = 2;
    SchemaPtr schema1 = Schema::create()->addField(field1, BasicType::UINT64);
    SchemaPtr schema2 = Schema::create()->addField(field2, BasicType::UINT64);

    static void SetUpTestCase() { NES::Logger::setupLogging("GetSourceInformationRequestTest.log", NES::LogLevel::LOG_DEBUG); }

    void SetUp() {
        Testing::BaseUnitTest::SetUp();
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        storageHandler = TwoPhaseLockingStorageHandler::create(
            StorageDataStructures{nullptr, nullptr, nullptr, nullptr, nullptr, sourceCatalog, nullptr});
        sourceCatalog->addLogicalSource(logicalSourceName1, schema1);
        sourceCatalog->addLogicalSource(logicalSourceName2, schema2);
        std::vector<PhysicalSourceDefinition> physicalSources = {{logicalSourceName1, physicalSourceName1},
                                                                 {logicalSourceName2, physicalSourceName2}};
        AddPhysicalSourcesEventPtr addPhysicalSourcesEvent = AddPhysicalSourcesEvent::create(physicalSources, workerId1);
        auto updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(addPhysicalSourcesEvent, retries);
        auto future = updateSourceCatalogRequest->getFuture();
        updateSourceCatalogRequest->setId(1);
        updateSourceCatalogRequest->execute(storageHandler);
        auto response = std::static_pointer_cast<AddPhysicalSourcesResponse>(future.get());

        physicalSources = {{logicalSourceName2, physicalSourceName3}};
        addPhysicalSourcesEvent = AddPhysicalSourcesEvent::create(physicalSources, workerId2);
        updateSourceCatalogRequest = UpdateSourceCatalogRequest::create(addPhysicalSourcesEvent, retries);
        future = updateSourceCatalogRequest->getFuture();
        updateSourceCatalogRequest->setId(1);
        updateSourceCatalogRequest->execute(storageHandler);
        response = std::static_pointer_cast<AddPhysicalSourcesResponse>(future.get());
    }
};

TEST_F(GetSourceInformationRequestTest, GetAllLogicalSources) {
    //create request
    auto event = GetAllLogicalSourcesEvent::create();
    auto request = GetSourceInformationRequest::create(event, retries);
    auto requestId = 1;
    request->setId(requestId);
    auto future = request->getFuture();
    request->execute(storageHandler);
    auto response = std::static_pointer_cast<GetSourceJsonResponse>(future.get());
    auto json = response->getJson();
    ASSERT_EQ(json.size(), 3);
    ASSERT_EQ(json[0]["default_logical"], "id:INTEGER(32 bits) value:INTEGER(64 bits)");
    ASSERT_EQ(json[1][logicalSourceName1], schema1->toString());
    ASSERT_EQ(json[2][logicalSourceName2], schema2->toString());
    std::vector<std::pair<std::string, std::string>> expected = {{logicalSourceName1, schema1->toString()}, {logicalSourceName2, schema2->toString()}, {"default_logical", "id:INTEGER(32 bits) value:INTEGER(64 bits)"}};
    for (auto& el : json.items()) {
        bool found = false;
        for (auto& source : expected) {
            auto [name, schema] = source;
            if (el.value().contains(name)) {
                ASSERT_EQ(el.value().at(name), schema);
                found = true;
                expected.erase(std::remove(expected.begin(), expected.end(), source), expected.end());
            }
        }
        ASSERT_TRUE(found);
    }
}

TEST_F(GetSourceInformationRequestTest, GetLogicalSource) {
    //create request
    auto event = GetSchemaEvent::create(logicalSourceName1);
    auto request = GetSourceInformationRequest::create(event, retries);
    auto requestId = 1;
    request->setId(requestId);
    auto future = request->getFuture();
    request->execute(storageHandler);
    auto response = std::static_pointer_cast<GetSchemaResponse>(future.get());
    auto schema = response->getSchema();
    ASSERT_EQ(schema->toString(), schema1->toString());
}
}// namespace NES::RequestProcessor