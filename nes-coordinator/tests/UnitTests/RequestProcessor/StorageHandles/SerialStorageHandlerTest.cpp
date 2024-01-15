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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>

namespace NES::RequestProcessor::Experimental {
class SerialStorageHandlerTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SerialStorageHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SerialAccessHandle test class.");
    }
};

TEST_F(SerialStorageHandlerTest, TestResourceAccess) {
    constexpr RequestId requestId = 1;
    //create access handle
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    StorageDataStructures storageDataStructures = {coordinatorConfiguration,
                                                   topology,
                                                   globalExecutionPlan,
                                                   queryCatalogService,
                                                   globalQueryPlan,
                                                   sourceCatalog,
                                                   udfCatalog};
    auto serialAccessHandle = SerialStorageHandler::create(storageDataStructures);

    //test if we can obtain the resource we passed to the constructor
    ASSERT_EQ(globalExecutionPlan.get(), serialAccessHandle->getGlobalExecutionPlanHandle(requestId).get());
    ASSERT_EQ(topology.get(), serialAccessHandle->getTopologyHandle(requestId).get());
    ASSERT_EQ(queryCatalogService.get(), serialAccessHandle->getQueryCatalogServiceHandle(requestId).get());
    ASSERT_EQ(globalQueryPlan.get(), serialAccessHandle->getGlobalQueryPlanHandle(requestId).get());
    ASSERT_EQ(sourceCatalog.get(), serialAccessHandle->getSourceCatalogHandle(requestId).get());
    ASSERT_EQ(udfCatalog.get(), serialAccessHandle->getUDFCatalogHandle(requestId).get());
}

}// namespace NES::RequestProcessor::Experimental
