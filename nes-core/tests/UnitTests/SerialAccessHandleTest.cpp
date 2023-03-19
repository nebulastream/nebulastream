#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <WorkQueues/SerialStorageAccessHandle.hpp>

#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>

namespace NES {
using SerialAccessHandlePtr = std::shared_ptr<SerialStorageAccessHandle>;
class SerialAccessHandleTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SerialAccessHandleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SerialAccessHandle test class.");
    }
};

TEST_F(SerialAccessHandleTest, TestResourceAccess) {
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    Catalogs::Query::QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto serialAccessHandle = SerialStorageAccessHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                globalQueryPlan, sourceCatalog, udfCatalog);
    ASSERT_EQ(topology.get(), serialAccessHandle->getTopologyHandle().get());
    ASSERT_EQ(topology->getRoot(), serialAccessHandle->getTopologyHandle()->getRoot());
}

}
