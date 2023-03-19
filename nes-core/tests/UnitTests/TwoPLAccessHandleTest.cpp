#include <NesBaseTest.hpp>
#include <Topology/Topology.hpp>
#include <WorkQueues/TwoPhaseLockingStorageAccessHandle.hpp>

#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>

namespace NES {
class TwoPLAccessHandleTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("2PLAccessHandleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TwoPhaseLockingAccessHandle test class.");
    }
};

TEST_F(TwoPLAccessHandleTest, TestResourceAccess) {
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    Catalogs::Query::QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageAccessHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                globalQueryPlan, sourceCatalog, udfCatalog);
    ASSERT_EQ(topology.get(), twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_EQ(topology->getRoot(), twoPLAccessHandle->getTopologyHandle()->getRoot());
}

TEST_F(TwoPLAccessHandleTest, TestLocking) {
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    Catalogs::Query::QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageAccessHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                         globalQueryPlan, sourceCatalog, udfCatalog);
    {
        //constructor acquires lock
        auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
        auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
          ASSERT_THROW((twoPLAccessHandle->getTopologyHandle()), std::exception);
        });
        thread->join();
        //destructor releases lock
    }
    auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
      ASSERT_NO_THROW((twoPLAccessHandle->getTopologyHandle()));
    });
    thread->join();
}

TEST_F(TwoPLAccessHandleTest, TestConcurrentAccess) {
    size_t numThreads = 20;
    size_t acquiringMax = 10;
    size_t acquired = 0;

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    topology->setAsRoot(TopologyNode::create(1, "127.0.0.1", 1, 0, 0, {}));
    Catalogs::Query::QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageAccessHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                         globalQueryPlan, sourceCatalog, udfCatalog);
    std::vector<std::thread> threadList;
    for (size_t i = 0; i < numThreads; ++i) {
        threadList.emplace_back([&acquired, &acquiringMax, &twoPLAccessHandle]() {
            while (true) {
                try {
                    (void) twoPLAccessHandle;
                    auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
                    if (acquired >= acquiringMax) {
                        break;
                    }
                    sleep(1);
                    ++acquired;
                } catch (std::exception& ex) {
                }
            }
        });
    }

    for (auto& thread : threadList) {
        thread.join();
    }

    ASSERT_EQ(acquired, acquiringMax);
}

}
