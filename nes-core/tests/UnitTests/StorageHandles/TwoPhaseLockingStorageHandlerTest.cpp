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
#include <Topology/Topology.hpp>
#include <WorkQueues/StorageHandles/LockManager.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/TopologyNode.hpp>

namespace NES {
class TwoPhaseLockingStorageHandlerTest : public Testing::TestWithErrorHandling {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TwoPhaseLockingStorageHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TwoPhaseLockingAccessHandle test class.")
    }
};

TEST_F(TwoPhaseLockingStorageHandlerTest, TestResourceAccess) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto lockManager = std::make_shared<LockManager>(globalExecutionPlan,
                                                     topology,
                                                     queryCatalogService,
                                                     globalQueryPlan,
                                                     sourceCatalog,
                                                     udfCatalog);
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);

    //test if we can obtain the resource we passed to the constructor
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getTopologyHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle().get(), std::exception);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestNoResourcesLocked) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto lockManager = std::make_shared<LockManager>(globalExecutionPlan,
                                                     topology,
                                                     queryCatalogService,
                                                     globalQueryPlan,
                                                     sourceCatalog,
                                                     udfCatalog);
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({}));
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({}));
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestDoubleLocking) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto lockManager = std::make_shared<LockManager>(globalExecutionPlan,
                                                     topology,
                                                     queryCatalogService,
                                                     globalQueryPlan,
                                                     sourceCatalog,
                                                     udfCatalog);
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({ResourceType::Topology}));
    ASSERT_THROW(twoPLAccessHandle->acquireResources({}), std::exception);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestLocking) {
    std::shared_ptr<std::thread> thread;
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto lockManager = std::make_shared<LockManager>(globalExecutionPlan,
                                                     topology,
                                                     queryCatalogService,
                                                     globalQueryPlan,
                                                     sourceCatalog,
                                                     udfCatalog);
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    auto twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources({ResourceType::Topology,
                                                              ResourceType::GlobalExecutionPlan,
                                                              ResourceType::QueryCatalogService,
                                                              ResourceType::GlobalQueryPlan,
                                                              ResourceType::SourceCatalog,
                                                              ResourceType::UdfCatalog}));
    });
    thread->join();

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({}));
    twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({ResourceType::Topology}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle().get(), std::exception);

    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources({ResourceType::GlobalExecutionPlan,
                                                              ResourceType::QueryCatalogService,
                                                              ResourceType::GlobalQueryPlan,
                                                              ResourceType::SourceCatalog,
                                                              ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(
        {ResourceType::Topology, ResourceType::GlobalExecutionPlan}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle().get(), std::exception);

    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources({ResourceType::QueryCatalogService,
                                                              ResourceType::GlobalQueryPlan,
                                                              ResourceType::SourceCatalog,
                                                              ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({ResourceType::Topology, ResourceType::GlobalExecutionPlan, ResourceType::QueryCatalogService}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle().get(), std::exception);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources({ResourceType::GlobalQueryPlan, ResourceType::SourceCatalog, ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({ResourceType::Topology,
                                                         ResourceType::GlobalExecutionPlan,
                                                         ResourceType::QueryCatalogService,
                                                         ResourceType::GlobalQueryPlan}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle().get(), std::exception);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources(
            {ResourceType::SourceCatalog, ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({ResourceType::Topology,
                                                         ResourceType::GlobalExecutionPlan,
                                                         ResourceType::QueryCatalogService,
                                                         ResourceType::GlobalQueryPlan,
                                                         ResourceType::SourceCatalog}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle().get(), std::exception);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources({ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources({ResourceType::Topology,
                                                         ResourceType::GlobalExecutionPlan,
                                                         ResourceType::QueryCatalogService,
                                                         ResourceType::GlobalQueryPlan,
                                                         ResourceType::SourceCatalog,
                                                         ResourceType::UdfCatalog}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getUDFCatalogHandle().get());
    thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
        ASSERT_NO_THROW(twoPLAccessHandle2->acquireResources({}));
    });
    thread->join();
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestNoDeadLock) {
    size_t numThreads = 100;
    size_t lockHolder;
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto lockManager = std::make_shared<LockManager>(globalExecutionPlan,
                                                     topology,
                                                     queryCatalogService,
                                                     globalQueryPlan,
                                                     sourceCatalog,
                                                     udfCatalog);
    std::vector<ResourceType> resourceVector = {ResourceType::Topology,
                                                ResourceType::GlobalExecutionPlan,
                                                ResourceType::QueryCatalogService,
                                                ResourceType::GlobalQueryPlan,
                                                ResourceType::SourceCatalog,
                                                ResourceType::UdfCatalog};
    auto reverseResourceVector = resourceVector;
    std::reverse(reverseResourceVector.begin(), reverseResourceVector.end());

    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
        auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
        threads.emplace_back([i, &lockHolder, &resourceVector, &reverseResourceVector, twoPLAccessHandle]() {
            if (i % 2 == 0) {
                ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(resourceVector));
                NES_DEBUG2("Previous lock holder {}", lockHolder)
                lockHolder = i;
                NES_DEBUG2("Locked using resource vector in thread {}", i)
            } else {
                ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(reverseResourceVector));
                NES_DEBUG2("Previous lock holder {}", lockHolder)
                lockHolder = i;
                NES_DEBUG2("Locked using reverse resource vector in thread {}", i)
            }
            ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
            ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
            ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
            ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
            ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle().get());
            ASSERT_NO_THROW(twoPLAccessHandle->getUDFCatalogHandle().get());
            EXPECT_EQ(lockHolder, i);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}
}// namespace NES
