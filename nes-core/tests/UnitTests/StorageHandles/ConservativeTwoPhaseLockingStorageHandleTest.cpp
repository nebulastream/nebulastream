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
#include <WorkQueues/StorageHandles/ConservativeTwoPhaseLockManager.hpp>
#include <WorkQueues/StorageHandles/ConservativeTwoPhaseLockingStorageHandle.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/TopologyNode.hpp>
#include <barrier>

namespace NES {
class ConservativeTwoPhaseLockingStorageHandleTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConservativeTwoPhaseLockingStorageHandleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ConservativeTwoPhaseLockingAccessHandle test class.");
    }
};

TEST_F(ConservativeTwoPhaseLockingStorageHandleTest, TestResourceAccess) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto lockManager = std::make_shared<ConservativeTwoPhaseLockManager>();
    auto twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                              topology,
                                                                              queryCatalogService,
                                                                              globalQueryPlan,
                                                                              sourceCatalog,
                                                                              udfCatalog,
                                                                              lockManager);

    //test if we can obtain the resource we passed to the constructor
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getTopologyHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUdfCatalogHandle().get(), std::exception);
}
TEST_F(ConservativeTwoPhaseLockingStorageHandleTest, TestDoubleLocking) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto lockManager = std::make_shared<ConservativeTwoPhaseLockManager>();
    auto twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                              topology,
                                                                              queryCatalogService,
                                                                              globalQueryPlan,
                                                                              sourceCatalog,
                                                                              udfCatalog,
                                                                              lockManager);
    twoPLAccessHandle->preExecution({});
    ASSERT_THROW(twoPLAccessHandle->preExecution({}), std::exception);
}

TEST_F(ConservativeTwoPhaseLockingStorageHandleTest, TestLocking) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto lockManager = std::make_shared<ConservativeTwoPhaseLockManager>();
    auto twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                              topology,
                                                                              queryCatalogService,
                                                                              globalQueryPlan,
                                                                              sourceCatalog,
                                                                              udfCatalog,
                                                                              lockManager);
    twoPLAccessHandle->preExecution({StorageHandleResourceType::Topology});
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUdfCatalogHandle().get(), std::exception);

    twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                         topology,
                                                                         queryCatalogService,
                                                                         globalQueryPlan,
                                                                         sourceCatalog,
                                                                         udfCatalog,
                                                                         lockManager);
    twoPLAccessHandle->preExecution({StorageHandleResourceType::Topology, StorageHandleResourceType::GlobalExecutionPlan});
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUdfCatalogHandle().get(), std::exception);

    twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                         topology,
                                                                         queryCatalogService,
                                                                         globalQueryPlan,
                                                                         sourceCatalog,
                                                                         udfCatalog,
                                                                         lockManager);
    twoPLAccessHandle->preExecution({StorageHandleResourceType::Topology,
                                     StorageHandleResourceType::GlobalExecutionPlan,
                                     StorageHandleResourceType::QueryCatalogService});
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUdfCatalogHandle().get(), std::exception);

    twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                         topology,
                                                                         queryCatalogService,
                                                                         globalQueryPlan,
                                                                         sourceCatalog,
                                                                         udfCatalog,
                                                                         lockManager);
    twoPLAccessHandle->preExecution({StorageHandleResourceType::Topology,
                                     StorageHandleResourceType::GlobalExecutionPlan,
                                     StorageHandleResourceType::QueryCatalogService,
                                     StorageHandleResourceType::GlobalQueryPlan});
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle().get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUdfCatalogHandle().get(), std::exception);

    twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                         topology,
                                                                         queryCatalogService,
                                                                         globalQueryPlan,
                                                                         sourceCatalog,
                                                                         udfCatalog,
                                                                         lockManager);
    twoPLAccessHandle->preExecution({StorageHandleResourceType::Topology,
                                     StorageHandleResourceType::GlobalExecutionPlan,
                                     StorageHandleResourceType::QueryCatalogService,
                                     StorageHandleResourceType::GlobalQueryPlan,
                                     StorageHandleResourceType::SourceCatalog});
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle().get());
    ASSERT_THROW(twoPLAccessHandle->getUdfCatalogHandle().get(), std::exception);

    twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                         topology,
                                                                         queryCatalogService,
                                                                         globalQueryPlan,
                                                                         sourceCatalog,
                                                                         udfCatalog,
                                                                         lockManager);
    twoPLAccessHandle->preExecution({StorageHandleResourceType::Topology,
                                     StorageHandleResourceType::GlobalExecutionPlan,
                                     StorageHandleResourceType::QueryCatalogService,
                                     StorageHandleResourceType::GlobalQueryPlan,
                                     StorageHandleResourceType::SourceCatalog,
                                     StorageHandleResourceType::UdfCatalog});
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle().get());
    ASSERT_NO_THROW(twoPLAccessHandle->getUdfCatalogHandle().get());
}

TEST_F(ConservativeTwoPhaseLockingStorageHandleTest, TestNoDeadLock) {
    size_t numThreads = 100;
    size_t lockHolder;
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto lockManager = std::make_shared<ConservativeTwoPhaseLockManager>();
    std::vector<StorageHandleResourceType> resourceVector = {StorageHandleResourceType::Topology,
                                                             StorageHandleResourceType::GlobalExecutionPlan,
                                                             StorageHandleResourceType::QueryCatalogService,
                                                             StorageHandleResourceType::GlobalQueryPlan,
                                                             StorageHandleResourceType::SourceCatalog,
                                                             StorageHandleResourceType::UdfCatalog};
    auto reverseResourceVector = resourceVector;
    std::reverse(reverseResourceVector.begin(), reverseResourceVector.end());

    std::barrier beforeLockingBarrier(numThreads, []() {
    });

    std::vector<std::jthread> threads;
    threads.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
        auto twoPLAccessHandle = ConservativeTwoPhaseLockingStorageHandle::create(globalExecutionPlan,
                                                                                  topology,
                                                                                  queryCatalogService,
                                                                                  globalQueryPlan,
                                                                                  sourceCatalog,
                                                                                  udfCatalog,
                                                                                  lockManager);
        threads.emplace_back(
            [i, &lockHolder, &resourceVector, &reverseResourceVector, &beforeLockingBarrier, twoPLAccessHandle]() {
                beforeLockingBarrier.arrive_and_wait();
                if (i % 2 == 0) {
                    twoPLAccessHandle->preExecution(resourceVector);
                    NES_DEBUG2("Previous lockholder {}", lockHolder);
                    lockHolder = i;
                    NES_DEBUG2("Locked using resource vector in thread {}", i)
                } else {
                    twoPLAccessHandle->preExecution(reverseResourceVector);
                    NES_DEBUG2("Previous lockholder {}", lockHolder);
                    lockHolder = i;
                    NES_DEBUG2("Locked using reverse resource vector in thread {}", i)
                }
                ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle().get());
                ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
                ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle().get());
                ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle().get());
                ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle().get());
                ASSERT_NO_THROW(twoPLAccessHandle->getUdfCatalogHandle().get());
                EXPECT_EQ(lockHolder, i);
            });
    }
}
}// namespace NES
