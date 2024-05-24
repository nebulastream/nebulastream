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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ResourceLockingException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/StorageResourceLocker.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>

namespace NES::RequestProcessor {
class TwoPhaseLockingStorageHandlerTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TwoPhaseLockingStorageHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TwoPhaseLockingAccessHandle test class.")
    }
};

TEST_F(TwoPhaseLockingStorageHandlerTest, TestResourceAccess) {
    constexpr RequestId queryId1 = RequestId(1);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          topology);
    auto amendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, false>>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    globalQueryPlan,
                                                                    queryCatalog,
                                                                    sourceCatalog,
                                                                    udfCatalog,
                                                                    amendmentQueue,
                                                                    statisticProbeHandler});

    //test if we can obtain the resource we passed to the constructor
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getStatisticProbeHandler(queryId1).get(), std::exception);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestNoResourcesLocked) {
    constexpr RequestId queryId1 = RequestId(1);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          topology);
    auto amendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, false>>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    globalQueryPlan,
                                                                    queryCatalog,
                                                                    sourceCatalog,
                                                                    udfCatalog,
                                                                    amendmentQueue,
                                                                    statisticProbeHandler});
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {}));
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {}));
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestDoubleLocking) {
    constexpr auto queryId1 = RequestId(1);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          topology);
    auto amendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, false>>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    globalQueryPlan,
                                                                    queryCatalog,
                                                                    sourceCatalog,
                                                                    udfCatalog,
                                                                    amendmentQueue,
                                                                    statisticProbeHandler});
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {ResourceType::Topology}));
    ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId1, {}), std::exception);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestLocking) {
    constexpr auto queryId1 = RequestId(1);
    constexpr auto queryId2 = RequestId(2);
    constexpr auto queryId3 = RequestId(3);
    constexpr auto queryId4 = RequestId(4);
    std::shared_ptr<std::thread> thread;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          topology);
    auto amendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, false>>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    globalQueryPlan,
                                                                    queryCatalog,
                                                                    sourceCatalog,
                                                                    udfCatalog,
                                                                    amendmentQueue,
                                                                    statisticProbeHandler});
    thread = std::make_shared<std::thread>([&twoPLAccessHandle, queryId2]() {
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2,
                                                            {ResourceType::Topology,
                                                             ResourceType::GlobalExecutionPlan,
                                                             ResourceType::QueryCatalogService,
                                                             ResourceType::GlobalQueryPlan,
                                                             ResourceType::SourceCatalog,
                                                             ResourceType::UdfCatalog,
                                                             ResourceType::StatisticProbeHandler}));
    });
    thread->join();

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {}));
    twoPLAccessHandle->releaseResources(queryId1);
    twoPLAccessHandle->releaseResources(queryId2);

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {ResourceType::Topology}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getStatisticProbeHandler(queryId1).get(), std::exception);

    uint32_t releaseCount = 0;
    thread = std::make_shared<std::thread>([&twoPLAccessHandle, &releaseCount, queryId2, queryId3]() {
        auto thread3 = std::make_shared<std::thread>([&twoPLAccessHandle, &releaseCount, queryId3]() {
            //wait until the other request is in waiting list for the resource
            while (twoPLAccessHandle->getCurrentTicket(ResourceType::Topology) < 1) {
                NES_DEBUG("Waiting request count {}", twoPLAccessHandle->getCurrentTicket(ResourceType::Topology))
            }
            ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId3, {ResourceType::Topology}));
            ASSERT_EQ(releaseCount, 2);
        });
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}));
        ASSERT_EQ(releaseCount, 1);
        ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId2).get());
        ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId2).get(), std::exception);
        ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle(queryId2).get(), std::exception);
        ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId2).get(), std::exception);
        ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId2).get(), std::exception);
        ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId2).get(), std::exception);
        ASSERT_THROW(twoPLAccessHandle->getStatisticProbeHandler(queryId2).get(), std::exception);
        releaseCount++;
        twoPLAccessHandle->releaseResources(queryId2);
        thread3->join();
    });

    //acquiring other resource should work without problem
    auto thread2 = std::make_shared<std::thread>([&twoPLAccessHandle, queryId4]() {
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId4,
                                                            {ResourceType::QueryCatalogService,
                                                             ResourceType::GlobalQueryPlan,
                                                             ResourceType::SourceCatalog,
                                                             ResourceType::UdfCatalog}));
        twoPLAccessHandle->releaseResources(queryId4);
    });
    thread2->join();

    //wait until the other request is in waiting list for the resource
    while (twoPLAccessHandle->getCurrentTicket(ResourceType::Topology) < 1) {
        NES_DEBUG("Waiting request count {}", twoPLAccessHandle->getCurrentTicket(ResourceType::Topology))
    }
    releaseCount++;
    twoPLAccessHandle->releaseResources(queryId1);
    thread->join();
    twoPLAccessHandle->releaseResources(queryId3);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestNoDeadLock) {
    size_t numThreads = 100;
    RequestId lockHolder = INVALID_REQUEST_ID;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(Statistic::StatisticRegistry::create(),
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          topology);
    auto amendmentQueue = std::make_shared<folly::UMPMCQueue<Optimizer::PlacementAmendmentInstancePtr, false>>();
    std::vector<ResourceType> resourceVector = {ResourceType::Topology,
                                                ResourceType::GlobalExecutionPlan,
                                                ResourceType::QueryCatalogService,
                                                ResourceType::GlobalQueryPlan,
                                                ResourceType::SourceCatalog,
                                                ResourceType::UdfCatalog};
    auto reverseResourceVector = resourceVector;
    std::reverse(reverseResourceVector.begin(), reverseResourceVector.end());
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    globalQueryPlan,
                                                                    queryCatalog,
                                                                    sourceCatalog,
                                                                    udfCatalog,
                                                                    amendmentQueue,
                                                                    statisticProbeHandler});
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (uint64_t underlying_request_id = 1; underlying_request_id < numThreads; ++underlying_request_id) {
        const auto i = RequestId(underlying_request_id);
        threads.emplace_back([i, &lockHolder, &resourceVector, &reverseResourceVector, twoPLAccessHandle]() {
            if (i.getRawValue() % 2 == 0) {
                twoPLAccessHandle->acquireResources(RequestId(i), resourceVector);
                NES_TRACE("Previous lock holder {}", lockHolder)
                lockHolder = i;
                NES_TRACE("Locked using resource vector in thread {}", i)
            } else {
                twoPLAccessHandle->acquireResources(i, reverseResourceVector);
                NES_TRACE("Previous lock holder {}", lockHolder)
                lockHolder = i;
                NES_TRACE("Locked using reverse resource vector in thread {}", i)
            }
            ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getUDFCatalogHandle(i).get());
            EXPECT_EQ(lockHolder, i);
            twoPLAccessHandle->releaseResources(i);
            NES_DEBUG("Thread {} released all resources", i);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}
}// namespace NES::RequestProcessor
