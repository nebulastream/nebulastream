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
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ResourceLockingException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>

namespace NES::RequestProcessor::Experimental {
class TwoPhaseLockingStorageHandlerTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TwoPhaseLockingStorageHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TwoPhaseLockingAccessHandle test class.")
    }
};

TEST_F(TwoPhaseLockingStorageHandlerTest, TestResourceAccess) {
    constexpr QueryId queryId1 = 1;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    queryCatalogService,
                                                                    globalQueryPlan,
                                                                    sourceCatalog,
                                                                    udfCatalog});

    //test if we can obtain the resource we passed to the constructor
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestNoResourcesLocked) {
    constexpr QueryId queryId1 = 1;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    queryCatalogService,
                                                                    globalQueryPlan,
                                                                    sourceCatalog,
                                                                    udfCatalog});
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {}));
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {}));
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestDoubleLocking) {
    constexpr QueryId queryId1 = 1;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    queryCatalogService,
                                                                    globalQueryPlan,
                                                                    sourceCatalog,
                                                                    udfCatalog});
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {ResourceType::Topology}));
    ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId1, {}), std::exception);
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestLocking) {
    constexpr QueryId queryId1 = 1;
    constexpr QueryId queryId2 = 2;
    std::shared_ptr<std::thread> thread;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create({coordinatorConfiguration,
                                                                    topology,
                                                                    globalExecutionPlan,
                                                                    queryCatalogService,
                                                                    globalQueryPlan,
                                                                    sourceCatalog,
                                                                    udfCatalog});
    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2,
                                                            {ResourceType::Topology,
                                                             ResourceType::GlobalExecutionPlan,
                                                             ResourceType::QueryCatalogService,
                                                             ResourceType::GlobalQueryPlan,
                                                             ResourceType::SourceCatalog,
                                                             ResourceType::UdfCatalog}));
    });
    thread->join();

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {}));
    twoPLAccessHandle->releaseResources(queryId1);
    twoPLAccessHandle->releaseResources(queryId2);

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {ResourceType::Topology}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);

    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}),
                     Exceptions::ResourceLockingException);
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2,
                                                            {ResourceType::GlobalExecutionPlan,
                                                             ResourceType::QueryCatalogService,
                                                             ResourceType::GlobalQueryPlan,
                                                             ResourceType::SourceCatalog,
                                                             ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle->releaseResources(queryId1);
    twoPLAccessHandle->releaseResources(queryId2);

    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1, {ResourceType::Topology, ResourceType::GlobalExecutionPlan}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get());
    ASSERT_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);

    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalExecutionPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2,
                                                            {ResourceType::QueryCatalogService,
                                                             ResourceType::GlobalQueryPlan,
                                                             ResourceType::SourceCatalog,
                                                             ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle->releaseResources(queryId1);
    twoPLAccessHandle->releaseResources(queryId2);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(
        queryId1,
        {ResourceType::Topology, ResourceType::GlobalExecutionPlan, ResourceType::QueryCatalogService}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get());
    ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalExecutionPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::QueryCatalogService}),
                     Exceptions::ResourceLockingException);
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(
            queryId2,
            {ResourceType::GlobalQueryPlan, ResourceType::SourceCatalog, ResourceType::UdfCatalog}));
    });
    thread->join();

    twoPLAccessHandle->releaseResources(queryId1);
    twoPLAccessHandle->releaseResources(queryId2);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1,
                                                        {ResourceType::Topology,
                                                         ResourceType::GlobalExecutionPlan,
                                                         ResourceType::QueryCatalogService,
                                                         ResourceType::GlobalQueryPlan}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get());
    ASSERT_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get(), std::exception);
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalExecutionPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::QueryCatalogService}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalQueryPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::SourceCatalog, ResourceType::UdfCatalog}));
    });
    thread->join();

    //twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle->releaseResources(queryId1);
    //twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle->releaseResources(queryId2);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1,
                                                        {ResourceType::Topology,
                                                         ResourceType::GlobalExecutionPlan,
                                                         ResourceType::QueryCatalogService,
                                                         ResourceType::GlobalQueryPlan,
                                                         ResourceType::SourceCatalog}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get());
    ASSERT_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get(), std::exception);
    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalExecutionPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::QueryCatalogService}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalQueryPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::SourceCatalog}),
                     Exceptions::ResourceLockingException);
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::UdfCatalog}));
    });
    thread->join();

    //twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle->releaseResources(queryId1);
    //twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(lockManager);
    twoPLAccessHandle->releaseResources(queryId2);
    ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId1,
                                                        {ResourceType::Topology,
                                                         ResourceType::GlobalExecutionPlan,
                                                         ResourceType::QueryCatalogService,
                                                         ResourceType::GlobalQueryPlan,
                                                         ResourceType::SourceCatalog,
                                                         ResourceType::UdfCatalog}));
    ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getSourceCatalogHandle(queryId1).get());
    ASSERT_NO_THROW(twoPLAccessHandle->getUDFCatalogHandle(queryId1).get());
    thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::Topology}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalExecutionPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::QueryCatalogService}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::GlobalQueryPlan}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::SourceCatalog}),
                     Exceptions::ResourceLockingException);
        ASSERT_THROW(twoPLAccessHandle->acquireResources(queryId2, {ResourceType::UdfCatalog}),
                     Exceptions::ResourceLockingException);
        ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(queryId2, {}));
    });
    thread->join();
}

TEST_F(TwoPhaseLockingStorageHandlerTest, TestNoDeadLock) {
    size_t numThreads = 100;
    size_t lockHolder;
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto topology = Topology::create();
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
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
                                                                    queryCatalogService,
                                                                    globalQueryPlan,
                                                                    sourceCatalog,
                                                                    udfCatalog});
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (uint64_t i = 1; i < numThreads; ++i) {
        threads.emplace_back([i, &lockHolder, &resourceVector, &reverseResourceVector, twoPLAccessHandle]() {
            while (true) {//todo: insert timeout
                try {
                    if (i % 2 == 0) {
                        //ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(i, resourceVector));
                        twoPLAccessHandle->acquireResources(i, resourceVector);
                        NES_TRACE("Previous lock holder {}", lockHolder)
                        lockHolder = i;
                        NES_TRACE("Locked using resource vector in thread {}", i)
                    } else {
                        //ASSERT_NO_THROW(twoPLAccessHandle->acquireResources(i, reverseResourceVector));
                        twoPLAccessHandle->acquireResources(i, reverseResourceVector);
                        NES_TRACE("Previous lock holder {}", lockHolder)
                        lockHolder = i;
                        NES_TRACE("Locked using reverse resource vector in thread {}", i)
                    }
                } catch (Exceptions::ResourceLockingException& e) {
                    twoPLAccessHandle->releaseResources(i);
                    continue;
                }
                break;
            }
            ASSERT_NO_THROW(twoPLAccessHandle->getTopologyHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getGlobalExecutionPlanHandle(i).get());
            ASSERT_NO_THROW(twoPLAccessHandle->getQueryCatalogServiceHandle(i).get());
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
}// namespace NES::RequestProcessor::Experimental
