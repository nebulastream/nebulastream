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
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandle.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/TopologyNode.hpp>

namespace NES {
class TwoPhaseLockingStorageHandleTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TwoPhaseLockingStorageHandleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TwoPhaseLockingAccessHandle test class.");
    }
};

TEST_F(TwoPhaseLockingStorageHandleTest, TestResourceAccess) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                globalQueryPlan, sourceCatalog, udfCatalog);

    //test if we can obtain the resource we passed to the constructor
    ASSERT_EQ(globalExecutionPlan.get(), twoPLAccessHandle->getGlobalExecutionPlanHandle().get());
    ASSERT_EQ(topology.get(), twoPLAccessHandle->getTopologyHandle().get());
    ASSERT_EQ(queryCatalogService.get(), twoPLAccessHandle->getQueryCatalogHandle().get());
    ASSERT_EQ(globalQueryPlan.get(), twoPLAccessHandle->getGlobalQueryPlanHandle().get());
    ASSERT_EQ(sourceCatalog.get(), twoPLAccessHandle->getSourceCatalogHandle().get());
    ASSERT_EQ(udfCatalog.get(), twoPLAccessHandle->getUdfCatalogHandle().get());
}

TEST_F(TwoPhaseLockingStorageHandleTest, TestLocking) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto rootNode = TopologyNode::create(1, "127.0.0.1", 1, 0, 0, {});
    topology->setAsRoot(rootNode);
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                         globalQueryPlan, sourceCatalog, udfCatalog);

    //if a thread holds a handle to a resource, another thread should not be able to acquire a handle at the same time
    {
        //constructor acquires lock
        auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
        auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
          ASSERT_THROW((twoPLAccessHandle->getTopologyHandle()), std::exception);
        });
        thread->join();
        //destructor releases lock
    }
    auto thread = std::make_shared<std::thread>([&twoPLAccessHandle, &rootNode]() {
      ASSERT_NO_THROW((twoPLAccessHandle->getTopologyHandle()));
      ASSERT_EQ(twoPLAccessHandle->getTopologyHandle()->getRoot(), rootNode);
    });
    thread->join();
    ASSERT_EQ(twoPLAccessHandle->getTopologyHandle()->getRoot(), rootNode);
}

TEST_F(TwoPhaseLockingStorageHandleTest, TestCopyingHandle) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto rootNode = TopologyNode::create(1, "127.0.0.1", 1, 0, 0, {});
    topology->setAsRoot(rootNode);
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                   globalQueryPlan, sourceCatalog, udfCatalog);

    //if a thread holds a handle to a resource, another thread should not be able to acquire a handle at the same time
    {
        std::shared_ptr<Topology> topologyHandleCopy;
        {
            //constructor acquires lock
            auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
            auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
                ASSERT_THROW((twoPLAccessHandle->getTopologyHandle()), std::exception);
            });
            thread->join();
            topologyHandleCopy = topologyHandle;
        }
        auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
            //cannot acquire resource because copy of handle still exists
          ASSERT_THROW((twoPLAccessHandle->getTopologyHandle()), std::exception);
        });
        thread->join();
        //copy of handle goes out of scope and releases lock
    }
    auto thread = std::make_shared<std::thread>([&twoPLAccessHandle, &rootNode]() {
      ASSERT_NO_THROW((twoPLAccessHandle->getTopologyHandle()));
      ASSERT_EQ(twoPLAccessHandle->getTopologyHandle()->getRoot(), rootNode);
    });
    thread->join();
    ASSERT_EQ(twoPLAccessHandle->getTopologyHandle()->getRoot(), rootNode);
}

TEST_F(TwoPhaseLockingStorageHandleTest, TestConcurrentAccess) {
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    topology->setAsRoot(TopologyNode::create(1, "127.0.0.1", 1, 0, 0, {}));
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UdfCatalog>();
    auto twoPLAccessHandle  = TwoPhaseLockingStorageHandle::create(globalExecutionPlan, topology, queryCatalogService,
                                                                         globalQueryPlan, sourceCatalog, udfCatalog);

    //test with multiple threads, that other threads will not be able to acquire a handle while another thread holds it

    {
        //constructor acquires lock
        auto globalExecutionPlanHandle = twoPLAccessHandle->getGlobalExecutionPlanHandle();
        auto thread1 = std::make_shared<std::thread>([&twoPLAccessHandle]() {
          ASSERT_THROW((twoPLAccessHandle->getGlobalExecutionPlanHandle()), std::exception);
          auto topologyHandle = twoPLAccessHandle->getTopologyHandle();
          auto thread2 = std::make_shared<std::thread>([&twoPLAccessHandle]() {
            ASSERT_THROW((twoPLAccessHandle->getGlobalExecutionPlanHandle()), std::exception);
            ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(), std::exception);
            auto queryCatalogHandle = twoPLAccessHandle->getQueryCatalogHandle();
            auto thread3 = std::make_shared<std::thread>([&twoPLAccessHandle]() {
              ASSERT_THROW((twoPLAccessHandle->getGlobalExecutionPlanHandle()), std::exception);
              ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(), std::exception);
              ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle(), std::exception);
              auto globalQueryPlanHandle = twoPLAccessHandle->getGlobalQueryPlanHandle();
              auto thread4 = std::make_shared<std::thread>([&twoPLAccessHandle]() {
                ASSERT_THROW((twoPLAccessHandle->getGlobalExecutionPlanHandle()), std::exception);
                ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(), std::exception);
                ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle(), std::exception);
                ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(), std::exception);
                auto sourceCatalogHandle = twoPLAccessHandle->getSourceCatalogHandle();
                auto thread5 = std::make_shared<std::thread>([&twoPLAccessHandle]() {
                  ASSERT_THROW((twoPLAccessHandle->getGlobalExecutionPlanHandle()), std::exception);
                  ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(), std::exception);
                  ASSERT_THROW(twoPLAccessHandle->getQueryCatalogHandle(), std::exception);
                  ASSERT_THROW(twoPLAccessHandle->getGlobalQueryPlanHandle(), std::exception);
                  auto sourceCatalogHandle = twoPLAccessHandle->getSourceCatalogHandle();
                });
                thread5->join();
              });
              thread4->join();
            });
            thread3->join();
          });
          thread2->join();
        });
        thread1->join();
        //destructor releases lock
    }
    auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
      ASSERT_NO_THROW((twoPLAccessHandle->getTopologyHandle()));
    });
    thread->join();
}

}
