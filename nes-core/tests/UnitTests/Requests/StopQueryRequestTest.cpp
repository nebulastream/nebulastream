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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <WorkQueues/RequestTypes/Experimental/StopQueryRequest.hpp>
#include <WorkQueues/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <gtest/gtest.h>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {
class StopQueryRequestTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StopQueryRequestTest.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup StopQueryRequestTest test class.");
    }
};
/**
 * @brief Test that the constructor of StopQueryRequest works as expected
 */
TEST_F(StopQueryRequestTest, createSimpleStopRequest) {
    constexpr QueryId queryId = 1;
    constexpr RequestId requestId = 1;
    WorkerRPCClientPtr workerRPCClient = std::make_shared<WorkerRPCClient>();
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    std::promise<StopQueryResponse> promise;
    auto stopQueryRequest = StopQueryRequestExperimental::create(requestId,
                                                                 queryId,
                                                                 0,
                                                                 workerRPCClient,
                                                                 coordinatorConfiguration,
                                                                 std::move(promise));
    EXPECT_EQ(stopQueryRequest->toString(), "StopQueryRequest { QueryId: " + std::to_string(queryId) + "}");
}
/**
 * @brief Test that the preExecution method of StopQueryRequest works as expected
 */
TEST_F(StopQueryRequestTest, testAccessToLockedResourcesDenied) {
    constexpr QueryId queryId = 1;
    constexpr RequestId requestId = 1;
    WorkerRPCClientPtr workerRPCClient = std::make_shared<WorkerRPCClient>();
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    std::promise<StopQueryResponse> promise;
    auto stopQueryRequest = StopQueryRequestExperimental::create(requestId,
                                                                 queryId,
                                                                 0,
                                                                 workerRPCClient,
                                                                 coordinatorConfiguration,
                                                                 std::move(promise));
    auto globalExecutionPlan = GlobalExecutionPlan::create();
    auto topology = Topology::create();
    auto queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
    auto queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
    auto globalQueryPlan = GlobalQueryPlan::create();
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto udfCatalog = std::make_shared<Catalogs::UDF::UDFCatalog>();
    auto twoPLAccessHandle = TwoPhaseLockingStorageHandler::create(globalExecutionPlan,
                                                                   topology,
                                                                   queryCatalogService,
                                                                   globalQueryPlan,
                                                                   sourceCatalog,
                                                                   udfCatalog);
    auto twoPLAccessHandle2 = TwoPhaseLockingStorageHandler::create(globalExecutionPlan,
                                                                    topology,
                                                                    queryCatalogService,
                                                                    globalQueryPlan,
                                                                    sourceCatalog,
                                                                    udfCatalog);
    //if thread 1 holds a handle to the topology, thread 2 should not be able to acquire a handle at the same time

    //constructor acquires lock
    {
        ASSERT_NO_THROW(stopQueryRequest->preExecution(*twoPLAccessHandle));
        auto thread = std::make_shared<std::thread>([&twoPLAccessHandle2]() {
            ASSERT_THROW(twoPLAccessHandle2->getTopologyHandle(requestId), std::exception);
        });
        //release lock
        stopQueryRequest->postExecution(*twoPLAccessHandle);
        thread->join();
    }
    //now thread 2 should be able to acquire lock on topology manager service
    auto thread = std::make_shared<std::thread>([&twoPLAccessHandle]() {
        ASSERT_THROW(twoPLAccessHandle->getTopologyHandle(requestId), std::exception);
    });
    thread->join();
}
}// namespace NES