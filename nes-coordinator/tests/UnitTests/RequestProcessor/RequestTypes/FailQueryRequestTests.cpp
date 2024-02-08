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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/DecomposedQueryPlanMetaData.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/RPCQueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Identifiers.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <RequestProcessor/StorageHandles/TwoPhaseLockingStorageHandler.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace std;

namespace NES::RequestProcessor::Experimental {

using namespace Configurations;

class FailQueryRequestTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG); }
    static void TearDownTestCase() {
        auto inputSequence = std::filesystem::path(TEST_DATA_DIRECTORY) / "sequence_long.csv";
        remove(inputSequence.c_str());
    }

    void createDataStructures() {
        coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        topology = Topology::create();
        globalQueryPlan = GlobalQueryPlan::create();
        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
        std::map<const Compiler::Language, std::shared_ptr<const Compiler::LanguageCompiler>> compilerMap = {
            {Compiler::Language::CPP, Compiler::CPPCompiler::create()}};
        auto jitCompiler = std::make_shared<Compiler::JITCompiler>(compilerMap, false);
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        auto z3context = std::make_shared<z3::context>();
        const auto coordinatorConfig = CoordinatorConfiguration::createDefault();
        udfCatalog = std::shared_ptr<Catalogs::UDF::UDFCatalog>(Catalogs::UDF::UDFCatalog::create().release());
        globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                   queryCatalog,
                                                                                   sourceCatalog,
                                                                                   globalQueryPlan,
                                                                                   z3context,
                                                                                   coordinatorConfig,
                                                                                   udfCatalog,
                                                                                   globalExecutionPlan);
        syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
        sourceCatalogService = std::make_shared<SourceCatalogService>(sourceCatalog);
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        queryPlacementAmendmentPhase =
            Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    }

    void populateTopology() {
        std::string address = "localhost";
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;
        uint64_t resources = 4;
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;

        topology->registerTopologyNode(rootId, address, grpcPort, dataPort, resources, properties);
        topology->setRootTopologyNodeId(rootId);
        ++grpcPort;
        ++dataPort;
        topology->registerTopologyNode(workerId, address, grpcPort, dataPort, resources, properties);
        topology->addTopologyNodeAsChild(rootId, workerId);
    }

    void deployQuery() {
        //register source
        sourceCatalog->addLogicalSource("test", Schema::create()->addField(createField("value", BasicType::UINT64)));
        sourceCatalogService->registerPhysicalSource("physical_test", "test", workerId);

        std::string outputFilePath = getTestResourceFolder() / "failQueryRequestTest.out";
        string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
            + R"(", "CSV_FORMAT", "APPEND"));)";
        const auto placementStrategy = Optimizer::PlacementStrategy::BottomUp;
        queryId = 1;
        auto queryPlan = syntacticQueryValidation->validate(query);
        queryPlan->setQueryId(queryId);
        queryCatalog->createQueryCatalogEntry(query, queryPlan, placementStrategy, QueryState::REGISTERED);
        const auto runRequest = AddQueryRequest::create(queryPlan, placementStrategy);

        globalQueryPlanUpdatePhase->execute({runRequest});

        auto sharedQueryPlanToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
        ASSERT_EQ(sharedQueryPlanToDeploy.size(), 1);

        auto sharedQueryPlan = sharedQueryPlanToDeploy.front();

        //Fetch the shared query plan id
        sharedQueryId = sharedQueryPlan->getId();
        NES_DEBUG("Updating Query Plan with global query id : {}", sharedQueryId);

        //Check If the shared query plan is newly created
        ASSERT_EQ(SharedQueryPlanStatus::CREATED, sharedQueryPlan->getStatus());

        //Perform placement of new shared query plan
        NES_DEBUG("QueryProcessingService: Performing Operator placement for shared query plan");
        queryPlacementAmendmentPhase->execute(sharedQueryPlan);

        //skip deployment

        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
        ASSERT_FALSE(executionNodes.empty());

        //Remove the old mapping of the shared query plan
        if (SharedQueryPlanStatus::UPDATED == sharedQueryPlan->getStatus()) {
            queryCatalog->removeSharedQueryPlanMapping(sharedQueryId);
        }

        //Reset all sub query plan metadata in the catalog
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalog->resetSubQueryMetaData(queryId);
            queryCatalog->mapSharedQueryPlanId(sharedQueryId, queryId);
        }

        //Add sub query plan metadata in the catalog
        for (auto& executionNode : executionNodes) {
            auto workerId = executionNode->operator*()->getId();
            auto allDecomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            for (auto& decomposedQueryPlan : allDecomposedQueryPlans) {
                auto decomposedQueryPlanId = decomposedQueryPlan->getDecomposedQueryPlanId();
                auto decomposedQueryPlanVersion = decomposedQueryPlan->getVersion();
                queryCatalog->addDecomposedQueryMetaData(sharedQueryId,
                                                         decomposedQueryPlanId,
                                                         decomposedQueryPlanVersion,
                                                         workerId,
                                                         QueryState::RUNNING);
            }
        }

        //Mark queries as deployed
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalog->updateQueryStatus(queryId, QueryState::DEPLOYED, "");
        }

        //Mark queries as running
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalog->updateQueryStatus(queryId, QueryState::RUNNING, "");
        }

        //Update the shared query plan as deployed
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);

        ASSERT_EQ(queryCatalog->getQueryState(queryId), QueryState::RUNNING);
        ASSERT_NE(globalQueryPlan->getSharedQueryId(queryId), INVALID_SHARED_QUERY_ID);
        ASSERT_NE(sharedQueryId, INVALID_SHARED_QUERY_ID);
        ASSERT_EQ(globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::DEPLOYED);
    }

    WorkerId rootId = 1;
    WorkerId workerId = 2;
    std::shared_ptr<Catalogs::Query::QueryCatalog> queryCatalog;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    TopologyPtr topology;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    SourceCatalogServicePtr sourceCatalogService;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;
    Optimizer::GlobalQueryPlanUpdatePhasePtr globalQueryPlanUpdatePhase;
    Optimizer::QueryPlacementAmendmentPhasePtr queryPlacementAmendmentPhase;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    QueryId queryId{};
    SharedQueryId sharedQueryId{};
};

//test successful execution of fail query request for a single query until the undeployment step, which cannot be done in a unit test
TEST_F(FailQueryRequestTest, testValidFailRequestNoSubPlanSpecified) {
    constexpr RequestId requestId = 1;
    constexpr uint8_t maxRetries = 1;
    createDataStructures();
    populateTopology();

    deployQuery();

    auto workerRpcClient = WorkerRPCClient::create();
    auto failQueryRequest = Experimental::FailQueryRequest::create(queryId, INVALID_DECOMPOSED_QUERY_PLAN_ID, "", maxRetries);
    auto future = failQueryRequest->getFuture();
    failQueryRequest->setId(requestId);
    auto storageHandler = TwoPhaseLockingStorageHandler::create(
        {coordinatorConfiguration, topology, globalExecutionPlan, globalQueryPlan, queryCatalog, sourceCatalog, udfCatalog});
    auto thread = std::make_shared<std::thread>([&failQueryRequest, &storageHandler, this]() {
        try {
            failQueryRequest->execute(storageHandler);
        } catch (Exceptions::RPCQueryUndeploymentException& e) {
            EXPECT_EQ(e.getMode(), RpcClientModes::Stop);
            const auto failedCallNodeIds = e.getFailedExecutionNodeIds();
            EXPECT_EQ(failedCallNodeIds.size(), 2);
            EXPECT_NE(std::find(failedCallNodeIds.cbegin(), failedCallNodeIds.cend(), rootId), failedCallNodeIds.cend());
            EXPECT_NE(std::find(failedCallNodeIds.cbegin(), failedCallNodeIds.cend(), workerId), failedCallNodeIds.cend());

            //expect the query to be marked for failure and not failed, because the deployment did not succeed
            EXPECT_EQ(queryCatalog->getQueryState(queryId), QueryState::MARKED_FOR_FAILURE);
            EXPECT_EQ(globalQueryPlan->getSharedQueryPlan(sharedQueryId)->getStatus(), SharedQueryPlanStatus::FAILED);
        }
    });
    thread->join();
    EXPECT_EQ(std::static_pointer_cast<Experimental::FailQueryResponse>(future.get())->sharedQueryId, sharedQueryId);
}

//test error handling if a fail query request is executed but no query with the supplied id exists
TEST_F(FailQueryRequestTest, testInvalidQueryId) {
    constexpr RequestId requestId = 1;
    constexpr uint8_t maxRetries = 1;
    createDataStructures();
    populateTopology();

    deployQuery();

    const auto nonExistentId = queryId + 1;
    auto workerRpcClient = WorkerRPCClient::create();
    auto failQueryRequest =
        Experimental::FailQueryRequest::create(nonExistentId, INVALID_DECOMPOSED_QUERY_PLAN_ID, "", maxRetries);
    failQueryRequest->setId(requestId);
    auto storageHandler = TwoPhaseLockingStorageHandler::create(
        {coordinatorConfiguration, topology, globalExecutionPlan, globalQueryPlan, queryCatalog, sourceCatalog, udfCatalog});
    EXPECT_THROW(failQueryRequest->execute(storageHandler), Exceptions::QueryNotFoundException);
}

//test error handling when trying to let a query fail after it has already been set to the status STOPPED
TEST_F(FailQueryRequestTest, testWrongQueryStatus) {
    constexpr RequestId requestId = 1;
    constexpr uint8_t maxRetries = 1;
    createDataStructures();
    populateTopology();

    deployQuery();

    //set the query status to stopped, it should not be possible to fail a stopped query
    queryCatalog->updateQueryStatus(queryId, QueryState::STOPPED, "");

    auto workerRpcClient = WorkerRPCClient::create();
    auto failQueryRequest = Experimental::FailQueryRequest::create(queryId, INVALID_DECOMPOSED_QUERY_PLAN_ID, "", maxRetries);
    failQueryRequest->setId(requestId);
    auto storageHandler = TwoPhaseLockingStorageHandler::create(
        {coordinatorConfiguration, topology, globalExecutionPlan, globalQueryPlan, queryCatalog, sourceCatalog, udfCatalog});
    EXPECT_THROW(failQueryRequest->execute(storageHandler), Exceptions::InvalidQueryStateException);
}
}// namespace NES::RequestProcessor::Experimental