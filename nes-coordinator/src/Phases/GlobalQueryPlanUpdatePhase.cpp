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
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Phases/SampleCodeGenerationPhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(
    const TopologyPtr& topology,
    const Catalogs::Query::QueryCatalogPtr& queryCatalog,
    const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
    const GlobalQueryPlanPtr& globalQueryPlan,
    const z3::ContextPtr& z3Context,
    const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
    const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
    const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan)
    : topology(topology), globalExecutionPlan(globalExecutionPlan), queryCatalog(queryCatalog), globalQueryPlan(globalQueryPlan),
      z3Context(z3Context) {

    auto optimizerConfigurations = coordinatorConfiguration->optimizer;
    queryMergerPhase = QueryMergerPhase::create(z3Context, optimizerConfigurations);
    typeInferencePhase = TypeInferencePhase::create(sourceCatalog, udfCatalog);
    sampleCodeGenerationPhase = SampleCodeGenerationPhase::create();
    queryRewritePhase = QueryRewritePhase::create(coordinatorConfiguration);
    originIdInferencePhase = OriginIdInferencePhase::create();
    topologySpecificQueryRewritePhase =
        TopologySpecificQueryRewritePhase::create(this->topology, sourceCatalog, optimizerConfigurations);
    signatureInferencePhase = SignatureInferencePhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
    setMemoryLayoutPhase = MemoryLayoutSelectionPhase::create(optimizerConfigurations.memoryLayoutPolicy);
}

GlobalQueryPlanUpdatePhasePtr
GlobalQueryPlanUpdatePhase::create(const TopologyPtr& topology,
                                   const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                   const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                   const GlobalQueryPlanPtr& globalQueryPlan,
                                   const z3::ContextPtr& z3Context,
                                   const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                   const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                   const GlobalExecutionPlanPtr& globalExecutionPlan) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(topology,
                                                                                   queryCatalog,
                                                                                   sourceCatalog,
                                                                                   globalQueryPlan,
                                                                                   z3Context,
                                                                                   coordinatorConfiguration,
                                                                                   udfCatalog,
                                                                                   globalExecutionPlan));
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<NESRequestPtr>& nesRequests) {
    //FIXME: Proper error handling #1585
    try {
        //TODO: Parallelize this loop #1738
        for (const auto& nesRequest : nesRequests) {
            auto requestType = nesRequest->getRequestType();
            switch (requestType) {
                case RequestType::StopQuery: {
                    processStopQueryRequest(nesRequest->as<StopQueryRequest>());
                    break;
                }
                case RequestType::FailQuery: {
                    processFailQueryRequest(nesRequest->as<FailQueryRequest>());
                    break;
                }
                case RequestType::AddQuery: {
                    processAddQueryRequest(nesRequest->as<AddQueryRequest>());
                    break;
                }
                default:
                    NES_ERROR("QueryProcessingService: Received unhandled request type  {}", nesRequest->toString());
                    NES_WARNING("QueryProcessingService: Skipping to process next request.");
            }
        }
        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with:  {}", ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

void GlobalQueryPlanUpdatePhase::processStopQueryRequest(const StopQueryRequestPtr& stopQueryRequest) {
    QueryId queryId = stopQueryRequest->getQueryId();
    NES_INFO("QueryProcessingService: Request received for stopping the query {}", queryId);
    globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
}

void GlobalQueryPlanUpdatePhase::processFailQueryRequest(const FailQueryRequestPtr& failQueryRequest) {
    SharedQueryId sharedQueryId = failQueryRequest->getSharedQueryId();
    NES_INFO("QueryProcessingService: Request received for stopping the query {}", sharedQueryId);
    globalQueryPlan->removeQuery(sharedQueryId, RequestType::FailQuery);
}

void GlobalQueryPlanUpdatePhase::processAddQueryRequest(const AddQueryRequestPtr& addQueryRequest) {
    QueryId queryId = addQueryRequest->getQueryId();
    auto runRequest = addQueryRequest->as<AddQueryRequest>();
    auto queryPlan = runRequest->getQueryPlan();

    //1. Add the initial version of the query to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);
    NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query {}", queryId);

    //2. Set query status as Optimizing
    queryCatalog->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

    //3. Execute type inference phase
    NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
    queryPlan = typeInferencePhase->execute(queryPlan);

    //4. Set memory layout of each logical operator
    NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
    queryPlan = setMemoryLayoutPhase->execute(queryPlan);

    //5. Perform query re-write
    NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query:  {}", queryId);
    queryPlan = queryRewritePhase->execute(queryPlan);

    //6. Add the updated query plan to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

    //7. Execute type inference phase on rewritten query plan
    queryPlan = typeInferencePhase->execute(queryPlan);

    //8. Generate sample code for elegant planner
    if (addQueryRequest->getQueryPlacementStrategy() == PlacementStrategy::ELEGANT_BALANCED
        || addQueryRequest->getQueryPlacementStrategy() == PlacementStrategy::ELEGANT_PERFORMANCE
        || addQueryRequest->getQueryPlacementStrategy() == PlacementStrategy::ELEGANT_ENERGY) {
        queryPlan = sampleCodeGenerationPhase->execute(queryPlan);
    }

    //9. Perform signature inference phase for sharing identification among query plans
    signatureInferencePhase->execute(queryPlan);

    //10. Perform topology specific rewrites to the query plan
    queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

    //11. Add the updated query plan to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

    //12. Perform type inference over re-written query plan
    queryPlan = typeInferencePhase->execute(queryPlan);

    //13. Identify the number of origins and their ids for all logical operators
    queryPlan = originIdInferencePhase->execute(queryPlan);

    //14. Set memory layout of each logical operator in the rewritten query
    NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
    queryPlan = setMemoryLayoutPhase->execute(queryPlan);

    //15. Add the updated query plan to the query catalog
    queryCatalog->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

    //16. Add the updated query plan to the global query plan
    NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
    globalQueryPlan->addQueryPlan(queryPlan);

    //17. Perform query merging for newly added query plan
    NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
    queryMergerPhase->execute(globalQueryPlan);
}
}// namespace NES::Optimizer
