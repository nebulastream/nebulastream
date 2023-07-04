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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SampleCodeGenerationPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <WorkQueues/RequestTypes/TopologyRequests/MigrateTopologyNodeRequest.hpp>
#include <WorkQueues/RequestTypes/TopologyRequests/RemoveTopologyLinkRequest.hpp>
#include <WorkQueues/RequestTypes/TopologyRequests/RemoveTopologyNodeRequest.hpp>
#include <utility>

namespace NES::Optimizer {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(
    TopologyPtr topology,
    QueryCatalogServicePtr queryCatalogService,
    Catalogs::Source::SourceCatalogPtr sourceCatalog,
    GlobalQueryPlanPtr globalQueryPlan,
    z3::ContextPtr z3Context,
    const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
    Catalogs::UDF::UDFCatalogPtr udfCatalog,
    GlobalExecutionPlanPtr globalExecutionPlan)
    : topology(std::move(topology)), globalExecutionPlan(std::move(globalExecutionPlan)),
      queryCatalogService(std::move(queryCatalogService)), globalQueryPlan(std::move(globalQueryPlan)),
      z3Context(std::move(z3Context)) {

    auto optimizerConfigurations = coordinatorConfiguration->optimizer;
    queryMergerPhase = QueryMergerPhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
    typeInferencePhase = TypeInferencePhase::create(sourceCatalog, std::move(udfCatalog));
    sampleCodeGenerationPhase = SampleCodeGenerationPhase::create();
    queryRewritePhase = QueryRewritePhase::create(coordinatorConfiguration);
    originIdInferencePhase = OriginIdInferencePhase::create();
    topologySpecificQueryRewritePhase =
        TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfigurations);
    signatureInferencePhase = SignatureInferencePhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
    setMemoryLayoutPhase = MemoryLayoutSelectionPhase::create(optimizerConfigurations.memoryLayoutPolicy);
}

GlobalQueryPlanUpdatePhasePtr
GlobalQueryPlanUpdatePhase::create(TopologyPtr topology,
                                   QueryCatalogServicePtr queryCatalogService,
                                   Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                   GlobalQueryPlanPtr globalQueryPlan,
                                   z3::ContextPtr z3Context,
                                   const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                   Catalogs::UDF::UDFCatalogPtr udfCatalog,
                                   GlobalExecutionPlanPtr globalExecutionPlan) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(std::move(topology),
                                                                                   std::move(queryCatalogService),
                                                                                   std::move(sourceCatalog),
                                                                                   std::move(globalQueryPlan),
                                                                                   std::move(z3Context),
                                                                                   coordinatorConfiguration,
                                                                                   std::move(udfCatalog),
                                                                                   std::move(globalExecutionPlan)));
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<NESRequestPtr>& nesRequests) {
    //FIXME: Proper error handling #1585
    try {
        //TODO: Parallelize this loop #1738
        for (const auto& nesRequest : nesRequests) {

            auto requestType = nesRequest->getRequestType();

            switch (requestType) {
                case RequestType::StopQuery: {
                    auto stopQueryRequest = nesRequest->as<StopQueryRequest>();
                    QueryId queryId = stopQueryRequest->getQueryId();
                    NES_INFO2("QueryProcessingService: Request received for stopping the query {}", queryId);
                    globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
                    break;
                }
                case RequestType::FailQuery: {
                    auto failQueryRequest = nesRequest->as<FailQueryRequest>();
                    QueryId queryId = failQueryRequest->getQueryId();
                    NES_INFO2("QueryProcessingService: Request received for stopping the query {}", queryId);
                    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);
                    break;
                }
                case RequestType::AddQuery: {
                    auto runQueryRequest = nesRequest->as<AddQueryRequest>();
                    QueryId queryId = runQueryRequest->getQueryId();
                    auto runRequest = nesRequest->as<AddQueryRequest>();
                    auto queryPlan = runRequest->getQueryPlan();
                    try {
                        queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

                        NES_INFO2("QueryProcessingService: Request received for optimizing and deploying of the query {}",
                                  queryId);
                        queryCatalogService->updateQueryStatus(queryId, QueryStatus::OPTIMIZING, "");

                        NES_DEBUG2("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
                        queryPlan = typeInferencePhase->execute(queryPlan);

                        NES_DEBUG2("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
                        setMemoryLayoutPhase->execute(queryPlan);

                        NES_DEBUG2("QueryProcessingService: Performing Query rewrite phase for query:  {}", queryId);
                        queryPlan = queryRewritePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw GlobalQueryPlanUpdateException(
                                "QueryProcessingService: Failed during query rewrite phase for query: "
                                + std::to_string(queryId));
                        }

                        queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

                        //Execute type inference phase on rewritten query plan
                        queryPlan = typeInferencePhase->execute(queryPlan);

                        //Generate sample code for elegant planner
                        if (runQueryRequest->getQueryPlacementStrategy() == Optimizer::PlacementStrategy::ELEGANT_BALANCED
                            || runQueryRequest->getQueryPlacementStrategy() == Optimizer::PlacementStrategy::ELEGANT_PERFORMANCE
                            || runQueryRequest->getQueryPlacementStrategy() == Optimizer::PlacementStrategy::ELEGANT_ENERGY) {
                            queryPlan = sampleCodeGenerationPhase->execute(queryPlan);
                        }

                        NES_DEBUG2("QueryProcessingService: Compute Signature inference phase for query:  {}", queryId);
                        signatureInferencePhase->execute(queryPlan);

                        NES_INFO2("Before {}", queryPlan->toString());
                        queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw GlobalQueryPlanUpdateException(
                                "QueryProcessingService: Failed during query topology specific rewrite phase for query: "
                                + std::to_string(queryId));
                        }
                        queryCatalogService->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

                        queryPlan = typeInferencePhase->execute(queryPlan);

                        if (!queryPlan) {
                            throw GlobalQueryPlanUpdateException(
                                "QueryProcessingService: Failed during Type inference phase for query: "
                                + std::to_string(queryId));
                        }

                        queryPlan = originIdInferencePhase->execute(queryPlan);

                        if (!queryPlan) {
                            throw GlobalQueryPlanUpdateException(
                                "QueryProcessingService: Failed during origin id inference phase for query: "
                                + std::to_string(queryId));
                        }

                        queryPlan = setMemoryLayoutPhase->execute(queryPlan);
                        if (!queryPlan) {
                            throw GlobalQueryPlanUpdateException(
                                "QueryProcessingService: Failed during Memory Layout Selection phase for query: "
                                + std::to_string(queryId));
                        }

                        queryCatalogService->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);
                        NES_DEBUG2("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
                        globalQueryPlan->addQueryPlan(queryPlan);
                    } catch (std::exception const& ex) {
                        throw;
                    }
                    break;
                }
                case RequestType::RemoveTopologyLink: break;
                case RequestType::RemoveTopologyNode: break;
                case RequestType::MigrateTopologyNode: break;
                default:
                    NES_ERROR2("QueryProcessingService: Received unhandled request type  {}", nesRequest->toString());
                    NES_WARNING2("QueryProcessingService: Skipping to process next request.");
            }
        }

        NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
        queryMergerPhase->execute(globalQueryPlan);
        //needed for containment merger phase to make sure that all operators have correct input and output schema
        for (const auto& item : globalQueryPlan->getSharedQueryPlansToDeploy()) {
            typeInferencePhase->execute(item->getQueryPlan());
        }
        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with:  {}", ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES::Optimizer
