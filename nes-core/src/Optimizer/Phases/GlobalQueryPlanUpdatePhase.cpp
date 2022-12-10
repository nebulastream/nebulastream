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
#include <Common/Identifiers.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/FailQueryRequest.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>

#include <utility>

namespace NES::Optimizer {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(TopologyPtr topology,
                                                       QueryCatalogServicePtr queryCatalogService,
                                                       const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                       GlobalQueryPlanPtr globalQueryPlan,
                                                       z3::ContextPtr z3Context,
                                                       const Configurations::OptimizerConfiguration optimizerConfiguration,
                                                       const Catalogs::UDF::UdfCatalogPtr& udfCatalog)
    : topology(topology), queryCatalogService(std::move(queryCatalogService)), globalQueryPlan(std::move(globalQueryPlan)),
      z3Context(std::move(z3Context)) {
    queryMergerPhase = QueryMergerPhase::create(this->z3Context, optimizerConfiguration.queryMergerRule);
    typeInferencePhase = TypeInferencePhase::create(sourceCatalog, udfCatalog);
    //If query merger rule is using string based signature or graph isomorphism to identify the sharing opportunities
    //then apply special rewrite rules for improving the match identification
    bool applyRulesImprovingSharingIdentification =
        optimizerConfiguration.queryMergerRule == QueryMergerRule::SyntaxBasedCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::ImprovedHashSignatureBasedCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::HybridCompleteQueryMergerRule;

    queryRewritePhase = QueryRewritePhase::create(applyRulesImprovingSharingIdentification);
    originIdInferencePhase = OriginIdInferencePhase::create();
    topologySpecificQueryRewritePhase =
        TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfiguration);
    signatureInferencePhase = SignatureInferencePhase::create(this->z3Context, optimizerConfiguration.queryMergerRule);
    setMemoryLayoutPhase = MemoryLayoutSelectionPhase::create(optimizerConfiguration.memoryLayoutPolicy);
}

GlobalQueryPlanUpdatePhasePtr
GlobalQueryPlanUpdatePhase::create(TopologyPtr topology,
                                   QueryCatalogServicePtr queryCatalogService,
                                   Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                   GlobalQueryPlanPtr globalQueryPlan,
                                   z3::ContextPtr z3Context,
                                   const Configurations::OptimizerConfiguration optimizerConfiguration,
                                   Catalogs::UDF::UdfCatalogPtr udfCatalog) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(topology,
                                                                                   std::move(queryCatalogService),
                                                                                   std::move(sourceCatalog),
                                                                                   std::move(globalQueryPlan),
                                                                                   std::move(z3Context),
                                                                                   optimizerConfiguration,
                                                                                   std::move(udfCatalog)));
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<NESRequestPtr>& nesRequests) {
    //FIXME: Proper error handling #1585
    try {
        //TODO: Parallelize this loop #1738
        for (const auto& nesRequest : nesRequests) {
            if (nesRequest->instanceOf<StopQueryRequest>()) {
                auto stopQueryRequest = nesRequest->as<StopQueryRequest>();
                QueryId queryId = stopQueryRequest->getQueryId();
                NES_INFO("QueryProcessingService: Request received for stopping the query " << queryId);
                globalQueryPlan->removeQuery(queryId, RequestType::Stop);
            } else if (nesRequest->instanceOf<FailQueryRequest>()) {
                auto failQueryRequest = nesRequest->as<FailQueryRequest>();
                QueryId queryId = failQueryRequest->getQueryId();
                NES_INFO("QueryProcessingService: Request received for stopping the query " << queryId);
                globalQueryPlan->removeQuery(queryId, RequestType::Fail);
            } else if (nesRequest->instanceOf<RunQueryRequest>()) {
                auto runQueryRequest = nesRequest->as<RunQueryRequest>();
                QueryId queryId = runQueryRequest->getQueryId();
                auto runRequest = nesRequest->as<RunQueryRequest>();
                auto queryPlan = runRequest->getQueryPlan();
                try {
                    queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

                    NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query " << queryId);
                    queryCatalogService->updateQueryStatus(queryId, QueryStatus::Optimizing, "");

                    NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                    queryPlan = typeInferencePhase->execute(queryPlan);

                    NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase: " << queryId);
                    setMemoryLayoutPhase->execute(queryPlan);

                    NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " << queryId);
                    queryPlan = queryRewritePhase->execute(queryPlan);

                    if (!queryPlan) {
                        throw GlobalQueryPlanUpdateException(
                            "QueryProcessingService: Failed during query rewrite phase for query: " + std::to_string(queryId));
                    }
                    queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

                    queryPlan = typeInferencePhase->execute(queryPlan);

                    NES_DEBUG("QueryProcessingService: Compute Signature inference phase for query: " << queryId);
                    signatureInferencePhase->execute(queryPlan);

                    NES_INFO("Before " << queryPlan->toString());
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
                            "QueryProcessingService: Failed during Type inference phase for query: " + std::to_string(queryId));
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
                    NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                    globalQueryPlan->addQueryPlan(queryPlan);
                } catch (std::exception const& ex) {
                    throw;
                }
            } else {
                NES_ERROR("QueryProcessingService: Received unhandled request type " << nesRequest->toString());
                NES_WARNING("QueryProcessingService: Skipping to process next request.");
                continue;
            }
        }

        NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
        queryMergerPhase->execute(globalQueryPlan);

        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with: " << ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES::Optimizer
