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
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>
#include <utility>

namespace NES::Optimizer {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(QueryCatalogPtr queryCatalog,
                                                       const SourceCatalogPtr& streamCatalog,
                                                       GlobalQueryPlanPtr globalQueryPlan,
                                                       z3::ContextPtr z3Context,
                                                       const Configurations::OptimizerConfiguration optimizerConfiguration)
    : queryCatalog(std::move(queryCatalog)), globalQueryPlan(std::move(globalQueryPlan)), z3Context(std::move(z3Context)) {
    queryMergerPhase = QueryMergerPhase::create(this->z3Context, optimizerConfiguration.queryMergerRule);
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    //If query merger rule is using string based signature or graph isomorphism to identify the sharing opportunities
    //then apply special rewrite rules for improving the match identification
    bool applyRulesImprovingSharingIdentification =
        optimizerConfiguration.queryMergerRule == QueryMergerRule::SyntaxBasedCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::ImprovedStringSignatureBasedCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::HybridCompleteQueryMergerRule;

    queryRewritePhase = QueryRewritePhase::create(applyRulesImprovingSharingIdentification);
    topologySpecificQueryRewritePhase =
        TopologySpecificQueryRewritePhase::create(streamCatalog, optimizerConfiguration.performOnlySourceOperatorExpansion.getValue());
    signatureInferencePhase = SignatureInferencePhase::create(this->z3Context, optimizerConfiguration.queryMergerRule);
    setMemoryLayoutPhase = MemoryLayoutSelectionPhase::create(optimizerConfiguration.memoryLayoutPolicy);
}

GlobalQueryPlanUpdatePhasePtr
GlobalQueryPlanUpdatePhase::create(QueryCatalogPtr queryCatalog,
                                   SourceCatalogPtr streamCatalog,
                                   GlobalQueryPlanPtr globalQueryPlan,
                                   z3::ContextPtr z3Context,
                                   const Configurations::OptimizerConfiguration optimizerConfiguration) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(std::move(queryCatalog),
                                                                                   std::move(streamCatalog),
                                                                                   std::move(globalQueryPlan),
                                                                                   std::move(z3Context),
                                                                                   optimizerConfiguration));
}

GlobalQueryPlanPtr GlobalQueryPlanUpdatePhase::execute(const std::vector<NESRequestPtr>& nesRequests) {
    //FIXME: Proper error handling #1585
    try {
        //TODO: Parallelize this loop #1738
        for (const auto& nesRequest : nesRequests) {
            QueryId queryId = nesRequest->getQueryId();
            if (nesRequest->instanceOf<StopQueryRequest>()) {

                NES_INFO("QueryProcessingService: Request received for stopping the query " << queryId);
                globalQueryPlan->removeQuery(queryId);
            } else if (nesRequest->instanceOf<RunQueryRequest>()) {

                auto runRequest = nesRequest->as<RunQueryRequest>();
                auto queryPlan = runRequest->getQueryPlan();

                NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query " << queryId);
                queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);

                NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                queryPlan = typeInferencePhase->execute(queryPlan);

                NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase: " << queryId);
                setMemoryLayoutPhase->execute(queryPlan);

                NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " << queryId);
                queryPlan = queryRewritePhase->execute(queryPlan);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during query rewrite phase for query: "
                                    + std::to_string(queryId));
                }
                queryPlan = typeInferencePhase->execute(queryPlan);
                NES_DEBUG("QueryProcessingService: Compute Signature inference phase for query: " << queryId);
                signatureInferencePhase->execute(queryPlan);

                NES_INFO("Before " << queryPlan->toString());
                queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during query topology specific rewrite phase for query: "
                                    + std::to_string(queryId));
                }
                queryPlan = typeInferencePhase->execute(queryPlan);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during Type inference phase for query: "
                                    + std::to_string(queryId));
                }
                queryPlan = setMemoryLayoutPhase->execute(queryPlan);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during Memory Layout Selection phase for query: "
                                    + std::to_string(queryId));
                }
                queryCatalog->setExecutedQueryPlanForQuery(queryId, queryPlan);
                NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                globalQueryPlan->addQueryPlan(queryPlan);
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
