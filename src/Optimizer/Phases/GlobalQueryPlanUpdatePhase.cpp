/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>
#include <utility>

namespace NES::Optimizer {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(QueryCatalogPtr queryCatalog,
                                                       const StreamCatalogPtr& streamCatalog,
                                                       GlobalQueryPlanPtr globalQueryPlan,
                                                       z3::ContextPtr z3Context,
                                                       Optimizer::QueryMergerRule queryMergerRule)
    : queryCatalog(std::move(queryCatalog)), globalQueryPlan(std::move(globalQueryPlan)), z3Context(std::move(z3Context)) {
    queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, queryMergerRule);
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    bool applyRulesImprovingSharingIdentification = false;
    //If query merger rule is using string based signature or graph isomorphism to identify the sharing opportunities
    // then apply special rewrite rules for improving the match identification
    if (queryMergerRule == Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule
        || queryMergerRule == Optimizer::QueryMergerRule::ImprovedStringSignatureBasedCompleteQueryMergerRule
        || queryMergerRule == Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule
        || queryMergerRule == Optimizer::QueryMergerRule::HybridCompleteQueryMergerRule) {
        applyRulesImprovingSharingIdentification = true;
    }
    queryRewritePhase = QueryRewritePhase::create(applyRulesImprovingSharingIdentification);
    topologySpecificQueryRewritePhase = TopologySpecificQueryRewritePhase::create(streamCatalog);
    signatureInferencePhase = Optimizer::SignatureInferencePhase::create(this->z3Context, queryMergerRule);
}

GlobalQueryPlanUpdatePhasePtr GlobalQueryPlanUpdatePhase::create(QueryCatalogPtr queryCatalog,
                                                                 StreamCatalogPtr streamCatalog,
                                                                 GlobalQueryPlanPtr globalQueryPlan,
                                                                 z3::ContextPtr z3Context,
                                                                 Optimizer::QueryMergerRule queryMergerRule) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(std::move(queryCatalog),
                                                                                   std::move(streamCatalog),
                                                                                   std::move(globalQueryPlan),
                                                                                   std::move(z3Context),
                                                                                   queryMergerRule));
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
                auto startTI1 =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                queryPlan = typeInferencePhase->execute(queryPlan);
                auto endTI1 =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",Type-Inference-1 (micro)," << endTI1 - startTI1);

                NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query: " << queryId);
                auto startQR =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                queryPlan = queryRewritePhase->execute(queryPlan);
                auto endQR =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",QueryRewritePhase-Time (micro)," << endQR - startQR);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during query rewrite phase for query: "
                                    + std::to_string(queryId));
                }

                auto startTI2 =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                queryPlan = typeInferencePhase->execute(queryPlan);
                auto endTI2 =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",Type-Inference-2 (micro)," << endTI2 - startTI2);

                NES_DEBUG("QueryProcessingService: Compute Signature inference phase for query: " << queryId);

                auto startSig =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                signatureInferencePhase->execute(queryPlan);
                auto endSig =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",Signature-Computation-Time (micro)," << endSig - startSig);

                NES_INFO("Before " << queryPlan->toString());
                auto startTSQR =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);
                auto endTSQR =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",TopologySpecificQueryRewritePhase-Time (micro)," << endTSQR - startTSQR);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during query topology specific rewrite phase for query: "
                                    + std::to_string(queryId));
                }
                auto startTI3 =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                queryPlan = typeInferencePhase->execute(queryPlan);
                auto endTI3 =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",Type-Inference-3 (micro)," << endTI3 - startTI3);
                if (!queryPlan) {
                    throw Exception("QueryProcessingService: Failed during Type inference phase for query: "
                                    + std::to_string(queryId));
                }

                NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query: " << queryId);
                auto startGQP =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                globalQueryPlan->addQueryPlan(queryPlan);
                auto endGQP =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_BM(queryId << ",GlobalQueryPlan-Addition (micro)," << endGQP - startGQP);
            } else {
                NES_ERROR("QueryProcessingService: Received unhandled request type " << nesRequest->toString());
                NES_WARNING("QueryProcessingService: Skipping to process next request.");
                continue;
            }
        }

        NES_BM("Benchmark Size: " << nesRequests.size());
        NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
        auto startQM =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        queryMergerPhase->execute(globalQueryPlan);
        auto endQM =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        NES_BM(nesRequests[0]->getQueryId() << ",Total-QM-Time (micro)," << endQM - startQM);
        NES_DEBUG("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with: " << ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES::Optimizer
