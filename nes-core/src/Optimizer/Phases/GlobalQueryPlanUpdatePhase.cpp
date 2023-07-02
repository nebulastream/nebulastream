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
#include <Plans/Global/Query/SharedQueryPlan.hpp>
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
                                                       const Catalogs::UDF::UDFCatalogPtr& udfCatalog)
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
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::HybridCompleteQueryMergerRule
        || optimizerConfiguration.queryMergerRule == QueryMergerRule::ImprovedHashSignatureBasedPartialQueryMergerRule;

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
                                   Catalogs::UDF::UDFCatalogPtr udfCatalog) {
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
                NES_DEBUG2("QueryProcessingService: Request received for stopping the query {}", queryId);
                globalQueryPlan->removeQuery(queryId, RequestType::Stop);
            } else if (nesRequest->instanceOf<FailQueryRequest>()) {
                auto failQueryRequest = nesRequest->as<FailQueryRequest>();
                QueryId queryId = failQueryRequest->getQueryId();
                NES_DEBUG2("QueryProcessingService: Request received for stopping the query {}", queryId);
                globalQueryPlan->removeQuery(queryId, RequestType::Fail);
            } else if (nesRequest->instanceOf<RunQueryRequest>()) {
                auto runQueryRequest = nesRequest->as<RunQueryRequest>();
                QueryId queryId = runQueryRequest->getQueryId();
                auto runRequest = nesRequest->as<RunQueryRequest>();
                auto queryPlan = runRequest->getQueryPlan();
                try {
                    queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

                    NES_DEBUG2("QueryProcessingService: Request received for optimizing and deploying of the query {}", queryId);
                    queryCatalogService->updateQueryStatus(queryId, QueryStatus::OPTIMIZING, "");

                    NES_DEBUG2("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
                    auto startTI1 =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    queryPlan = typeInferencePhase->execute(queryPlan);
                    auto endTI1 =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->typeInferencePhase1 += endTI1 - startTI1;
                    NES_DEBUG2("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
                    setMemoryLayoutPhase->execute(queryPlan);

                    auto startQR =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    NES_DEBUG2("QueryProcessingService: Performing Query rewrite phase for query:  {}", queryId);
                    queryPlan = queryRewritePhase->execute(queryPlan);
                    auto endQR =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->queryRewritePhaseTime += endQR - startQR;

                    if (!queryPlan) {
                        throw GlobalQueryPlanUpdateException(
                            "QueryProcessingService: Failed during query rewrite phase for query: " + std::to_string(queryId));
                    }
                    queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

                    auto startTI2 =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    queryPlan = typeInferencePhase->execute(queryPlan);
                    auto endTI2 =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->typeInferencePhase2 += endTI2 - startTI2;

                    NES_DEBUG2("QueryProcessingService: Compute Signature inference phase for query:  {}", queryId);
                    auto startSig =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    signatureInferencePhase->execute(queryPlan);
                    auto endSig =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->signatureInferencePhase1 += endSig - startSig;

                    NES_DEBUG2("Before {}", queryPlan->toString());
                    auto startTSQR =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);
                    auto endTSQR =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->topologySpecificRewritePhase += endTSQR - startTSQR;
                    if (!queryPlan) {
                        throw GlobalQueryPlanUpdateException(
                            "QueryProcessingService: Failed during query topology specific rewrite phase for query: "
                            + std::to_string(queryId));
                    }
                    queryCatalogService->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

                    auto startTI3 =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    queryPlan = typeInferencePhase->execute(queryPlan);
                    auto endTI3 =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->typeInferencePhase3 += endTI3 - startTI3;

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
                    NES_DEBUG2("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
                    auto startGQP =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->addQueryPlan(queryPlan);
                    auto endGQP =
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                    globalQueryPlan->globalQueryPlanAddition += endGQP - startGQP;
                } catch (std::exception const& ex) {
                    throw;
                }
            } else {
                NES_ERROR2("QueryProcessingService: Received unhandled request type  {}", nesRequest->toString());
                NES_WARNING2("QueryProcessingService: Skipping to process next request.");
                continue;
            }
        }

        NES_DEBUG2("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
        auto startQM =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        queryMergerPhase->execute(globalQueryPlan);
        auto endQM =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        globalQueryPlan->mergerExecutionPhase += endQM - startQM;
        NES_DEBUG2("GlobalQueryPlanUpdatePhase: Successfully updated global query plan");
        for (const auto& item : globalQueryPlan->getAllSharedQueryPlans()) {
            typeInferencePhase->execute(item->getQueryPlan());
        }
        return globalQueryPlan;
    } catch (std::exception& ex) {
        NES_ERROR2("GlobalQueryPlanUpdatePhase: Exception occurred while updating global query plan with:  {}", ex.what());
        throw GlobalQueryPlanUpdateException("GlobalQueryPlanUpdatePhase: Exception occurred while updating Global Query Plan");
    }
}

}// namespace NES::Optimizer
