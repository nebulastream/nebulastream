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
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SampleCodeGenerationPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <WorkQueues/RequestTypes/QueryRequests/StopQueryRequest.hpp>
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
                    try {
                        auto runQueryRequest = nesRequest->as<AddQueryRequest>();
                        QueryId queryId = runQueryRequest->getQueryId();
                        auto runRequest = nesRequest->as<AddQueryRequest>();
                        auto queryPlan = runRequest->getQueryPlan();

                        //1. Add the initial version of the query to the query catalog
                        queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);
                        NES_INFO2("QueryProcessingService: Request received for optimizing and deploying of the query {}",
                                  queryId);

                        //2. Set query status as Optimizing
                        queryCatalogService->updateQueryStatus(queryId, QueryStatus::OPTIMIZING, "");

                        //3. Execute type inference phase
                        NES_DEBUG2("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
                        queryPlan = typeInferencePhase->execute(queryPlan);

                        //4. Set memory layout of each logical operator
                        NES_DEBUG2("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
                        queryPlan = setMemoryLayoutPhase->execute(queryPlan);

                        //5. Perform query re-write
                        NES_DEBUG2("QueryProcessingService: Performing Query rewrite phase for query:  {}", queryId);
                        queryPlan = queryRewritePhase->execute(queryPlan);

                        //6. Add the updated query plan to the query catalog
                        queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

                        //7. Execute type inference phase on rewritten query plan
                        queryPlan = typeInferencePhase->execute(queryPlan);

                        //8. Generate sample code for elegant planner
                        if (runQueryRequest->getQueryPlacementStrategy() == Optimizer::PlacementStrategy::ELEGANT_BALANCED
                            || runQueryRequest->getQueryPlacementStrategy() == Optimizer::PlacementStrategy::ELEGANT_PERFORMANCE
                            || runQueryRequest->getQueryPlacementStrategy() == Optimizer::PlacementStrategy::ELEGANT_ENERGY) {
                            queryPlan = sampleCodeGenerationPhase->execute(queryPlan);
                        }

                        //9. Perform signature inference phase for sharing identification among query plans
                        signatureInferencePhase->execute(queryPlan);

                        //10. Perform topology specific rewrites to the query plan
                        queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

                        //11. Add the updated query plan to the query catalog
                        queryCatalogService->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

                        //12. Perform type inference over re-written query plan
                        queryPlan = typeInferencePhase->execute(queryPlan);

                        //13. Identify the number of origins and their ids for all logical operators
                        queryPlan = originIdInferencePhase->execute(queryPlan);

                        //14. Set memory layout of each logical operator in the rewritten query
                        NES_DEBUG2("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
                        queryPlan = setMemoryLayoutPhase->execute(queryPlan);

                        //15. Add the updated query plan to the query catalog
                        queryCatalogService->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

                        //16. Add the updated query plan to the global query plan
                        NES_DEBUG2("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
                        globalQueryPlan->addQueryPlan(queryPlan);
                    } catch (std::exception const& ex) {
                        throw;
                    }
                    break;
                }
                case RequestType::RemoveTopologyLink: {

                    auto removeTopologyLinkRequest = nesRequest->as<NES::Experimental::RemoveTopologyLinkRequest>();
                    TopologyNodeId upstreamNodeId = removeTopologyLinkRequest->getUpstreamNodeId();
                    TopologyNodeId downstreamNodeId = removeTopologyLinkRequest->getDownstreamNodeId();

                    auto upstreamExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(upstreamNodeId);
                    auto downstreamExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(downstreamNodeId);
                    //If any of the two execution nodes do not exist then remove the
                    if (!upstreamExecutionNode || !downstreamExecutionNode) {
                        NES_INFO2("RemoveTopologyLinkRequest: {} has no effect on the running queries",
                                  removeTopologyLinkRequest->toString());
                        break;
                    }

                    auto upstreamSharedQueryIds = upstreamExecutionNode->getPlacedSharedQueryPlanIds();
                    auto downstreamSharedQueryIds = downstreamExecutionNode->getPlacedSharedQueryPlanIds();
                    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
                    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
                        NES_INFO2("RemoveTopologyLinkRequest: {} has no effect on the running queries",
                                  removeTopologyLinkRequest->toString());
                        break;
                    }

                    //compute intersection among the shared query plans placed on two nodes
                    std::set<SharedQueryId> impactedSharedQueryIds;
                    std::set_intersection(upstreamSharedQueryIds.begin(),
                                          upstreamSharedQueryIds.end(),
                                          downstreamSharedQueryIds.begin(),
                                          downstreamSharedQueryIds.end(),
                                          std::inserter(impactedSharedQueryIds, impactedSharedQueryIds.begin()));

                    //If no common shared query plan was found to be placed on two nodes then skip rest of the operation
                    if (impactedSharedQueryIds.empty()) {
                        NES_INFO2("Found no shared query plan that was using the removed link");
                        break;
                    }

                    //Iterate over each shared query plan id and identify the operators that need to be replaced
                    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
                        markOperatorsForReOperatorPlacement(impactedSharedQueryId,
                                                            upstreamExecutionNode,
                                                            downstreamExecutionNode);
                    }
                    break;
                }
                case RequestType::RemoveTopologyNode: {
                    auto removeTopologyLinkRequest = nesRequest->as<NES::Experimental::RemoveTopologyLinkRequest>();
                    TopologyNodeId upstreamNodeId = removeTopologyLinkRequest->getUpstreamNodeId();
                    TopologyNodeId downstreamNodeId = removeTopologyLinkRequest->getDownstreamNodeId();

                    auto upstreamExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(upstreamNodeId);
                    auto downstreamExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(downstreamNodeId);
                    //If any of the two execution nodes do not exist then remove the
                    if (!upstreamExecutionNode || !downstreamExecutionNode) {
                        NES_INFO2("RemoveTopologyLinkRequest: {} has no effect on the running queries",
                                  removeTopologyLinkRequest->toString());
                        break;
                    }

                    auto upstreamSharedQueryIds = upstreamExecutionNode->getPlacedSharedQueryPlanIds();
                    auto downstreamSharedQueryIds = downstreamExecutionNode->getPlacedSharedQueryPlanIds();
                    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
                    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
                        NES_INFO2("RemoveTopologyLinkRequest: {} has no effect on the running queries",
                                  removeTopologyLinkRequest->toString());
                        break;
                    }

                    //compute intersection among the shared query plans placed on two nodes
                    std::set<SharedQueryId> impactedSharedQueryIds;
                    std::set_intersection(upstreamSharedQueryIds.begin(),
                                          upstreamSharedQueryIds.end(),
                                          downstreamSharedQueryIds.begin(),
                                          downstreamSharedQueryIds.end(),
                                          std::inserter(impactedSharedQueryIds, impactedSharedQueryIds.begin()));

                    //If no common shared query plan was found to be placed on two nodes then skip rest of the operation
                    if (impactedSharedQueryIds.empty()) {
                        NES_INFO2("Found no shared query plan that was using the removed link");
                        break;
                    }

                    //Iterate over each shared query plan id and identify the operators that need to be replaced
                    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
                        markOperatorsForReOperatorPlacement(impactedSharedQueryId,
                                                            upstreamExecutionNode,
                                                            downstreamExecutionNode);
                    }
                    break;
                }
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

void GlobalQueryPlanUpdatePhase::markOperatorsForReOperatorPlacement(SharedQueryId sharedQueryPlanId,
                                                                     const ExecutionNodePtr& upstreamExecutionNode,
                                                                     const ExecutionNodePtr& downstreamExecutionNode) {

    //1. Compute the upstream and downstream operator ids
    std::set<OperatorId> upstreamOperatorIds;
    std::set<OperatorId> downstreamOperatorIds;

    TopologyNodeId upstreamTopologyNodeId = upstreamExecutionNode->getId();
    TopologyNodeId downstreamTopologyNodeId = downstreamExecutionNode->getId();

    //2. Iterate over all upstream sub query plans and extract the operator id of most upstream non-system
    // generated (anything except Network Sink or Source) operator.
    auto upstreamSubQueryPlans = upstreamExecutionNode->getQuerySubPlans(sharedQueryPlanId);
    for (const auto& upstreamSubQueryPlan : upstreamSubQueryPlans) {
        //2.1 Fetch all sink operators of the sub query plan to find the most upstream non-system generated operator.
        auto sinkOperators = upstreamSubQueryPlan->getSinkOperators();
        for (const auto& sinkOperator : sinkOperators) {
            //2.2 Fetch upstream operator of the sink operator to find the most upstream non-system generated operator
            auto children = sinkOperator->getChildren();
            for (const NodePtr& child : children) {
                OperatorId upstreamOperatorId;
                if (child->instanceOf<SourceLogicalOperatorNode>()
                    && child->as<SourceLogicalOperatorNode>()
                           ->getSourceDescriptor()
                           ->instanceOf<Network::NetworkSourceDescriptor>()) {
                    //FIXME: add logic to find the next possible non-system generated downstream operator
                } else {
                    upstreamOperatorId = child->as<LogicalOperatorNode>()->getId();
                }
                upstreamOperatorIds.insert(upstreamOperatorId);
            }
        }
    }

    //3. Iterate over all downstream sub query plans and extract the operator id of most upstream non-system
    // generated (anything except Network Sink or Source) operator.
    auto downstreamSubQueryPlans = downstreamExecutionNode->getQuerySubPlans(sharedQueryPlanId);
    for (const auto& downstreamSubQueryPlan : downstreamSubQueryPlans) {
        //3.1 Fetch all source operators of the sub query plan to find the most downstream non-system generated operator.
        auto sourceOperators = downstreamSubQueryPlan->getSourceOperators();
        for (const SourceLogicalOperatorNodePtr& sourceOperator : sourceOperators) {
            //3.2 Fetch upstream operator of the sink operator to find the most upstream non-system generated operator
            auto parents = sourceOperator->getParents();
            for (const NodePtr& parent : parents) {
                OperatorId downstreamOperatorId;
                if (parent->instanceOf<SinkLogicalOperatorNode>()
                    && parent->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                    //FIXME: add logic to find the next possible non-system generated downstream operator
                } else {
                    downstreamOperatorId = parent->as<LogicalOperatorNode>()->getId();
                }
                downstreamOperatorIds.insert(downstreamOperatorId);
            }
        }
    }

    //NOTE: to self - do we need to consider the possibility that a sub query plan that is placed on the given upstream node does not
    // communicate with the given downstream node?

    //4. Fetch the shared query plan
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId);

    //5. Mark the operators for re-operator placement
    sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);
}

}// namespace NES::Optimizer
