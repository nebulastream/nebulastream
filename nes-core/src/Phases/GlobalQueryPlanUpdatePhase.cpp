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
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Phases/SampleCodeGenerationPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyLinkRequest.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyNodeRequest.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

GlobalQueryPlanUpdatePhase::GlobalQueryPlanUpdatePhase(
    const TopologyPtr& topology,
    const QueryCatalogServicePtr& queryCatalogService,
    const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
    const GlobalQueryPlanPtr& globalQueryPlan,
    const z3::ContextPtr& z3Context,
    const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
    const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
    const GlobalExecutionPlanPtr& globalExecutionPlan)
    : topology(topology), globalExecutionPlan(globalExecutionPlan), queryCatalogService(queryCatalogService),
      globalQueryPlan(globalQueryPlan), z3Context(z3Context) {

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
                                   const QueryCatalogServicePtr& queryCatalogService,
                                   const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                   const GlobalQueryPlanPtr& globalQueryPlan,
                                   const z3::ContextPtr& z3Context,
                                   const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                   const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                   const GlobalExecutionPlanPtr& globalExecutionPlan) {
    return std::make_shared<GlobalQueryPlanUpdatePhase>(GlobalQueryPlanUpdatePhase(topology,
                                                                                   queryCatalogService,
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
                case RequestType::RemoveTopologyLink: {
                    processRemoveTopologyLinkRequest(nesRequest->as<NES::Experimental::RemoveTopologyLinkRequest>());
                    break;
                }
                case RequestType::RemoveTopologyNode: {
                    processRemoveTopologyNodeRequest(nesRequest->as<NES::Experimental::RemoveTopologyNodeRequest>());
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
    queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);
    NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query {}", queryId);

    //2. Set query status as Optimizing
    queryCatalogService->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

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
    queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

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
    queryCatalogService->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

    //12. Perform type inference over re-written query plan
    queryPlan = typeInferencePhase->execute(queryPlan);

    //13. Identify the number of origins and their ids for all logical operators
    queryPlan = originIdInferencePhase->execute(queryPlan);

    //14. Set memory layout of each logical operator in the rewritten query
    NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
    queryPlan = setMemoryLayoutPhase->execute(queryPlan);

    //15. Add the updated query plan to the query catalog
    queryCatalogService->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

    //16. Add the updated query plan to the global query plan
    NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
    globalQueryPlan->addQueryPlan(queryPlan);

    //17. Perform query merging for newly added query plan
    NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
    queryMergerPhase->execute(globalQueryPlan);
}

void GlobalQueryPlanUpdatePhase::processRemoveTopologyLinkRequest(
    const NES::Experimental::RemoveTopologyLinkRequestPtr& removeTopologyLinkRequest) {

    TopologyNodeId upstreamNodeId = removeTopologyLinkRequest->getUpstreamNodeId();
    TopologyNodeId downstreamNodeId = removeTopologyLinkRequest->getDownstreamNodeId();

    auto upstreamExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(upstreamNodeId);
    auto downstreamExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(downstreamNodeId);
    //If any of the two execution nodes do not exist then skip rest of the operation
    if (!upstreamExecutionNode || !downstreamExecutionNode) {
        NES_INFO("RemoveTopologyLinkRequest: {} has no effect on the running queries", removeTopologyLinkRequest->toString());
        return;
    }

    auto upstreamSharedQueryIds = upstreamExecutionNode->getPlacedSharedQueryPlanIds();
    auto downstreamSharedQueryIds = downstreamExecutionNode->getPlacedSharedQueryPlanIds();
    //If any of the two execution nodes do not have any shared query plan placed then skip rest of the operation
    if (upstreamSharedQueryIds.empty() || downstreamSharedQueryIds.empty()) {
        NES_INFO("RemoveTopologyLinkRequest: {} has no effect on the running queries", removeTopologyLinkRequest->toString());
        return;
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
        NES_INFO("Found no shared query plan that was using the removed link");
        return;
    }

    //Iterate over each shared query plan id and identify the operators that need to be replaced
    for (auto impactedSharedQueryId : impactedSharedQueryIds) {
        markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
    }
}

void GlobalQueryPlanUpdatePhase::processRemoveTopologyNodeRequest(
    const NES::Experimental::RemoveTopologyNodeRequestPtr& removeTopologyNodeRequest) {

    TopologyNodeId removedNodeId = removeTopologyNodeRequest->getTopologyNodeId();

    //1. If the removed execution nodes do not exist then remove skip rest of the operation
    auto removedExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(removedNodeId);
    if (!removedExecutionNode) {
        NES_INFO("RemoveTopologyNodeRequest: {} has no effect on the running queries as there are no queries "
                 "placed on the node.",
                 removeTopologyNodeRequest->toString());
        return;
    }

    //2. If the removed execution nodes does not have any shared query plan placed then skip rest of the operation
    auto impactedSharedQueryIds = removedExecutionNode->getPlacedSharedQueryPlanIds();
    if (impactedSharedQueryIds.empty()) {
        NES_INFO("RemoveTopologyNodeRequest: {} has no effect on the running queries as there are no queries placed "
                 "on the node.",
                 removeTopologyNodeRequest->toString());
        return;
    }

    //3. Get the topology node with removed node id
    TopologyNodePtr removedTopologyNode = topology->findNodeWithId(removedNodeId);

    //4. Fetch upstream and downstream topology nodes connected via the removed topology node
    auto downstreamTopologyNodes = removedTopologyNode->getParents();
    auto upstreamTopologyNodes = removedTopologyNode->getChildren();

    //5. If the topology node either do not have upstream or downstream node then fail the request
    if (upstreamTopologyNodes.empty() || downstreamTopologyNodes.empty()) {
        //FIXME: how to handle this case? If the node to remove has physical source then we may need to kill the
        // whole query.
        NES_NOT_IMPLEMENTED();
    }

    //6. Iterate over all upstream and downstream topology node pairs and try to mark operators for re-placement
    for (auto const& upstreamTopologyNode : upstreamTopologyNodes) {
        for (auto const& downstreamTopologyNode : downstreamTopologyNodes) {

            //6.1. Iterate over impacted shared query plan ids to identify the shared query plans placed on the
            // upstream and downstream execution nodes
            for (auto const& impactedSharedQueryId : impactedSharedQueryIds) {

                auto upstreamExecutionNode =
                    globalExecutionPlan->getExecutionNodeByNodeId(upstreamTopologyNode->as<TopologyNode>()->getId());
                auto downstreamExecutionNode =
                    globalExecutionPlan->getExecutionNodeByNodeId(downstreamTopologyNode->as<TopologyNode>()->getId());

                //6.2. If there exists no upstream or downstream execution nodes than skip rest of the operation
                if (!upstreamExecutionNode || !downstreamExecutionNode) {
                    continue;
                }

                //6.3. Only process the upstream and downstream execution node pairs when both have shared query plans
                // with the impacted shared query id
                if (upstreamExecutionNode->hasQuerySubPlans(impactedSharedQueryId)
                    && downstreamExecutionNode->hasQuerySubPlans(impactedSharedQueryId)) {
                    markOperatorsForReOperatorPlacement(impactedSharedQueryId, upstreamExecutionNode, downstreamExecutionNode);
                }
            }
        }
    }
}

void GlobalQueryPlanUpdatePhase::markOperatorsForReOperatorPlacement(SharedQueryId sharedQueryPlanId,
                                                                     const ExecutionNodePtr& upstreamExecutionNode,
                                                                     const ExecutionNodePtr& downstreamExecutionNode) {

    //1. Iterate over all upstream sub query plans and extract the operator id of most upstream non-system
    // generated (anything except Network Sink or Source) operator.
    std::set<OperatorId> upstreamOperatorIds;
    getUpstreamPinnedOperatorIds(sharedQueryPlanId, upstreamExecutionNode, upstreamOperatorIds);

    //2. Iterate over all sub query plans in the downstream execution node and extract the operator ids of most upstream non-system
    // generated (anything except Network Sink or Source) operator.
    std::set<OperatorId> downstreamOperatorIds;
    getDownstreamPinnedOperatorIds(sharedQueryPlanId, downstreamExecutionNode, downstreamOperatorIds);

    //Note to self - do we need to consider the possibility that a sub query plan that is placed on the given upstream node does not
    // communicate with the given downstream node?

    //3. Fetch the shared query plan
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryPlanId);

    //4. Mark the operators for re-operator placement
    sharedQueryPlan->performReOperatorPlacement(upstreamOperatorIds, downstreamOperatorIds);
}

void GlobalQueryPlanUpdatePhase::getUpstreamPinnedOperatorIds(const SharedQueryId& sharedQueryPlanId,
                                                              const ExecutionNodePtr& upstreamExecutionNode,
                                                              std::set<OperatorId>& upstreamOperatorIds) const {

    auto upstreamSubQueryPlans = upstreamExecutionNode->getQuerySubPlans(sharedQueryPlanId);
    for (const auto& upstreamSubQueryPlan : upstreamSubQueryPlans) {
        //1.1 Fetch all sink operators of the sub query plan to find the most upstream non-system generated operator.
        auto sinkOperators = upstreamSubQueryPlan->getSinkOperators();
        for (const auto& sinkOperator : sinkOperators) {
            //1.2 Fetch upstream operator of the sink operator to find the most upstream non-system generated operator
            auto children = sinkOperator->getChildren();
            for (const NodePtr& child : children) {
                if (child->instanceOf<SourceLogicalOperatorNode>()
                    && child->as<SourceLogicalOperatorNode>()
                           ->getSourceDescriptor()
                           ->instanceOf<Network::NetworkSourceDescriptor>()) {

                    //1.3 Identify non-system generated pinned upstream operator from the next upstream execution node
                    for (const auto& nextUpstreamExecutionNode : upstreamExecutionNode->getChildren()) {
                        getUpstreamPinnedOperatorIds(sharedQueryPlanId,
                                                     nextUpstreamExecutionNode->as<ExecutionNode>(),
                                                     upstreamOperatorIds);
                    }
                } else {
                    OperatorId upstreamOperatorId = child->as<LogicalOperatorNode>()->getId();
                    upstreamOperatorIds.insert(upstreamOperatorId);
                }
            }
        }
    }
}

void GlobalQueryPlanUpdatePhase::getDownstreamPinnedOperatorIds(const SharedQueryId& sharedQueryPlanId,
                                                                const ExecutionNodePtr& downstreamExecutionNode,
                                                                std::set<OperatorId>& downstreamOperatorIds) const {

    auto downstreamSubQueryPlans = downstreamExecutionNode->getQuerySubPlans(sharedQueryPlanId);
    for (const auto& downstreamSubQueryPlan : downstreamSubQueryPlans) {
        //1.1 Fetch all source operators of the sub query plan to find the most downstream non-system generated operator.
        auto sourceOperators = downstreamSubQueryPlan->getSourceOperators();
        for (const SourceLogicalOperatorNodePtr& sourceOperator : sourceOperators) {
            //1.2 Fetch upstream operator of the sink operator to find the most upstream non-system generated operator
            auto parents = sourceOperator->getParents();
            for (const NodePtr& parent : parents) {
                if (parent->instanceOf<SinkLogicalOperatorNode>()
                    && parent->as<SinkLogicalOperatorNode>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                    //1.3 Identify non-system generated pinned downstream operator from the next downstream execution node
                    for (const auto& nextDownstreamExecutionNode : downstreamExecutionNode->getParents()) {
                        getDownstreamPinnedOperatorIds(sharedQueryPlanId,
                                                       nextDownstreamExecutionNode->as<ExecutionNode>(),
                                                       downstreamOperatorIds);
                    }
                } else {
                    OperatorId downstreamOperatorId = parent->as<LogicalOperatorNode>()->getId();
                    downstreamOperatorIds.insert(downstreamOperatorId);
                }
            }
        }
    }
}

}// namespace NES::Optimizer
