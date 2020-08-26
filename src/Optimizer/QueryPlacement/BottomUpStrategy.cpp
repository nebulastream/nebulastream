#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

using namespace std;
namespace NES {

std::unique_ptr<BottomUpStrategy> BottomUpStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog) {
    return std::make_unique<BottomUpStrategy>(BottomUpStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

BottomUpStrategy::BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const QueryId queryId = queryPlan->getQueryId();
    NES_INFO("BottomUpStrategy: Performing placement of the input query plan with id " << queryId);

    NES_DEBUG("BottomUpStrategy: Get all source operators");
    const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
    if (!sourceOperators.empty()) {
        NES_ERROR("BottomUpStrategy: No source operators found in the query plan wih id: " << queryId);
        throw QueryPlacementException("BottomUpStrategy: No source operators found in the query plan wih id: " + queryId);
    }

    NES_DEBUG("BottomUpStrategy: Prepare a map of source to physical nodes");
    std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes;
    for (auto& sourceOperator : sourceOperators) {
        if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
            throw QueryPlacementException("BottomUpStrategy: Source Descriptor need stream name: " + queryId);
        }
        const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        if (mapOfSourceToTopologyNodes.find(streamName) != mapOfSourceToTopologyNodes.end()) {
            NES_TRACE("BottomUpStrategy: Entry for the logical stream " << streamName << " already present.");
            continue;
        }
        NES_TRACE("BottomUpStrategy: Get all topology nodes for the logical source stream");
        const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
        if (sourceNodes.empty()) {
            NES_ERROR("BottomUpStrategy: No source found in the topology for stream " + streamName + "for query with id : " + std::to_string(queryId));
            throw QueryPlacementException("BottomUpStrategy: No source found in the topology for stream " + streamName + "for query with id : " + std::to_string(queryId));
        }
        mapOfSourceToTopologyNodes[streamName] = sourceNodes;
    }

    NES_DEBUG("BottomUpStrategy: Preparing execution plan for query with id : " << queryId);
    placeOperators(queryPlan, mapOfSourceToTopologyNodes);

    TopologyNodePtr rootNode = topology->getRoot();

    for (TopologyNodePtr targetSource : sourceNodes) {
        NES_DEBUG("BottomUpStrategy: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
        TopologyNodePtr startNode = topology->findAllPathBetween(targetSource, rootNode).value();
        NES_INFO("BottomUpStrategy: Adding system generated operators for query with id : " << queryId);
        addSystemGeneratedOperators(queryId, startNode);
    }
    return true;
}

void BottomUpStrategy::placeOperators(QueryPlanPtr queryPlan, std::map<std::string, std::vector<TopologyNodePtr>> mapOfSourceToTopologyNodes) {

    QueryId queryId = queryPlan->getQueryId();
    TopologyNodePtr sinkNode = topology->getRoot();

    vector<TopologyNodePtr> topoSubGrphForOP;
    for (auto& entry : mapOfSourceToTopologyNodes) {
        NES_TRACE("BottomUpStrategy: ");
        vector<TopologyNodePtr> topoSubGrphForSrc = topology->findPathBetween(entry.second, {sinkNode});
        if (topoSubGrphForOP.empty()) {
            topoSubGrphForOP = topoSubGrphForSrc;
            continue;
        } else {
            topoSubGrphForOP.insert(topoSubGrphForOP.end(), topoSubGrphForSrc.begin(), topoSubGrphForSrc.end());
        }
        NES_TRACE("BottomUpStrategy: Merge the topology sub-graphs found till now");
        topoSubGrphForOP = topology->mergeGraphs(topoSubGrphForOP);
    }

    NES_DEBUG("BottomUpStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;

    for (auto& sourceOperator : sourceOperators) {
        NES_DEBUG("BottomUpStrategy: Get the topology node for source operator " << sourceOperator->toString() << " placement.");
        SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
        std::vector<TopologyNodePtr> topologyNodes = mapOfSourceToTopologyNodes[sourceDescriptor->getStreamName()];
        if (topologyNodes.empty()) {
            NES_ERROR("BottomUpStrategy: unable to find topology nodes for logical source " << sourceDescriptor->getStreamName());
            throw QueryPlacementException("BottomUpStrategy: unable to find topology nodes for logical source " + sourceDescriptor->getStreamName());
        }

        TopologyNodePtr candidateTopologyNode = topologyNodes[0];
        topologyNodes.erase(topologyNodes.begin());
        if (candidateTopologyNode->getAvailableResources() == 0) {
            NES_ERROR("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
            throw QueryPlacementException("BottomUpStrategy: Unable to find resources on the physical node for placement of source operator");
        }
        placeOperator(queryId, sourceOperator, candidateTopologyNode, operatorToExecutionNodeMap);
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}

bool BottomUpStrategy::placeOperator(QueryId queryId, OperatorNodePtr candidateOperator, TopologyNodePtr candidateTopologyNode, std::map<uint64_t, ExecutionNodePtr>& operatorToExecutionNodeMap) {

    if (candidateTopologyNode->getAvailableResources() == 0) {
        NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
        while (!candidateTopologyNode->getParents().empty()) {
            candidateTopologyNode = candidateTopologyNode->getParents()[0]->as<TopologyNode>();
            if (candidateTopologyNode->getAvailableResources() > 0) {
                NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : " << candidateTopologyNode->getId());
                break;
            }
        }
    }

    if (!candidateTopologyNode || candidateTopologyNode->getAvailableResources() == 0) {
        NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
        throw QueryPlacementException("BottomUpStrategy: No node available for further placement of operators");
    }

    ExecutionNodePtr candidateExecutionNode;
    if (globalExecutionPlan->checkIfExecutionNodeExists(candidateTopologyNode->getId())) {
        NES_TRACE("BottomUpStrategy: node " << candidateTopologyNode->toString() << " was already used by other deployment");
        candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateTopologyNode->getId());
    } else {

        NES_DEBUG("BottomUpStrategy: create new execution node with id: " << candidateTopologyNode->getId());
        candidateExecutionNode = ExecutionNode::createExecutionNode(candidateTopologyNode);
        NES_DEBUG("BottomUpStrategy: Adding new execution node with id: " << candidateTopologyNode->getId());
        if (!globalExecutionPlan->addExecutionNode(candidateExecutionNode)) {
            NES_ERROR("BottomUpStrategy: failed to add execution node for query " << queryId);
            throw QueryPlacementException("BottomUpStrategy: failed to add execution node for query " + queryId);
        }
    }

    std::vector<QueryPlanPtr> queryPlansWithChildren;
    std::vector<QueryPlanPtr> querySubPlans = candidateExecutionNode->getQuerySubPlans(queryId);
    for (auto& child : candidateOperator->getChildren()) {
        auto found = std::find_if(querySubPlans.begin(), querySubPlans.end(), [&](QueryPlanPtr querySubPlan) {
            const vector<OperatorNodePtr> rootOperators = querySubPlan->getRootOperators();
            for (auto root : rootOperators) {
                if (root->as<OperatorNode>()->getId() == child->as<OperatorNode>()->getId()) {
                    return true;
                }
            }
            return false;
        });

        if (found != querySubPlans.end()) {
            queryPlansWithChildren.push_back(*found);
            querySubPlans.erase(found);
        }
    }

    const OperatorNodePtr copyOfCandidateOperator = candidateOperator->copy();
    if (queryPlansWithChildren.empty()) {
        NES_DEBUG("BottomUpStrategy: Adding a new query plan for the operator to an existing execution node");
        QueryPlanPtr querySubPlan = QueryPlan::create(copyOfCandidateOperator);
        querySubPlan->setQueryId(queryId);
        if (!candidateExecutionNode->createNewQuerySubPlan(queryId, querySubPlan)) {
            NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
            throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
        }
    } else {
        NES_DEBUG("BottomUpStrategy: Merging the Query plans");
        for (auto& queryPlan : queryPlansWithChildren) {
            for (auto& rootOperator : queryPlan->getRootOperators()) {
                copyOfCandidateOperator->addChild(rootOperator);
            }
        }
        QueryPlanPtr querySubPlan = QueryPlan::create(copyOfCandidateOperator);
        querySubPlan->setQueryId(queryId);
        querySubPlans.push_back(querySubPlan);
        if (!candidateExecutionNode->updateQuerySubPlans(queryId, querySubPlans)) {
            NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " << queryId);
            throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
        }
    }

    operatorToExecutionNodeMap[candidateOperator->getId()] = candidateExecutionNode;

    for (auto& parent : candidateOperator->getParents()) {
        OperatorNodePtr parentOperator = parent->as<OperatorNode>();
        if (parentOperator->isNAryOperator()) {
            std::vector<TopologyNodePtr> childTopologyNodes;
            vector<NodePtr> children = parentOperator->getChildren();
            for (auto& child : children) {
                const auto& found = operatorToExecutionNodeMap.find(child->as<OperatorNode>()->getId());
                if (found == operatorToExecutionNodeMap.end()) {
                    break;
                }
                TopologyNodePtr childPhysicalNode = found->second->getPhysicalNode();
                childTopologyNodes.push_back(childPhysicalNode);
            }
            if (childTopologyNodes.size() != children.size()) {
                continue;
            }
            candidateTopologyNode = topology->findCommonAncestor(childTopologyNodes);
        }
        placeOperator(queryId, parentOperator, candidateTopologyNode, operatorToExecutionNodeMap);
    }

    return true;
}

}// namespace NES
