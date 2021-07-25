//
// Created by andre on 26.06.2021.
//

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>

#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/GradientStrategy.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<GradientStrategy> GradientStrategy::create(GlobalExecutionPlanPtr globalExecutionPlanPtr,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           StreamCatalogPtr streamCatalog) {
    return std::make_unique<GradientStrategy>(GradientStrategy(globalExecutionPlanPtr, topology, typeInferencePhase, streamCatalog));
}

GradientStrategy::GradientStrategy(GlobalExecutionPlanPtr globalExecutionPlanPtr,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlanPtr, topology, typeInferencePhase, streamCatalog) {}

bool GradientStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {
    const QueryId queryId = queryPlan->getQueryId();
    try {
        NES_INFO("GradientStrategy: Performing placement of the input query plan with id " << queryId);
        NES_INFO("GradientStrategy: And query plan \n" << queryPlan->toString());

        NES_DEBUG("GradientStrategy: Get all source operators");
        const std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
        if (sourceOperators.empty()) {
            NES_ERROR("GradientStrategy: No source operators found in the query plan wih id: " << queryId);
            throw Exception("GradientStrategy: No source operators found in the query plan wih id: " + std::to_string(queryId));
        }

        NES_DEBUG("GradientStrategy: map pinned operators to the physical location");
        mapPinnedOperatorToTopologyNodes(queryId, sourceOperators);

        NES_DEBUG("GradientStrategy: Get all sink operators");
        const std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();
        if (sinkOperators.empty()) {
            NES_ERROR("GradientStrategy: No sink operators found in the query plan wih id: " << queryId);
            throw Exception("GradientStrategy: No sink operators found in the query plan wih id: " + std::to_string(queryId));
        }

        NES_DEBUG("GradientStrategy: place query plan with id : " << queryId);
        placeQueryPlanOnTopology(queryPlan);

        NES_DEBUG("GradientStrategy: clear the temporary map : " << queryId);
        operatorToExecutionNodeMap.clear();
        pinnedOperatorLocationMap.clear();

        NES_DEBUG("GradientStrategy: Run type inference phase for query plans in global execution plan for query with id : "
                  << queryId);

        NES_DEBUG("GradientStrategy: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());
        return runTypeInferencePhase(queryId);
    } catch (Exception& ex) {
        throw QueryPlacementException(queryId, ex.what());
    }
}

void GradientStrategy::placeQueryPlanOnTopology(const QueryPlanPtr& queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    NES_DEBUG("GradientStrategy: Get the all source operators for performing the placement.");
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();

    std::map<int, std::vector<int>> assignedOperators;

    for (auto& sourceOperator : sourceOperators) {
        NES_DEBUG("GradientStrategy: Get the topology node for source operator " << sourceOperator->toString() << " placement.");
        std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        TopologyNodePtr sourceTopologyNode = streamCatalog->getSourceNodesForLogicalStream(streamName)[0];
        std::vector<NodePtr> operatorPath = findPathToRoot(sourceOperator);

        std::vector<NodePtr> topologyPath = findPathToRoot(sourceTopologyNode);

        torch::Tensor finalizedMatrix = computePlacement(operatorPath, topologyPath);
        std::map<int, std::vector<int>> map = operatorAssignment(operatorPath, topologyPath, finalizedMatrix);
        assignedOperators = modifyMapping(assignedOperators, map);

        //assignedOperators.insert(map.begin(), map.end());
        reduceResources(topologyPath, torch::sum(finalizedMatrix, 1));
    }

    finalizePlacement(queryPlan, assignedOperators);
}

/**
 * assuming each node has only one parent, a path to the root of the topology is found
 * credit to: Juliane Verwiebe and Robert Gericke
 * @param sourceNode source operator or source topology node
 * @return array containing all nodes on path from source to sink or parent topology node
 */
std::vector<NodePtr> GradientStrategy::findPathToRoot(NodePtr sourceNode) {
    std::vector<NodePtr> path;
    path.push_back(sourceNode);
    while (!path.back()->getParents().empty()) {
        path.push_back(path.back()->getParents()[0]);// assuming single parent.
    }
    return path;
}


torch::Tensor GradientStrategy::computePlacement(std::vector<NodePtr> operators, std::vector<NodePtr> nodes) {
    int width = (int)nodes.size();
    int height = (int)operators.size();
    const int epochs = 15;
    torch::Tensor finalizedMatrix = torch::zeros({height, width});
    torch::Tensor assignmentMatrix = torch::rand_like(finalizedMatrix);
    torch::Tensor operatorCostMatrix = assignCostParameter(operators);

    assignmentMatrix.requires_grad_(true);
    std::vector<torch::Tensor> vec;
    vec.push_back(assignmentMatrix);

    torch::optim::Adam optimizer = torch::optim::Adam(vec, {0.1});
    for (int i = 0; i < height-2; ++i) {
        for (int j = 0; j < epochs ; ++j) {
            torch::Tensor loss = operatorPlacement(assignmentMatrix, operatorCostMatrix, finalizedMatrix, nodes, i+1);
            optimizer.zero_grad();
            loss.backward();
            torch::nn::utils::clip_grad_value_(assignmentMatrix, 1.0);
            optimizer.step();
        }
        finalizedMatrix[i+1] = finalize(assignmentMatrix[i+1]);
    }
    return finalizedMatrix;
}

torch::Tensor GradientStrategy::operatorPlacement(torch::Tensor assignmentMatrix, torch::Tensor operatorCostMatrix,
                                                  torch::Tensor finalizedMatrix, std::vector<NodePtr> nodes, int opIndex) {

    long connections = nodes.size() - 1;
    torch::Tensor opCost = operatorCostMatrix[opIndex];
    torch::Tensor prevCost = torch::prod(operatorCostMatrix.slice(0, 0, opIndex));

    torch::Tensor capacity = torch::zeros(nodes.size());

    for (unsigned long i = 0; i < nodes.size(); ++i) {
        capacity[i] = nodes[i]->as<TopologyNode>()->getAvailableResources();
    }

    torch::Tensor usedCap = torch::sum(finalizedMatrix.slice(0, 1, -1));

    torch::Tensor cost = torch::zeros(1);
    for (int i = 0; i < connections + 1; ++i) {
        cost = cost + (((connections - i) * opCost + i * prevCost) * assignmentMatrix.index({opIndex, i}).square()) +
            (connections - torch::sum(assignmentMatrix[opIndex]) + assignmentMatrix.index({opIndex, i}));
    }

    torch::Tensor bound = torch::sqrt((torch::sum(assignmentMatrix[opIndex]) - 1).square());
    int startIndex = torch::where(finalizedMatrix[opIndex - 1] > 0.9)[0].item<int>();

    torch::Tensor validity = torch::sum(torch::slice(assignmentMatrix[opIndex], 0, 0, startIndex).square());

    torch::Tensor capBound = torch::sqrt(assignmentMatrix[opIndex].square()) + usedCap + capacity;
    capBound[capBound < 0] = 0;

    return cost*4 + bound*5 + validity*20 + torch::sum(capBound)*10;
}


at::Tensor GradientStrategy::finalize(at::Tensor tensor) {
    if ( (tensor > 0.85).nonzero().size(0) == 1) {
        return (tensor > 0.85).int_repr();
    } return (tensor == tensor.max()).int_repr();

}

torch::Tensor GradientStrategy::assignCostParameter(std::vector<NodePtr> vector) {
    torch::Tensor costTensor = torch::zeros(vector.size());

    for (int i = 0; i < (int64_t) vector.size(); ++i) {
        LogicalOperatorNodePtr logicalNode = vector.at(i)->as<LogicalOperatorNode>();

        /**
         * Credit to Juliane Verwiebe and Robert Gericke
         */
        if (logicalNode->instanceOf<SourceLogicalOperatorNode>() || logicalNode->instanceOf<SinkLogicalOperatorNode>()) {
            costTensor[i] = 1;
        } else if (logicalNode->instanceOf<FilterLogicalOperatorNode>()) {
            costTensor[i] = 0.5;
        } else if (logicalNode->instanceOf<JoinLogicalOperatorNode>()) {
            costTensor[i] = 2;
        } else if (logicalNode->instanceOf<MapLogicalOperatorNode>()) {
            costTensor[i] = 2;
        } else if (logicalNode->instanceOf<UnionLogicalOperatorNode>()) {
            costTensor[i] = 2;
        } else if (logicalNode->instanceOf<ProjectionLogicalOperatorNode>()) {
            costTensor[i] = 1;
        }
    }

    return costTensor;
}

void GradientStrategy::reduceResources(std::vector<NodePtr> nodes, at::Tensor placementTensor) {
    for (size_t i = 0; i < nodes.size(); ++i) {
        int res = placementTensor[i].item<int>();
        nodes[i]->as<TopologyNode>()->reduceResources(res);
    }
}

std::map<int, std::vector<int>>
GradientStrategy::operatorAssignment(std::vector<NodePtr> operators, std::vector<NodePtr> nodes, torch::Tensor finalizedMatrix) {

    std::map<int, std::vector<int>> operatorToTopologyNodeMap;
    for (int i = 0; i < finalizedMatrix.size(0); ++i) {
        OperatorNodePtr operatorNode = operators[i]->as<OperatorNode>();
        TopologyNodePtr topologyNode = nodes[(finalizedMatrix[i] == 1).nonzero()[0].item<int>()]->as<TopologyNode>();

        std::vector<int> operatorVector;
        operatorVector.push_back(operatorNode->getId());

        operatorToTopologyNodeMap.insert(std::make_pair(topologyNode->getId(), operatorVector));
    }

    return operatorToTopologyNodeMap;
}

/**
 * partial credit to Juliane Verwiebe and Robert Gericke
 * @param queryPlan
 * @param assignedOperators
 */
void GradientStrategy::finalizePlacement(const QueryPlanPtr& queryPlan, std::map<int, std::vector<int>> assignedOperators) {
    auto topologyIterator = NES::DepthFirstNodeIterator(topology->getRoot()).begin();
    std::vector<std::vector<bool>> binaryMapping;

    while (topologyIterator != DepthFirstNodeIterator::end()){
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        std::string topologyID = std::to_string(currentTopologyNode->getId());

        std::vector<int> operators = assignedOperators.at(currentTopologyNode->getId());
        std::vector<bool> tmp;

        auto queryPlanIterator = NES::QueryPlanIterator(queryPlan);

        for (auto&& op: queryPlanIterator){
            OperatorNodePtr operatorNode = op->as<OperatorNode>();
            if (std::find(operators.begin(), operators.end(), operatorNode->getId()) != operators.end()) {
                tmp.push_back(true);
            } else {
                tmp.push_back(false);
            }
        }
        binaryMapping.push_back(tmp);
        ++topologyIterator;
    }

    assignMappingToTopology(topology, queryPlan, binaryMapping);
    addNetworkSourceAndSinkOperators(queryPlan);
}
std::map<int, std::vector<int>> GradientStrategy::modifyMapping(std::map<int, std::vector<int>> assignedOperators,
                                                                std::map<int, std::vector<int>> input) {
    for (auto const& i : input){
        auto item = assignedOperators.find(i.first);
        if (item != assignedOperators.end()){
            item->second.insert(item->second.end(), i.second.begin(), i.second.end());
        }
    }

    return assignedOperators;
}

}
