#include <Optimizer/NESExecutionPlan.hpp>
#include <Optimizer/ExecutionGraph.hpp>
#include <Util/Logger.hpp>
#include <Topology/NESTopologySensorNode.hpp>

using namespace NES;
using namespace web;

NESExecutionPlan::NESExecutionPlan() {
    exeGraphPtr = std::make_shared<ExecutionGraph>();
    totalComputeTimeInMillis = 0;
}

ExecutionNodePtr NESExecutionPlan::getRootNode() const {
    return exeGraphPtr->getRoot();
};

ExecutionNodePtr NESExecutionPlan::createExecutionNode(std::string operatorName, std::string nodeName,
                                                       NESTopologyEntryPtr nesNode, OperatorPtr executableOperator) {

    NES_DEBUG(
        "NESExecutionPlan::createExecutionNode operatorName=" << operatorName << " nodeName=" << nodeName << " nesNode="
                                                              << nesNode << " executableOperaor=" << executableOperator)
    auto ptr = std::make_shared<ExecutionNode>(operatorName, nodeName, nesNode, executableOperator);
    exeGraphPtr->addVertex(ptr);
    return ptr;
};

bool NESExecutionPlan::hasVertex(int searchId) {
    return exeGraphPtr->hasVertex(searchId);
}

ExecutionNodePtr NESExecutionPlan::getExecutionNode(int searchId) {
    return exeGraphPtr->getNode(searchId);
}

json::value NESExecutionPlan::getExecutionGraphAsJson() const {

    const ExecutionNodePtr rootNode = getRootNode();
    const ExecutionGraphPtr exeGraph = getExecutionGraph();

    const std::vector<ExecutionEdge>& allEdges = exeGraph->getAllEdges();
    const std::vector<ExecutionVertex>& allVertex = exeGraph->getAllVertex();

    auto result = json::value::object();
    std::vector<json::value> edges{};
    std::vector<json::value> vertices{};
    for (u_int i = 0; i < allEdges.size(); i++) {
        const ExecutionEdge& edge = allEdges[i];
        const ExecutionNodePtr& sourceNode = edge.ptr->getSource();
        const ExecutionNodePtr& destNode = edge.ptr->getDestination();
        auto edgeInfo = json::value::object();
        const auto source = "Node-" + std::to_string(sourceNode->getId());
        const auto dest = "Node-" + std::to_string(destNode->getId());
        edgeInfo["source"] = json::value::string(source);
        edgeInfo["target"] = json::value::string(dest);
        edgeInfo["linkCapacity"] = json::value::string(
            std::to_string(edge.ptr->getLinkCapacity()));
        edgeInfo["linkLatency"] = json::value::string(
            std::to_string(edge.ptr->getLinkLatency()));
        edges.push_back(edgeInfo);
    }

    for (u_int i = 0; i < allVertex.size(); i++) {
        const ExecutionVertex& vertex = allVertex[i];
        auto vertexInfo = json::value::object();
        const ExecutionNodePtr& executionNodePtr = vertex.ptr;
        const std::string id = "Node-" + std::to_string(executionNodePtr->getId());
        const std::string& operatorName = executionNodePtr->getOperatorName();
        const std::string nodeType = executionNodePtr->getNESNode()->getEntryTypeString();

        int cpuCapacity = vertex.ptr->getNESNode()->getCpuCapacity();
        int remainingCapacity = vertex.ptr->getNESNode()->getRemainingCpuCapacity();

        vertexInfo["id"] = json::value::string(id);
        vertexInfo["capacity"] = json::value::string(std::to_string(cpuCapacity));
        vertexInfo["remainingCapacity"] = json::value::string(
            std::to_string(remainingCapacity));
        vertexInfo["operators"] = json::value::string(operatorName);
        vertexInfo["nodeType"] = json::value::string(nodeType);
        if (nodeType == "Sensor") {
            NESTopologySensorNodePtr ptr = std::static_pointer_cast<NESTopologySensorNode>(vertex.ptr->getNESNode());
            vertexInfo["physicalStreamName"] = json::value::string(ptr->getPhysicalStreamName());
        }
        vertices.push_back(vertexInfo);
    }

    result["nodes"] = json::value::array(vertices);
    result["edges"] = json::value::array(edges);
    return result;
}

std::vector<json::value> NESExecutionPlan::getChildrenNode(
    ExecutionNodePtr executionParentNode) const {

    const std::shared_ptr<ExecutionGraph>& exeGraph = getExecutionGraph();

    const std::vector<ExecutionEdge>& edgesToNode = exeGraph->getAllEdgesToNode(executionParentNode);

    std::vector<json::value> children;

    if (edgesToNode.empty()) {
        return {};
    }

    for (ExecutionEdge edge : edgesToNode) {
        const ExecutionNodePtr& sourceNode = edge.ptr->getSource();
        if (sourceNode) {
            auto child = json::value::object();
            const auto label = std::to_string(sourceNode->getId()) + "-"
                + sourceNode->getOperatorName();
            child["name"] = json::value::string(label);
            const std::vector<json::value>& grandChildren = getChildrenNode(
                sourceNode);
            if (!grandChildren.empty()) {
                child["children"] = json::value::array(grandChildren);
            }
            children.push_back(child);
        }
    }
    return children;
}

ExecutionNodeLinkPtr NESExecutionPlan::createExecutionNodeLink(
    size_t id, ExecutionNodePtr sourceNode, ExecutionNodePtr destinationNode,
    size_t linkCapacity, size_t linkLatency) {

    NES_DEBUG(
        "NESExecutionPlan::createExecutionNodeLink: create link id=" << id
                                                                     << " sourceNodeId=" << sourceNode->getId()
                                                                     << " destinationNodeId="
                                                                     << destinationNode->getId() << " linkcapacity="
                                                                     << linkCapacity
                                                                     << " linkLatency=" << linkLatency)
    // check if link already exists
    if (exeGraphPtr->hasLink(sourceNode, destinationNode)) {
        // return already existing link
        return exeGraphPtr->getLink(sourceNode, destinationNode);
    }

    // create new link
    auto linkPtr = std::make_shared<ExecutionNodeLink>(id, sourceNode,
                                                       destinationNode,
                                                       linkCapacity, linkLatency);
    assert(exeGraphPtr->addEdge(linkPtr));
    return linkPtr;
};

std::shared_ptr<ExecutionGraph> NESExecutionPlan::getExecutionGraph() const {
    return exeGraphPtr;
};

void NESExecutionPlan::freeResources() {
    for (const ExecutionVertex& v : getExecutionGraph()->getAllVertex()) {
        if (v.ptr->getRootOperator()) {
            // TODO: change that when proper placement is fixed
            NES_INFO(
                "NESEXECUTIONPLAN: Capacity before-" << v.ptr->getNESNode()->getId() << "->"
                                                     << v.ptr->getNESNode()->getRemainingCpuCapacity())
            int usedCapacity = v.ptr->getNESNode()->getCpuCapacity()
                - v.ptr->getNESNode()->getRemainingCpuCapacity();
            v.ptr->getNESNode()->increaseCpuCapacity(usedCapacity);
            NES_INFO(
                "NESEXECUTIONPLAN: Capacity after-" << v.ptr->getNESNode()->getId() << "->"
                                                    << v.ptr->getNESNode()->getRemainingCpuCapacity())
        }
    }
}

std::string NESExecutionPlan::getTopologyPlanString() const {
    return exeGraphPtr->getGraphString();
}

long NESExecutionPlan::getTotalComputeTimeInMillis() const {
    return totalComputeTimeInMillis;
}

void NESExecutionPlan::setTotalComputeTimeInMillis(long totalComputeTime) {
    NESExecutionPlan::totalComputeTimeInMillis = totalComputeTime;
}
