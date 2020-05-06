#include <Topology/NESTopologyManager.hpp>
#include <Util/Logger.hpp>
namespace NES {

NESTopologyCoordinatorNodePtr NESTopologyManager::createNESCoordinatorNode(size_t id,
                                                                           const std::string ipAddr,
                                                                           CPUCapacity cpuCapacity) {
    assert(0);
    return currentPlan->createNESCoordinatorNode(id, ipAddr, cpuCapacity);
}

NESTopologyWorkerNodePtr NESTopologyManager::createNESWorkerNode(
    size_t id, const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createNESWorkerNode(id, ipAddr, cpuCapacity);
}

NESTopologySensorNodePtr NESTopologyManager::createNESSensorNode(size_t id, std::string ip,
                                                                 CPUCapacity cpuCapacity) {
    return currentPlan->createNESSensorNode(id, ip, cpuCapacity);
}

bool NESTopologyManager::removeNESWorkerNode(NESTopologyWorkerNodePtr ptr) {
    return currentPlan->removeNESWorkerNode(std::move(ptr));
}

bool NESTopologyManager::removeNESSensorNode(NESTopologySensorNodePtr ptr) {
    return currentPlan->removeNESSensorNode(std::move(ptr));
}

bool NESTopologyManager::removeNESNode(NESTopologyEntryPtr ptr) {
    return currentPlan->removeNESNode(std::move(ptr));
}

NESTopologyLinkPtr NESTopologyManager::createNESTopologyLink(NESTopologyEntryPtr sourceNode,
                                                             NESTopologyEntryPtr destNode,
                                                             size_t linkCapacity,
                                                             size_t linkLatency) {
    return currentPlan->createNESTopologyLink(sourceNode, destNode, linkCapacity, linkLatency);
}

bool NESTopologyManager::removeNESTopologyLink(NESTopologyLinkPtr linkPtr) {
    return currentPlan->removeNESTopologyLink(linkPtr);
}

void NESTopologyManager::printNESTopologyPlan() {
    NES_DEBUG(getNESTopologyPlanString());
}

std::string NESTopologyManager::getNESTopologyPlanString() {
    return currentPlan->getTopologyPlanString();
}

NESTopologyPlanPtr NESTopologyManager::getNESTopologyPlan() {
    return currentPlan;
}

NESTopologyEntryPtr NESTopologyManager::getRootNode() {
    return currentPlan->getRootNode();
};

json::value NESTopologyManager::getNESTopologyGraphAsJson() {

    const NESTopologyGraphPtr& nesGraphPtr = getNESTopologyPlan()->getNESTopologyGraph();
    const std::vector<NESTopologyLinkPtr>& allEdges = nesGraphPtr->getAllEdges();
    const std::vector<NESVertex>& allVertex = nesGraphPtr->getAllVertex();

    auto result = json::value::object();
    std::vector<json::value> edges{};
    std::vector<json::value> vertices{};
    for (u_int i = 0; i < allEdges.size(); i++) {
        const NESTopologyLinkPtr& edge = allEdges[i];
        const NESTopologyEntryPtr& sourceNode = edge->getSourceNode();
        const NESTopologyEntryPtr& destNode = edge->getDestNode();
        auto edgeInfo = json::value::object();
        const auto source = "Node-" + std::to_string(sourceNode->getId());
        const auto dest = "Node-" + std::to_string(destNode->getId());
        edgeInfo["source"] = json::value::string(source);
        edgeInfo["target"] = json::value::string(dest);
        edgeInfo["linkCapacity"] = json::value::string(std::to_string(edge->getLinkCapacity()));
        edgeInfo["linkLatency"] = json::value::string(std::to_string(edge->getLinkLatency()));
        edges.push_back(edgeInfo);
    }

    for (u_int i = 0; i < allVertex.size(); i++) {
        const NESVertex& vertex = allVertex[i];
        auto vertexInfo = json::value::object();
        const std::string id = "Node-" + std::to_string(vertex.ptr->getId());
        const std::string nodeType = vertex.ptr->getEntryTypeString();
        int cpuCapacity = vertex.ptr->getCpuCapacity();
        int remainingCapacity = vertex.ptr->getRemainingCpuCapacity();

        vertexInfo["id"] = json::value::string(id);
        vertexInfo["title"] = json::value::string(id);
        vertexInfo["nodeType"] = json::value::string(nodeType);
        vertexInfo["capacity"] = json::value::string(std::to_string(cpuCapacity));
        vertexInfo["remainingCapacity"] = json::value::string(
            std::to_string(remainingCapacity));

        if (nodeType == "Sensor") {
            NESTopologySensorNodePtr ptr = std::static_pointer_cast<
                NESTopologySensorNode>(vertex.ptr);
            vertexInfo["physicalStreamName"] = json::value::string(
                ptr->getPhysicalStreamName());
        }

        vertices.push_back(vertexInfo);
    }

    result["nodes"] = json::value::array(vertices);
    result["edges"] = json::value::array(edges);
    return result;
}

std::vector<json::value> NESTopologyManager::getChildrenNodes(
    NESTopologyEntryPtr nesParentNode) {

    const NESTopologyGraphPtr& nesGraphPtr = getNESTopologyPlan()->getNESTopologyGraph();
    const std::vector<NESTopologyLinkPtr>& edgesToNode = nesGraphPtr
                                                             ->getAllEdgesToNode(nesParentNode);

    std::vector<json::value> children = {};

    if (edgesToNode.empty()) {
        return {};
    }

    for (NESTopologyLinkPtr edge : edgesToNode) {
        const NESTopologyEntryPtr& sourceNode = edge->getSourceNode();
        if (sourceNode) {
            auto child = json::value::object();
            const auto label = std::to_string(sourceNode->getId()) + "-"
                + sourceNode->getEntryTypeString();
            child["id"] = json::value::string(label);
            const std::vector<json::value>& grandChildren = getChildrenNodes(
                sourceNode);
            if (!grandChildren.empty()) {
                child["children"] = json::value::array(grandChildren);
            }
            children.push_back(child);
        }
    }
    return children;
}

void NESTopologyManager::resetNESTopologyPlan() {
    currentPlan.reset(new NESTopologyPlan());
    StreamCatalog::instance().reset();
}

}// namespace NES
