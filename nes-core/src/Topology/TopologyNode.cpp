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

#include "Plans/Global/Execution/ExecutionNode.hpp"
#include "Plans/Global/Execution/GlobalExecutionPlan.hpp"
#include <Common/Location.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Topology/TopologyNode.hpp>
#include <algorithm>
#include <utility>

namespace NES {

TopologyNode::TopologyNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources)
    : id(id), ipAddress(std::move(ipAddress)), grpcPort(grpcPort), dataPort(dataPort), resources(resources), usedResources(0),
      maintenanceFlag(false), isMobile(false) {}

TopologyNodePtr
TopologyNode::create(uint64_t id, const std::string& ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources) {
    NES_DEBUG("TopologyNode: Creating node with ID " << id << " and resources " << resources);
    return std::make_shared<TopologyNode>(id, ipAddress, grpcPort, dataPort, resources);
}

uint64_t TopologyNode::getId() const { return id; }

uint32_t TopologyNode::getGrpcPort() const { return grpcPort; }

uint32_t TopologyNode::getDataPort() const { return dataPort; }

uint16_t TopologyNode::getAvailableResources() const { return resources - usedResources; }

uint16_t TopologyNode::getUsedResources() const { return usedResources; }

uint16_t TopologyNode::getResourceCapacity() const { return resources; }

/*uint16_t TopologyNode::getEffectivelyUsedResources(GlobalExecutionPlan globalExecutionPlan, uint16_t queryId) const {

    //QueryId queryId1 =
    int highestChildCapacity = 0;
    int childCost = 0;



    if(this->getChildren().empty()){
        NES_INFO("This node has no children.")
        return this->usedResources;
    }

    for (auto& child : this->getChildren()){


            if (child->as<TopologyNode>()->getResourceCapacity() >= highestChildCapacity
            && globalExecutionPlan.getExecutionNodeByNodeId(child->as<TopologyNode>()->getId())->getOccupiedResources(queryId) > 0){
            highestChildCapacity = child->as<TopologyNode>()->getResourceCapacity();
            childCost = globalExecutionPlan.getExecutionNodeByNodeId(child->as<TopologyNode>()->getId())->getOccupiedResources(queryId);
        }
    }
    NES_INFO("HIGHEST CHILD CAP: " + std::to_string(highestChildCapacity) + ", RELATION: " + std::to_string(highestChildCapacity) + "/"
             + std::to_string(this->getResourceCapacity()) + " = " + std::to_string(highestChildCapacity / this->getResourceCapacity()))
    return (childCost * (highestChildCapacity / this->getResourceCapacity()));

}*/

bool TopologyNode::getMaintenanceFlag() const { return maintenanceFlag; };

void TopologyNode::setMaintenanceFlag(bool flag) { maintenanceFlag = flag; }

void TopologyNode::increaseResources(uint16_t freedCapacity) {
    NES_ASSERT(freedCapacity <= resources, "PhysicalNode: amount of resources to free can't be more than actual resources");
    NES_ASSERT(freedCapacity <= usedResources,
               "PhysicalNode: amount of resources to free can't be more than actual consumed resources");
    usedResources = usedResources - freedCapacity;
}

void TopologyNode::reduceResources(uint16_t usedCapacity) {
    NES_DEBUG("TopologyNode: Reducing resources " << usedCapacity << " of " << resources);
    if (usedCapacity > resources) {
        NES_WARNING("PhysicalNode: amount of resources to be used should not be more than actual resources");
    }

    if (usedCapacity > (resources - usedResources)) {
        NES_WARNING("PhysicalNode: amount of resources to be used should not be more than available resources");
    }
    usedResources = usedResources + usedCapacity;
}

TopologyNodePtr TopologyNode::copy() {
    TopologyNodePtr copy = std::make_shared<TopologyNode>(TopologyNode(id, ipAddress, grpcPort, dataPort, resources));
    copy->reduceResources(usedResources);
    return copy;
}

std::string TopologyNode::getIpAddress() const { return ipAddress; }

std::string TopologyNode::toString() const {

    std::stringstream ss;

    if(nodeProperties.contains("ressources")){
        ss << "PhysicalNode[id=" + std::to_string(id) + ", ip=" + ipAddress + ", resourceCapacity=" + std::to_string(resources)
                + ", usedResource=" + std::to_string(usedResources) + ", effectiveResources=" +
                std::to_string(std::get<1>(std::any_cast<std::tuple<int,float>>(nodeProperties.at("ressources")))) + "]";

        if(nodeProperties.contains("connectivity")){
            for(auto& entry : std::any_cast<std::map<int,int>>(nodeProperties.at("connectivity"))){
                ss << "\nLatency to node#" + std::to_string(entry.first) + ": " + std::to_string(entry.second) + "ms";
            }
        }
    }else{
        ss << "PhysicalNode[id=" + std::to_string(id) + ", ip=" + ipAddress + ", resourceCapacity=" + std::to_string(resources)
                + ", usedResource=" + std::to_string(usedResources) + "]";
    }

    return ss.str();
}

bool TopologyNode::containAsParent(NodePtr node) {
    std::vector<NodePtr> ancestors = this->getAndFlattenAllAncestors();
    auto found = std::find_if(ancestors.begin(), ancestors.end(), [node](const NodePtr& familyMember) {
        return familyMember->as<TopologyNode>()->getId() == node->as<TopologyNode>()->getId();
    });
    return found != ancestors.end();
}

bool TopologyNode::containAsChild(NodePtr node) {
    std::vector<NodePtr> children = this->getAndFlattenAllChildren(false);
    auto found = std::find_if(children.begin(), children.end(), [node](const NodePtr& familyMember) {
        return familyMember->as<TopologyNode>()->getId() == node->as<TopologyNode>()->getId();
    });
    return found != children.end();
}

void TopologyNode::addNodeProperty(const std::string& key, const std::any& value) {
    nodeProperties.insert(std::make_pair(key, value));
}

std::any TopologyNode::getNodeProperty(const std::string& key) {
    if (nodeProperties.find(key) == nodeProperties.end()) {
        NES_ERROR("TopologyNode: Property '" << key << "'does not exist");
        NES_THROW_RUNTIME_ERROR("TopologyNode: Property '" << key << "'does not exist");
    } else {
        return nodeProperties.at(key);
    }
}

bool TopologyNode::hasNodeProperty(const std::string& key){
    if(nodeProperties.contains(key)){
        return true;
    }else{
        return false;
    }
}

bool TopologyNode::removeNodeProperty(const std::string& key) {
    if (nodeProperties.find(key) == nodeProperties.end()) {
        NES_ERROR("TopologyNode: Property '" << key << "' does not exist");
        return false;
    }
    nodeProperties.erase(key);
    return true;
}
void TopologyNode::addLinkProperty(const TopologyNodePtr& linkedNode, const LinkPropertyPtr& topologyLink) {
    linkProperties.insert(std::make_pair(linkedNode, topologyLink));
}
LinkPropertyPtr TopologyNode::getLinkProperty(const TopologyNodePtr& linkedNode) {
    if (linkProperties.find(linkedNode) == linkProperties.end()) {
        NES_ERROR("TopologyNode: Link property with node '" << linkedNode->getId() << "' does not exist");
        NES_THROW_RUNTIME_ERROR("TopologyNode: Link property to node with id='" << linkedNode->getId() << "' does not exist");
    } else {
        return linkProperties.at(linkedNode);
    }
}
bool TopologyNode::removeLinkProperty(const TopologyNodePtr& linkedNode) {
    if (linkProperties.find(linkedNode) == linkProperties.end()) {
        NES_ERROR("TopologyNode: Link property to node with id='" << linkedNode << "' does not exist");
        return false;
    }
    linkProperties.erase(linkedNode);
    return true;
}

bool TopologyNode::isFieldNode() { return (bool) fixedCoordinates; }

Spatial::Index::Experimental::LocationPtr TopologyNode::getCoordinates() {
    if (isMobile) {
        std::string destAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("getting location data for mobile node with adress: " << destAddress)
        return WorkerRPCClient::getLocation(destAddress);
    }
    return fixedCoordinates;
}

void TopologyNode::addConnectivity(int nodeId, float connectivity) {
    if(this->connectivities.contains(nodeId)){
        this->connectivities.erase(nodeId);
    }

    this->connectivities.insert({nodeId, connectivity});
}

float TopologyNode::getConnectivity(int nodeId){
    return this->connectivities.find(nodeId)->second;
}

std::map<int,float> TopologyNode::getAllConnectivities(){
    return this->connectivities;
}


void TopologyNode::setFixedCoordinates(double latitude, double longitude) {
    setFixedCoordinates(Spatial::Index::Experimental::Location(latitude, longitude));
}

void TopologyNode::setFixedCoordinates(Spatial::Index::Experimental::Location geoLoc) {
    fixedCoordinates = std::make_shared<Spatial::Index::Experimental::Location>(geoLoc);
}

void TopologyNode::setMobile(bool isMobile) { this->isMobile = isMobile; }

bool TopologyNode::isMobileNode() { return isMobile; }

float TopologyNode::getEffectiveRessources() const { return effectiveResources; }

int TopologyNode::getLatency() const { return latency; }

int TopologyNode::getAvailableBuffers() const { return availableBuffers; }

int TopologyNode::getEffectiveLatency() const { return effectiveLatency; }

void TopologyNode::setAvailableBuffers(int availableBuffersSet){ this->availableBuffers = availableBuffersSet; }

void TopologyNode::increaseUsedBuffers(int usedBuffers){ this->availableBuffers -= usedBuffers; }

void TopologyNode::setEffectiveRessources(float effectiveRessourcesSet){ effectiveResources = effectiveRessourcesSet; }

void TopologyNode::setLatency(int latencySet){ latency = latencySet;}

void TopologyNode::setEffectiveLatency(float effectiveLatencySet) { effectiveLatency = effectiveLatencySet; }




}// namespace NES
