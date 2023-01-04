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

#include <GRPC/WorkerRPCClient.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <algorithm>
#include <utility>

namespace NES {

TopologyNode::TopologyNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources)
    : id(id), ipAddress(std::move(ipAddress)), grpcPort(grpcPort), dataPort(dataPort), resources(resources), usedResources(0),
      maintenanceFlag(false), spatialType(Spatial::Index::Experimental::NodeType::NO_LOCATION) {}

TopologyNodePtr
TopologyNode::create(uint64_t id, const std::string& ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources) {
    NES_DEBUG("TopologyNode: Creating node with ID " << id << " and resources " << resources);
    return std::make_shared<TopologyNode>(id, ipAddress, grpcPort, dataPort, resources);
}

uint64_t TopologyNode::getId() const { return id; }

uint32_t TopologyNode::getGrpcPort() const { return grpcPort; }

uint32_t TopologyNode::getDataPort() const { return dataPort; }

uint16_t TopologyNode::getAvailableResources() const { return resources - usedResources; }

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
    copy->nodeProperties = nodeProperties;
    copy->reduceResources(usedResources);
    copy->linkProperties = this->linkProperties;
    copy->nodeProperties = this->nodeProperties;
    return copy;
}

std::string TopologyNode::getIpAddress() const { return ipAddress; }

std::string TopologyNode::toString() const {
    std::stringstream ss;
    ss << "PhysicalNode[id=" + std::to_string(id) + ", ip=" + ipAddress + ", resourceCapacity=" + std::to_string(resources)
            + ", usedResource=" + std::to_string(usedResources) + "]";
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

bool TopologyNode::hasNodeProperty(const std::string& key) {
    if (nodeProperties.find(key) == nodeProperties.end()) {
        return false;
    }
    return true;
}

std::any TopologyNode::getNodeProperty(const std::string& key) {
    if (nodeProperties.find(key) == nodeProperties.end()) {
        NES_ERROR("TopologyNode: Property '" << key << "'does not exist");
        NES_THROW_RUNTIME_ERROR("TopologyNode: Property '" << key << "'does not exist");
    } else {
        return nodeProperties.at(key);
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
    linkProperties.insert(std::make_pair(linkedNode->getId(), topologyLink));
}

LinkPropertyPtr TopologyNode::getLinkProperty(const TopologyNodePtr& linkedNode) {
    if (linkProperties.find(linkedNode->getId()) == linkProperties.end()) {
        NES_ERROR("TopologyNode: Link property with node '" << linkedNode->getId() << "' does not exist");
        NES_THROW_RUNTIME_ERROR("TopologyNode: Link property to node with id='" << linkedNode->getId() << "' does not exist");
    } else {
        return linkProperties.at(linkedNode->getId());
    }
}

bool TopologyNode::removeLinkProperty(const TopologyNodePtr& linkedNode) {
    if (linkProperties.find(linkedNode->getId()) == linkProperties.end()) {
        NES_ERROR("TopologyNode: Link property to node with id='" << linkedNode << "' does not exist");
        return false;
    }
    linkProperties.erase(linkedNode->getId());
    return true;
}

Spatial::Index::Experimental::WaypointPtr TopologyNode::getCoordinates() {
    std::string destAddress = ipAddress + ":" + std::to_string(grpcPort);
    switch (spatialType) {
        case Spatial::Index::Experimental::NodeType::MOBILE_NODE:
            NES_DEBUG("getting location data for mobile node with adress: " << destAddress)
            return WorkerRPCClient::getWaypoint(destAddress);
        case Spatial::Index::Experimental::NodeType::FIXED_LOCATION:
            return std::make_shared<Spatial::Index::Experimental::Waypoint>(fixedCoordinates);
        case Spatial::Index::Experimental::NodeType::NO_LOCATION:
            return std::make_shared<Spatial::Index::Experimental::Waypoint>(Spatial::Index::Experimental::Waypoint::invalid());
        case Spatial::Index::Experimental::NodeType::INVALID:
            NES_WARNING("trying to access location of a node with invalid spatial type")
            return std::make_shared<Spatial::Index::Experimental::Waypoint>(Spatial::Index::Experimental::Waypoint::invalid());
    }
}

NES::Spatial::Mobility::Experimental::ReconnectSchedulePtr TopologyNode::getReconnectSchedule() {
    if (spatialType == NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE) {
        std::string destAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("getting location data for mobile node with adress: " << destAddress)
        return WorkerRPCClient::getReconnectSchedule(destAddress);
    }
    return {};
}

void TopologyNode::setFixedCoordinates(double latitude, double longitude) {
    setFixedCoordinates(Spatial::Index::Experimental::Location(latitude, longitude));
}

void TopologyNode::setFixedCoordinates(Spatial::Index::Experimental::Location geoLoc) { fixedCoordinates = geoLoc; }

void TopologyNode::setSpatialNodeType(Spatial::Index::Experimental::NodeType workerSpatialType) {
    this->spatialType = workerSpatialType;
}

Spatial::Index::Experimental::NodeType TopologyNode::getSpatialNodeType() { return spatialType; }
}// namespace NES
