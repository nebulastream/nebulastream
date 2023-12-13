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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <algorithm>
#include <utility>

namespace NES {

TopologyNode::TopologyNode(WorkerId workerId,
                           std::string  ipAddress,
                           uint32_t grpcPort,
                           uint32_t dataPort,
                           uint16_t resources,
                           std::map<std::string, std::any> properties)
    : workerId(workerId), ipAddress(std::move(ipAddress)), grpcPort(grpcPort), dataPort(dataPort), resources(resources), usedResources(0),
      locked(false), nodeProperties(std::move(properties)) {}

TopologyNodePtr TopologyNode::create(WorkerId workerId,
                                     const std::string& ipAddress,
                                     uint32_t grpcPort,
                                     uint32_t dataPort,
                                     uint16_t resources,
                                     std::map<std::string, std::any> properties) {
    NES_DEBUG("TopologyNode: Creating node with ID {} and resources {}", workerId, resources);
    return std::make_shared<TopologyNode>(workerId, ipAddress, grpcPort, dataPort, resources, std::move(properties));
}

WorkerId TopologyNode::getId() const { return workerId; }

uint32_t TopologyNode::getGrpcPort() const { return grpcPort; }

uint32_t TopologyNode::getDataPort() const { return dataPort; }

uint16_t TopologyNode::getAvailableResources() const { return resources - usedResources; }

bool TopologyNode::isUnderMaintenance() { return std::any_cast<bool>(nodeProperties[NES::Worker::Properties::MAINTENANCE]); };

void TopologyNode::setForMaintenance(bool flag) { nodeProperties[NES::Worker::Properties::MAINTENANCE] = flag; }

void TopologyNode::increaseResources(uint16_t freedCapacity) {
    NES_ASSERT(freedCapacity <= resources, "PhysicalNode: amount of resources to free can't be more than actual resources");
    NES_ASSERT(freedCapacity <= usedResources,
               "PhysicalNode: amount of resources to free can't be more than actual consumed resources");
    usedResources = usedResources - freedCapacity;
}

void TopologyNode::reduceResources(uint16_t usedCapacity) {
    NES_DEBUG("TopologyNode: Reducing resources {} Currently occupied {} of {}", usedCapacity, usedResources, resources);
    if (usedCapacity > resources) {
        NES_WARNING("PhysicalNode: amount of resources to be used should not be more than actual resources");
    }

    if (usedCapacity > (resources - usedResources)) {
        NES_WARNING("PhysicalNode: amount of resources to be used should not be more than available resources");
    }
    usedResources = usedResources + usedCapacity;
}

TopologyNodePtr TopologyNode::copy() {
    TopologyNodePtr copy =
        std::make_shared<TopologyNode>(workerId, ipAddress, grpcPort, dataPort, resources, nodeProperties);
    copy->reduceResources(usedResources);
    copy->linkProperties = this->linkProperties;
    return copy;
}

std::string TopologyNode::getIpAddress() const { return ipAddress; }

std::string TopologyNode::toString() const {
    std::stringstream ss;
    ss << "PhysicalNode[id=" + std::to_string(workerId) + ", ip=" + ipAddress + ", resourceCapacity=" + std::to_string(resources)
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
        NES_ERROR("TopologyNode: Property '{}' does not exist", key);
        NES_THROW_RUNTIME_ERROR("TopologyNode: Property '" << key << "'does not exist");
    } else {
        return nodeProperties.at(key);
    }
}

bool TopologyNode::removeNodeProperty(const std::string& key) {
    if (nodeProperties.find(key) == nodeProperties.end()) {
        NES_ERROR("TopologyNode:  Property '{}' does not exist", key);
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
        NES_ERROR("TopologyNode: Link property with node '{}' does not exist", linkedNode->getId());
        NES_THROW_RUNTIME_ERROR("TopologyNode: Link property to node with id='" << linkedNode->getId() << "' does not exist");
    } else {
        return linkProperties.at(linkedNode->getId());
    }
}

bool TopologyNode::removeLinkProperty(const TopologyNodePtr& linkedNode) {
    if (linkProperties.find(linkedNode->getId()) == linkProperties.end()) {
        NES_ERROR("TopologyNode: Link property to node with id='{}' does not exist", linkedNode->getId());
        return false;
    }
    linkProperties.erase(linkedNode->getId());
    return true;
}

void TopologyNode::setSpatialType(NES::Spatial::Experimental::SpatialType spatialType) {
    nodeProperties[NES::Worker::Configuration::SPATIAL_SUPPORT] = spatialType;
}

Spatial::Experimental::SpatialType TopologyNode::getSpatialNodeType() {
    return std::any_cast<Spatial::Experimental::SpatialType>(nodeProperties[NES::Worker::Configuration::SPATIAL_SUPPORT]);
}

bool TopologyNode::acquireLock() {
    if (!locked) {
        locked = true;
        return true;
    }
    return false;
}

bool TopologyNode::releaseLock() {
    if (locked) {
        locked = false;
        return true;
    }
    return false;
}

}// namespace NES
