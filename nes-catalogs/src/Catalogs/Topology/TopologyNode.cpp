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
                           const std::string& ipAddress,
                           uint32_t grpcPort,
                           uint32_t dataPort,
                           uint16_t totalSlots,
                           const std::map<std::string, std::any>& properties)
    : workerId(workerId), ipAddress(ipAddress), grpcPort(grpcPort), dataPort(dataPort), totalSlots(totalSlots), occupiedSlots(0),
      nodeProperties(properties) {}

TopologyNodePtr TopologyNode::create(WorkerId workerId,
                                     const std::string& ipAddress,
                                     uint32_t grpcPort,
                                     uint32_t dataPort,
                                     uint16_t resources,
                                     const std::map<std::string, std::any>& properties) {
    NES_DEBUG("Creating node with ID {} and resources {}", workerId, resources);
    return std::make_shared<TopologyNode>(workerId, ipAddress, grpcPort, dataPort, resources, properties);
}

WorkerId TopologyNode::getId() const { return workerId; }

std::string TopologyNode::getGrpcAddress() const { return ipAddress + ":" + std::to_string(grpcPort); }

uint32_t TopologyNode::getGrpcPort() const { return grpcPort; }

uint32_t TopologyNode::getDataPort() const { return dataPort; }

uint16_t TopologyNode::getAvailableResources() const { return totalSlots - occupiedSlots; }

uint16_t TopologyNode::getTotalResources() const { return totalSlots; }

uint16_t TopologyNode::getOccupiedResources() const { return occupiedSlots; }

bool TopologyNode::isUnderMaintenance() { return std::any_cast<bool>(nodeProperties[NES::Worker::Properties::MAINTENANCE]); };

void TopologyNode::setForMaintenance(bool flag) { nodeProperties[NES::Worker::Properties::MAINTENANCE] = flag; }

bool TopologyNode::releaseSlots(uint16_t freedSlots) {
    NES_DEBUG("Releasing slots {} on topology node {}. Currently occupied {} of {}",
              freedSlots,
              workerId,
              occupiedSlots,
              totalSlots);
    NES_ASSERT(freedSlots <= totalSlots, "Amount of slots to free can't be more than actual resources");
    NES_ASSERT(freedSlots <= occupiedSlots, "Amount of slots to free can't be more than actual consumed resources");
    occupiedSlots = occupiedSlots - freedSlots;
    return true;
}

bool TopologyNode::occupySlots(uint16_t occupySlots) {
    NES_DEBUG("Reducing slots {} on topology node {}. Currently occupied {} of {}",
              occupySlots,
              workerId,
              occupiedSlots,
              totalSlots);
    if (occupySlots > (totalSlots - occupiedSlots)) {
        NES_WARNING("Amount of resources to be used should not be more than available resources.");
        return false;
    }
    occupiedSlots = occupiedSlots + occupySlots;
    return true;
}

TopologyNodePtr TopologyNode::copy() {

    //Copy the properties
    std::map<std::string, std::any> properties;
    for (const auto& [key, value] : nodeProperties) {
        properties[key] = value;
    }
    //Create the topologyNode
    TopologyNodePtr copy = std::make_shared<TopologyNode>(workerId, ipAddress, grpcPort, dataPort, totalSlots, properties);
    copy->occupySlots(occupiedSlots);
    //Copy the link properties
    for (const auto& [nodeId, linkProperty] : this->linkProperties) {
        copy->linkProperties[nodeId] = std::make_shared<LinkProperty>(linkProperty->bandwidth, linkProperty->latency);
    }
    return copy;
}

std::string TopologyNode::getIpAddress() const { return ipAddress; }

std::string TopologyNode::toString() const {
    return fmt::format("PhysicalNode[id={}, ip={}, resourceCapacity={}, usedResource={}]",
                       workerId,
                       ipAddress,
                       totalSlots,
                       occupiedSlots);
}

bool TopologyNode::containAsParent(NodePtr parentTopologyNode) {
    std::vector<NodePtr> ancestors = this->getAndFlattenAllAncestors();
    auto found = std::find_if(ancestors.begin(), ancestors.end(), [parentTopologyNode](const NodePtr& familyMember) {
        return familyMember->as<TopologyNode>()->getId() == parentTopologyNode->as<TopologyNode>()->getId();
    });
    return found != ancestors.end();
}

bool TopologyNode::containAsChild(NodePtr childTopologyNode) {
    std::vector<NodePtr> children = this->getAndFlattenAllChildren(false);
    auto found = std::find_if(children.begin(), children.end(), [childTopologyNode](const NodePtr& familyMember) {
        return familyMember->as<TopologyNode>()->getId() == childTopologyNode->as<TopologyNode>()->getId();
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
        NES_ERROR("Property '{}' does not exist", key);
        return std::any();
    } else {
        return nodeProperties.at(key);
    }
}

bool TopologyNode::removeNodeProperty(const std::string& key) {
    if (nodeProperties.find(key) == nodeProperties.end()) {
        NES_WARNING("Property '{}' does not exist", key);
        return false;
    }
    nodeProperties.erase(key);
    return true;
}

std::vector<WorkerId> TopologyNode::getAlternativeNodeCandidateIds() { return alternativeNodeIds; }

void TopologyNode::setAlternativeNodeCandidate(WorkerId node) { alternativeNodeIds.emplace_back(node); }

void TopologyNode::addLinkProperty(WorkerId linkedNodeId, const LinkPropertyPtr& topologyLink) {
    linkProperties[linkedNodeId] = topologyLink;
}

LinkPropertyPtr TopologyNode::getLinkProperty(WorkerId linkedNodeId) {
    if (!linkProperties.contains(linkedNodeId)) {
        NES_WARNING("Link property with node '{}' does not exist", linkedNodeId);
        return nullptr;
    } else {
        return linkProperties.at(linkedNodeId);
    }
}

bool TopologyNode::removeLinkProperty(WorkerId linkedNodeId) {
    if (!linkProperties.contains(linkedNodeId)) {
        NES_WARNING("Link property to node with id='{}' does not exist", linkedNodeId);
        return false;
    }
    linkProperties.erase(linkedNodeId);
    return true;
}

void TopologyNode::setSpatialType(NES::Spatial::Experimental::SpatialType spatialType) {
    nodeProperties[NES::Worker::Configuration::SPATIAL_SUPPORT] = spatialType;
}

Spatial::Experimental::SpatialType TopologyNode::getSpatialNodeType() {
    return std::any_cast<Spatial::Experimental::SpatialType>(nodeProperties[NES::Worker::Configuration::SPATIAL_SUPPORT]);
}
}// namespace NES
