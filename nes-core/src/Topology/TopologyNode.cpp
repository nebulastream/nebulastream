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

#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <algorithm>
#include <math.h>
#include <utility>

namespace NES {

TopologyNode::TopologyNode(uint64_t id,
                           std::string ipAddress,
                           uint32_t grpcPort,
                           uint32_t dataPort,
                           uint64_t resources,
                           uint64_t memoryCapacity,
                           uint64_t mtbfValue,
                           uint64_t launchTime,
                           uint64_t epochValue,
                           uint64_t ingestionRate,
                           uint64_t networkCapacity,
                           std::map<std::string, std::any> properties)
    : id(id), ipAddress(std::move(ipAddress)), grpcPort(grpcPort), dataPort(dataPort), resources(resources),
      initialMemoryCapacity(memoryCapacity), mtbfValue(mtbfValue), launchTime(launchTime),
      epochValue(epochValue), ingestionRate(ingestionRate), initialNetworkCapacity(networkCapacity), usedResources(0), usedMemory(0), usedNetwork(0), nodeProperties(std::move(properties)) {}

TopologyNodePtr TopologyNode::create(const uint64_t id,
                                     const std::string& ipAddress,
                                     const uint32_t grpcPort,
                                     const uint32_t dataPort,
                                     const uint64_t resources,
                                     std::map<std::string, std::any> properties,
                                     const uint64_t memoryCapacity,
                                     const uint64_t mtbfValue,
                                     const uint64_t launchTime,
                                     const uint64_t epochValue,
                                     const uint64_t ingestionRate,
                                     const uint64_t networkCapacity) {
    NES_DEBUG("TopologyNode: Creating node with ID " << id << " and resources " << resources);
    return std::make_shared<TopologyNode>(id,
                                          ipAddress,
                                          grpcPort,
                                          dataPort,
                                          resources,
                                          memoryCapacity,
                                          mtbfValue,
                                          launchTime,
                                          epochValue,
                                          ingestionRate,
                                          networkCapacity,
                                          properties);
}

uint64_t TopologyNode::getId() const { return id; }

uint32_t TopologyNode::getGrpcPort() const { return grpcPort; }

uint32_t TopologyNode::getDataPort() const { return dataPort; }

uint64_t TopologyNode::getAvailableResources() const { return resources - usedResources; }

double TopologyNode::getAvailableMemory() const { return initialMemoryCapacity - usedMemory; }

double TopologyNode::getAvailableNetwork() const { return initialNetworkCapacity - usedNetwork; }

uint64_t TopologyNode::getMTBFValue() const { return mtbfValue; }

uint64_t TopologyNode::getLaunchTime() const { return launchTime; }

uint64_t TopologyNode::getEpochValue() const { return epochValue; }

void TopologyNode::setEpochValue(uint64_t newEpochValue) { epochValue = newEpochValue; }

uint64_t TopologyNode::getIngestionRate() const { return ingestionRate; }

uint64_t TopologyNode::getInitialMemoryCapacity() const { return initialMemoryCapacity; }

uint64_t TopologyNode::getInitialNetworkCapacity() const { return initialNetworkCapacity; }

int TopologyNode::getResourcesUsed() { return usedResources; };

std::map<std::string, std::any> TopologyNode::getNodeProperties() { return nodeProperties; };

void TopologyNode::copyProperties(TopologyNodePtr node) {
    this->nodeProperties = node->getNodeProperties();
}

double TopologyNode::calculateReliability() const {
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::seconds>(now);
    auto epoch = now_ms.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::seconds>(epoch);
    double deviceRunningTime = (value.count() - launchTime) / 3600;
    return exp(-(deviceRunningTime / mtbfValue));
}

bool TopologyNode::isUnderMaintenance() { return std::any_cast<bool>(nodeProperties[NES::Worker::Properties::MAINTENANCE]); };

void TopologyNode::setForMaintenance(bool flag) { nodeProperties[NES::Worker::Properties::MAINTENANCE] = flag; }

void TopologyNode::increaseResources(int freedCapacity) {
    NES_ASSERT(freedCapacity <= resources, "PhysicalNode: amount of resources to free can't be more than actual resources");
    NES_ASSERT(freedCapacity <= usedResources,
               "PhysicalNode: amount of resources to free can't be more than actual consumed resources");
    usedResources = usedResources - freedCapacity;
}

void TopologyNode::reduceResources(int usedCapacity) {
//    NES_DEBUG("TopologyNode: Reducing resources " << usedCapacity << " of " << resources);
    if (usedCapacity > resources) {
        NES_WARNING("PhysicalNode: amount of resources to be used should not be more than actual resources");
    }

    if (usedCapacity > (resources - usedResources)) {
        NES_WARNING("PhysicalNode: amount of resources to be used should not be more than available resources");
    }
    usedResources = usedResources + usedCapacity;
}

void TopologyNode::reduceMemoryCapacity(double usedCapacity) {
//    NES_DEBUG("PhysicalNode: Reducing memory " << initialMemoryCapacity << " of " << usedMemory);
    if (usedCapacity > initialMemoryCapacity) {
        NES_WARNING("PhysicalNode: amount of memory to be used should not be more than actual memory");
    }
    if (usedCapacity > (initialMemoryCapacity - usedMemory)) {
        NES_WARNING("PhysicalNode: amount of memory to be used should not be more than available memory");
    }
    usedMemory = usedMemory + usedCapacity;
}

void TopologyNode::reduceNetworkCapacity(double usedCapacity) {
//    NES_DEBUG("PhysicalNode: Reducing network " << initialNetworkCapacity << " of " << usedNetwork);
    if (usedCapacity > initialNetworkCapacity) {
        NES_WARNING("PhysicalNode: amount of network to be used should not be more than actual network");
    }
    if (usedCapacity > (initialNetworkCapacity - usedNetwork)) {
        NES_WARNING("PhysicalNode: amount of network to be used should not be more than available network");
    }
    usedNetwork = usedNetwork + usedCapacity;
}

TopologyNodePtr TopologyNode::copy() {
    TopologyNodePtr copy = std::make_shared<TopologyNode>(TopologyNode(id,
                                                                       ipAddress,
                                                                       grpcPort,
                                                                       dataPort,
                                                                       resources,
                                                                       initialMemoryCapacity,
                                                                       mtbfValue,
                                                                       launchTime,
                                                                       epochValue,
                                                                       ingestionRate,
                                                                       initialNetworkCapacity,
                                                                       nodeProperties));
    copy->reduceResources(usedResources);
    copy->reduceMemoryCapacity(usedMemory);
    copy->reduceNetworkCapacity(usedNetwork);
    copy->linkProperties = this->linkProperties;
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

void TopologyNode::updateNodeProperty(const std::string& key, const std::any& value) {
    auto iter = nodeProperties.find(key);
    if (iter == nodeProperties.end()) {
        NES_ERROR("TopologyNode: Property '" << key << "'does not exist");
        NES_THROW_RUNTIME_ERROR("TopologyNode: Property '" << key << "'does not exist");
    } else {
        iter->second = value;
    }
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

void TopologyNode::setSpatialType(NES::Spatial::Experimental::SpatialType spatialType) {
    nodeProperties[NES::Worker::Configuration::SPATIAL_SUPPORT] = spatialType;
}

Spatial::Experimental::SpatialType TopologyNode::getSpatialNodeType() {
    return std::any_cast<Spatial::Experimental::SpatialType>(nodeProperties[NES::Worker::Configuration::SPATIAL_SUPPORT]);
}
}// namespace NES
