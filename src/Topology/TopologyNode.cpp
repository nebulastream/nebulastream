/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Topology/TopologyNode.hpp>
#include <algorithm>
#include <utility>

namespace NES {

TopologyNode::TopologyNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources)
    : id(id), ipAddress(std::move(ipAddress)), grpcPort(grpcPort), dataPort(dataPort), resources(resources), usedResources(0) {}

TopologyNodePtr
TopologyNode::create(uint64_t id, const std::string& ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources) {
    return std::make_shared<TopologyNode>(id, ipAddress, grpcPort, dataPort, resources);
}

uint64_t TopologyNode::getId() const { return id; }

uint32_t TopologyNode::getGrpcPort() const { return grpcPort; }

uint32_t TopologyNode::getDataPort() const { return dataPort; }

uint16_t TopologyNode::getAvailableResources() const { return resources - usedResources; }

bool TopologyNode::getMaintenanceFlag() const {return maintenanceFlag;};

void TopologyNode::setMaintenanceFlag(bool flag) {
    NES_DEBUG("Setting maintenance flag" );
    this->maintenanceFlag=flag;}

void TopologyNode::increaseResources(uint16_t freedCapacity) {
    NES_ASSERT(freedCapacity <= resources, "PhysicalNode: amount of resources to free can't be more than actual resources");
    NES_ASSERT(freedCapacity <= usedResources,
               "PhysicalNode: amount of resources to free can't be more than actual consumed resources");
    usedResources = usedResources - freedCapacity;
}

void TopologyNode::reduceResources(uint16_t usedCapacity) {
    NES_ASSERT(usedCapacity <= resources, "PhysicalNode: amount of resources to be used can't be more than actual resources");
    NES_ASSERT(usedCapacity <= (resources - usedResources),
               "PhysicalNode: amount of resources to be used can't be more than available resources");
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

}// namespace NES
