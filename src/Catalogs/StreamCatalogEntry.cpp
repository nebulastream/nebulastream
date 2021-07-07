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

#include <Catalogs/StreamCatalogEntry.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

StreamCatalogEntry::StreamCatalogEntry(std::string sourceType,
                                       std::string physicalStreamName,
                                       std::vector<std::string> logicalStreamName,
                                       TopologyNodePtr node)
    : sourceType(sourceType), physicalStreamName(physicalStreamName), logicalStreamName(logicalStreamName), node(node) {}

StreamCatalogEntry::StreamCatalogEntry(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node)
    : sourceType(config->getSourceType()), physicalStreamName(config->getPhysicalStreamName()),
      logicalStreamName(config->getLogicalStreamName()), node(node) {
    // nop
}

StreamCatalogEntryPtr StreamCatalogEntry::create(std::string sourceType,
                                                 std::string physicalStreamName,
                                                 std::vector<std::string> logicalStreamName,
                                                 TopologyNodePtr node) {
    return std::make_shared<StreamCatalogEntry>(sourceType, physicalStreamName, logicalStreamName, node);
}

StreamCatalogEntryPtr StreamCatalogEntry::create(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node) {
    return std::make_shared<StreamCatalogEntry>(config, node);
}

std::string StreamCatalogEntry::getSourceType() { return sourceType; }

TopologyNodePtr StreamCatalogEntry::getNode() { return node;}

std::string StreamCatalogEntry::getPhysicalName() { return physicalStreamName; }

std::vector<std::string> StreamCatalogEntry::getLogicalName() { return logicalStreamName; }

bool StreamCatalogEntry::addLogicalStreamName(std::string newLogicalStreamName){
    for(std::string name : logicalStreamName){
        if(name==newLogicalStreamName){
            NES_ERROR("StreamCatalogEntry::addLogicalStreamName logicalStreamName="<<newLogicalStreamName<<" could not be added to StreamCatalogEntry as it already exists");
            return false;
        }
    }
    logicalStreamName.push_back(newLogicalStreamName);
    return true;
}

std::string StreamCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << physicalStreamName << " logicalStreamName=(" << UtilityFunctions::combineStringsWithDelimiter(logicalStreamName,",") << ") sourceType=" << sourceType
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}
}// namespace NES
