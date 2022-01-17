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
#include <utility>

namespace NES {

StreamCatalogEntry::StreamCatalogEntry(std::string sourceType,
                                       std::string physicalStreamName,
                                       std::string logicalStreamName,
                                       TopologyNodePtr node)
    : sourceType(std::move(sourceType)), physicalStreamName(std::move(physicalStreamName)),
      logicalStreamName(std::move(logicalStreamName)), node(std::move(node)) {}

StreamCatalogEntry::StreamCatalogEntry(const AbstractPhysicalStreamConfigPtr& config, TopologyNodePtr node)
    : sourceType(config->getPhysicalStreamTypeConfig()->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue()),
      physicalStreamName(config->getPhysicalStreamTypeConfig()->getPhysicalStreamName()->getValue()),
      logicalStreamName(config->getPhysicalStreamTypeConfig()->getLogicalStreamName()->getValue()), node(std::move(node)) {
    // nop
}

StreamCatalogEntryPtr StreamCatalogEntry::create(const std::string& sourceType,
                                                 const std::string& physicalStreamName,
                                                 const std::string& logicalStreamName,
                                                 const TopologyNodePtr& node) {
    return std::make_shared<StreamCatalogEntry>(sourceType, physicalStreamName, logicalStreamName, node);
}

StreamCatalogEntryPtr StreamCatalogEntry::create(const AbstractPhysicalStreamConfigPtr& config, const TopologyNodePtr& node) {
    return std::make_shared<StreamCatalogEntry>(config, node);
}

std::string StreamCatalogEntry::getSourceType() { return sourceType; }

TopologyNodePtr StreamCatalogEntry::getNode() { return node; }

std::string StreamCatalogEntry::getPhysicalName() { return physicalStreamName; }

std::string StreamCatalogEntry::getLogicalName() { return logicalStreamName; }

std::string StreamCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << physicalStreamName << " logicalStreamName=" << logicalStreamName << " sourceType=" << sourceType
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}
}// namespace NES
