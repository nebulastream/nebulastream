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

#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

SourceCatalogEntry::SourceCatalogEntry(std::string sourceType,
                                       std::string physicalStreamName,
                                       std::string logicalStreamName,
                                       TopologyNodePtr node)
    : sourceType(std::move(sourceType)), physicalStreamName(std::move(physicalStreamName)),
      logicalStreamName(std::move(logicalStreamName)), node(std::move(node)) {}

SourceCatalogEntry::SourceCatalogEntry(const AbstractPhysicalStreamConfigPtr& config, TopologyNodePtr node)
    : sourceType(config->getPhysicalStreamTypeConfig()->getPhysicalStreamTypeConfiguration()->getSourceType()->getValue()),
      physicalStreamName(config->getPhysicalStreamTypeConfig()->getPhysicalStreamName()->getValue()),
      logicalStreamName(config->getPhysicalStreamTypeConfig()->getLogicalStreamName()->getValue()), node(std::move(node)) {
    // nop
}

SourceCatalogEntryPtr SourceCatalogEntry::create(const std::string& sourceType,
                                                 const std::string& physicalSourceName,
                                                 const std::string& logicalSourceName,
                                                 const TopologyNodePtr& node) {
    return std::make_shared<SourceCatalogEntry>(sourceType, physicalSourceName, logicalSourceName, node);
}

SourceCatalogEntryPtr SourceCatalogEntry::create(const AbstractPhysicalStreamConfigPtr& config, const TopologyNodePtr& node) {
    return std::make_shared<SourceCatalogEntry>(config, node);
}

std::string SourceCatalogEntry::getSourceType() { return sourceType; }

TopologyNodePtr SourceCatalogEntry::getNode() { return node; }

std::string SourceCatalogEntry::getPhysicalName() { return physicalStreamName; }

std::string SourceCatalogEntry::getLogicalName() { return logicalStreamName; }

std::string SourceCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalName=" << physicalStreamName << " logicalSourceName=" << logicalStreamName << " sourceType=" << sourceType
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}
}// namespace NES
