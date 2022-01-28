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

#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

SourceCatalogEntry::SourceCatalogEntry(PhysicalSourcePtr physicalSource,
                                       LogicalSourcePtr logicalSource,
                                       TopologyNodePtr topologyNode)
    : physicalSource(std::move(physicalSource)), logicalSource(std::move(logicalSource)), node(std::move(topologyNode)) {}

SourceCatalogEntryPtr
SourceCatalogEntry::create(PhysicalSourcePtr physicalSource, LogicalSourcePtr logicalSource, TopologyNodePtr topologyNode) {
    return std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, topologyNode);
}

const PhysicalSourcePtr& SourceCatalogEntry::getPhysicalSource() const { return physicalSource; }

const LogicalSourcePtr& SourceCatalogEntry::getLogicalSource() const { return logicalSource; }

const TopologyNodePtr& SourceCatalogEntry::getNode() const { return node; }

std::string SourceCatalogEntry::toString() {
    std::stringstream ss;
    ss << "physicalSource=" << physicalSource << " logicalSource=" << logicalSource
       << " on node=" + std::to_string(node->getId());
    return ss.str();
}
}// namespace NES
