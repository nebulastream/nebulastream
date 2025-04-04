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

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <SourceCatalogs/SourceCatalogEntry.hpp>

namespace NES::Catalogs::Source
{

SourceCatalogEntry::SourceCatalogEntry(
    std::shared_ptr<PhysicalSource> physicalSource, std::shared_ptr<LogicalSource> logicalSource, WorkerId topologyNodeId)
    : physicalSource(std::move(physicalSource)), logicalSource(std::move(logicalSource)), topologyNodeId(topologyNodeId)
{
}

std::shared_ptr<SourceCatalogEntry> SourceCatalogEntry::create(
    std::shared_ptr<PhysicalSource> physicalSource, std::shared_ptr<LogicalSource> logicalSource, WorkerId topologyNodeId)
{
    return std::make_shared<SourceCatalogEntry>(SourceCatalogEntry(std::move(physicalSource), std::move(logicalSource), topologyNodeId));
}

const std::shared_ptr<PhysicalSource>& SourceCatalogEntry::getPhysicalSource() const
{
    return physicalSource;
}

const std::shared_ptr<LogicalSource>& SourceCatalogEntry::getLogicalSource() const
{
    return logicalSource;
}

WorkerId SourceCatalogEntry::getTopologyNodeId() const
{
    return topologyNodeId;
}

std::string SourceCatalogEntry::toString() const
{
    std::stringstream ss;
    ss << "physicalSource=" << physicalSource << " logicalSource=" << logicalSource << " on node=" << topologyNodeId;
    return ss.str();
}
std::string SourceCatalogEntry::getPhysicalSourceName() const
{
    return physicalSourceName;
}
}
