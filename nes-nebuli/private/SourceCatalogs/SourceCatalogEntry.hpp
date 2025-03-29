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

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <Identifiers/Identifiers.hpp>

namespace NES
{
class LogicalSource;
class PhysicalSource;

namespace Catalogs::Source
{

/// @brief one entry in the catalog contains
/// - the physical data source that can be consumed
/// - The logical source to which the physical source contributes towards
/// - the topology that offer this source
class SourceCatalogEntry
{
public:
    static std::shared_ptr<SourceCatalogEntry>
    create(std::shared_ptr<PhysicalSource> physicalSource, std::shared_ptr<LogicalSource> logicalSource, WorkerId topologyNodeId);

    [[nodiscard]] const std::shared_ptr<PhysicalSource>& getPhysicalSource() const;
    [[nodiscard]] const std::shared_ptr<LogicalSource>& getLogicalSource() const;

    WorkerId getTopologyNodeId() const;

    std::string toString();

private:
    explicit SourceCatalogEntry(
        std::shared_ptr<PhysicalSource> physicalSource, std::shared_ptr<LogicalSource> logicalSource, WorkerId topologyNodeId);

    std::shared_ptr<PhysicalSource> physicalSource;
    std::shared_ptr<LogicalSource> logicalSource;
    WorkerId topologyNodeId;
};
}
}
