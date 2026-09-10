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

#include <optional>
#include <string_view>
#include <unordered_set>
#include <variant>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>

#include <Sinks/SinkDescriptor.hpp>
#include <Model.hpp>
#include <NetworkTopology.hpp>

namespace NES
{
enum class ConnectorKind : bool
{
    Anonymous = false,
    Internal = true,
};

namespace CapacityKind
{
struct Unlimited
{
};

struct Limited
{
    size_t value;
};
}

using Capacity = std::variant<CapacityKind::Unlimited, CapacityKind::Limited>;

struct WorkerInfo
{
    Host host;
    std::string data;
    Capacity maxOperators;
};

class Catalog
{
public:
    virtual ~Catalog() = default;

    /// Source catalog
    virtual LogicalSource getLogicalSource(std::string_view name) const = 0;
    virtual std::unordered_set<SourceDescriptor> getPhysicalSources(std::string_view logicalSourceName) const = 0;
    virtual SourceDescriptor createAnonymousSource(
        ConnectorKind kind,
        const Identifier& sourceType,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        std::unordered_map<Identifier, std::string> sourceCfg,
        std::unordered_map<Identifier, std::string> formatCfg)
        = 0;

    /// Sink catalog
    virtual SinkDescriptor getSinkDescriptor(std::string_view name) const = 0;
    virtual SinkDescriptor createAnonymousSink(
        ConnectorKind kind,
        const Identifier& sinkType,
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        std::unordered_map<Identifier, std::string> sinkCfg,
        const std::unordered_map<Identifier, std::string>& formatCfg)
        = 0;

    /// Worker catalog
    virtual WorkerInfo getWorker(const Host& host) const = 0;
    virtual NetworkTopology getTopology() const = 0;

    /// Model catalog
    virtual RegisteredModel getModel(std::string_view name) const = 0;
};
}
