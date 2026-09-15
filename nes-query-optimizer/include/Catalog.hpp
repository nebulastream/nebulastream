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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>

#include <Sinks/SinkDescriptor.hpp>
#include <Model.hpp>
#include <NetworkTopology.hpp>

namespace NES
{
/// Anonymous connectors are written inline in a query.
/// Internal ones are created by the optimizer, for example the network channel between two workers.
enum class ConnectorKind : uint8_t
{
    Anonymous,
    Internal,
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
    std::string dataAddress;
    Capacity maxOperators;
};

/// The optimizer's access to the catalog while it binds and places one query.
/// The bridge implements it on the open transaction of the coordinator.
/// A lookup of an unknown name throws.
/// An anonymous source or sink states its worker as the config key `host`.
class Catalog
{
public:
    virtual ~Catalog() = default;

    /// Source catalog
    virtual LogicalSource getLogicalSource(std::string_view name) const = 0;
    virtual std::unordered_set<SourceDescriptor> getPhysicalSources(const LogicalSource& logicalSource) const = 0;
    virtual SourceDescriptor createAnonymousSource(
        ConnectorKind kind,
        const Identifier& sourceType,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        const std::unordered_map<Identifier, std::string>& sourceConfig,
        const std::unordered_map<Identifier, std::string>& formatConfig) = 0;

    /// Sink catalog
    virtual SinkDescriptor getSinkDescriptor(std::string_view name) const = 0;
    virtual SinkDescriptor createAnonymousSink(
        ConnectorKind kind,
        const Identifier& sinkType,
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        const std::unordered_map<Identifier, std::string>& sinkConfig,
        const std::unordered_map<Identifier, std::string>& formatConfig) = 0;

    /// Worker catalog
    virtual WorkerInfo getWorker(const Host& host) const = 0;
    virtual NetworkTopology getTopology() const = 0;

    /// Model catalog
    virtual RegisteredModel getModel(std::string_view name) const = 0;
};
}
