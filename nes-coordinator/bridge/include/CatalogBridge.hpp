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
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Catalog.hpp>
#include <HostPolicy.hpp>
#include <Model.hpp>
#include <NetworkTopology.hpp>

namespace NES
{
namespace Bridge
{
struct PlanningTransaction;
}

/// Implements the optimizer's catalog by calling the Rust coordinator.
/// All calls run on the transaction of one request.
class CatalogBridge final : public Catalog
{
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) the request's transaction outlives its catalog
    const Bridge::PlanningTransaction& ctx;
    /// Used for an anonymous source or sink whose config has no host.
    std::optional<Host> defaultHost;

public:
    CatalogBridge(const Bridge::PlanningTransaction& ctx, std::optional<Host> defaultHost);

    [[nodiscard]] LogicalSource getLogicalSource(std::string_view name) const override;
    [[nodiscard]] std::unordered_set<SourceDescriptor> getPhysicalSources(const LogicalSource& logicalSource) const override;
    SourceDescriptor createAnonymousSource(
        ConnectorKind kind,
        const Identifier& sourceType,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        const std::unordered_map<Identifier, std::string>& sourceConfig,
        const std::unordered_map<Identifier, std::string>& formatConfig) override;

    [[nodiscard]] SinkDescriptor getSinkDescriptor(std::string_view name) const override;
    SinkDescriptor createAnonymousSink(
        ConnectorKind kind,
        const Identifier& sinkType,
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        const std::unordered_map<Identifier, std::string>& sinkConfig,
        const std::unordered_map<Identifier, std::string>& formatConfig) override;

    [[nodiscard]] WorkerInfo getWorker(const Host& host) const override;
    [[nodiscard]] NetworkTopology getTopology() const override;

    [[nodiscard]] RegisteredModel getModel(std::string_view name) const override;
};

}
