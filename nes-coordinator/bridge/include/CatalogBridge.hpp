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

/// The optimizer's catalog for one request, answered by the Rust coordinator on that request's transaction.
class CatalogBridge final : public Catalog
{
    const Bridge::PlanningTransaction& ctx;
    /// An anonymous source or sink stores its host in its own config instead of a HOST clause, and may omit it.
    /// The policy resolves an omitted host the same way as for a declared one.
    HostPolicy hostPolicy;

public:
    CatalogBridge(const Bridge::PlanningTransaction& ctx, HostPolicy hostPolicy);

    [[nodiscard]] LogicalSource getLogicalSource(std::string_view name) const override;
    [[nodiscard]] std::unordered_set<SourceDescriptor> getPhysicalSources(const LogicalSource& logicalSource) const override;
    SourceDescriptor createAnonymousSource(
        ConnectorKind kind,
        const Identifier& sourceType,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        std::unordered_map<Identifier, std::string> sourceCfg,
        std::unordered_map<Identifier, std::string> formatCfg) override;

    [[nodiscard]] SinkDescriptor getSinkDescriptor(std::string_view name) const override;
    SinkDescriptor createAnonymousSink(
        ConnectorKind kind,
        const Identifier& sinkType,
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        std::unordered_map<Identifier, std::string> sinkCfg,
        const std::unordered_map<Identifier, std::string>& formatCfg) override;

    [[nodiscard]] WorkerInfo getWorker(const Host& host) const override;
    [[nodiscard]] NetworkTopology getTopology() const override;

    [[nodiscard]] RegisteredModel getModel(std::string_view name) const override;
};

}
