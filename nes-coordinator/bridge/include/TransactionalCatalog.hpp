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
struct TransactionContext;

class TransactionalCatalog final : public Catalog
{
    const TransactionContext& ctx;
    /// A source or sink written into a query stores its host in its own config instead of a HOST clause, and may omit it.
    /// The policy decides what an omitted host means, the same way it does for a statement that declares one.
    HostPolicy hostPolicy;

public:
    TransactionalCatalog(const TransactionContext& ctx, HostPolicy hostPolicy);

    [[nodiscard]] LogicalSource getLogicalSource(std::string_view name) const override;
    [[nodiscard]] std::unordered_set<SourceDescriptor> getPhysicalSources(std::string_view logicalSourceName) const override;
    SourceDescriptor createAnonymousSource(
        ConnectorKind kind,
        const Identifier& sourceType,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        std::unordered_map<Identifier, std::string> sourceCfg,
        std::unordered_map<Identifier, std::string> formatCfg) override;

    /// Sink catalog
    [[nodiscard]] SinkDescriptor getSinkDescriptor(std::string_view name) const override;
    SinkDescriptor createAnonymousSink(
        ConnectorKind kind,
        const Identifier& sinkType,
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        std::unordered_map<Identifier, std::string> sinkCfg,
        const std::unordered_map<Identifier, std::string>& formatCfg) override;

    /// Worker catalog
    [[nodiscard]] WorkerInfo getWorker(const Host& host) const override;
    [[nodiscard]] NetworkTopology getTopology() const override;

    /// Model catalog
    [[nodiscard]] RegisteredModel getModel(std::string_view name) const override;
};

}
