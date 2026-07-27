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

#include <Catalog.hpp>

namespace NES
{
struct TransactionContext;

class TransactionalCatalog final : public Catalog
{
    const TransactionContext& ctx;

public:
    explicit TransactionalCatalog(const TransactionContext& ctx);

    LogicalSource getLogicalSource(std::string_view name) const override;
    std::unordered_set<SourceDescriptor> getPhysicalSources(std::string_view logicalSourceName) const override;
    SourceDescriptor createInlineSource(
        ConnectorKind kind,
        const Identifier& sourceType,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        std::unordered_map<Identifier, std::string> sourceCfg,
        std::unordered_map<Identifier, std::string> formatCfg) override;

    /// Sink catalog
    SinkDescriptor getSinkDescriptor(std::string_view name) const override;
    SinkDescriptor createInlineSink(
        ConnectorKind kind,
        const Identifier& sinkType,
        const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
        std::unordered_map<Identifier, std::string> sinkCfg,
        const std::unordered_map<Identifier, std::string>& formatCfg) override;

    /// Worker catalog
    WorkerInfo getWorker(const Host& host) const override;
    NetworkTopology getTopology() const override;

    /// Model catalog
    RegisteredModel getModel(std::string_view name) const override;
};

}
