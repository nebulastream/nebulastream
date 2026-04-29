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

#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <NetworkTopology.hpp>

#include <Model.hpp>

namespace NES {
    struct PlannerContext;

    enum class ConnectorKind : bool {
        Inline = false,
        Internal = true,
    };

    namespace CapacityKind {
        struct Unlimited {
        };

        struct Limited {
            size_t value;
        };
    }

    using Capacity = std::variant<CapacityKind::Unlimited, CapacityKind::Limited>;

    struct WorkerInfo {
        Host host;
        std::string data;
        Capacity maxOperators;
    };

    namespace SourceCatalog {
        LogicalSource getLogicalSource(const PlannerContext &ctx, std::string_view name);

        std::unordered_set<SourceDescriptor> getPhysicalSources(const PlannerContext &ctx,
                                                                std::string_view logicalSourceName);

        SourceDescriptor createInlineSource(
            const PlannerContext &ctx,
            ConnectorKind kind,
            const Identifier &sourceType,
            const Schema<UnqualifiedUnboundField, Ordered> &schema,
            std::unordered_map<Identifier, std::string> config,
            std::unordered_map<Identifier, std::string> formatConfig);
    }

    namespace SinkCatalog {
        SinkDescriptor getSinkDescriptor(const PlannerContext &ctx, std::string_view sinkName);

        SinkDescriptor createInlineSink(
            const PlannerContext &ctx,
            ConnectorKind kind,
            const Identifier &sinkType,
            const Schema<UnqualifiedUnboundField, Ordered> &schema,
            std::unordered_map<Identifier, std::string> config,
            const std::unordered_map<Identifier, std::string>& formatConfig);
}

namespace WorkerCatalog
{
WorkerInfo getWorker(const PlannerContext& ctx, const Host& host);

NetworkTopology getTopology(const PlannerContext& ctx);
}

namespace ModelCatalog
{
RegisteredModel getModel(const PlannerContext& ctx, std::string_view name);
}
}
