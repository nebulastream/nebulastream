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

#include <PlannerBridge.hpp>

#include <utility>

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <coordinator/lib.h>
#include <cpptrace/from_current.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <BridgeError.hpp>
#include <SqlPlanner.hpp>
#include <TransactionalCatalog.hpp>

namespace NES
{
namespace
{
PlannedStatement planStatement(const TransactionContext& ctx, rust::Str sql, rust::Str optimizerConfig, rust::Str defaultHost)
{
    auto sqlStr = std::string(sql.data(), sql.size());
    auto config = QueryOptimizerConfiguration{};
    if (!optimizerConfig.empty())
    {
        auto parsed = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string{optimizerConfig});
        if (!parsed)
        {
            throw CannotDeserialize("Failed to deserialize optimizer config from JSON: {}", parsed.error().what());
        }
        config.overwriteConfigWithCommandLineInput(*parsed);
    }
    /// An empty default host rejects statements that omit their HOST clause; a non-empty one substitutes
    /// it (embedded deployments, where the only worker is the local one).
    HostPolicy hostPolicy = RequireHostConfig{};
    if (!defaultHost.empty())
    {
        hostPolicy = DefaultHost{std::string{defaultHost}};
    }
    auto executed
        = SqlPlanner{std::make_shared<TransactionalCatalog>(ctx, hostPolicy), std::move(config), std::move(hostPolicy)}.plan(sqlStr);
    if (!executed)
    {
        /// Rethrown rather than converted here so the guard below records the code and stacktrace this carries.
        throw executed.error();
    }

    /// `StatementResult` is already shaped like Rust's `model::Statement`, so it serializes without any intermediate conversion.
    auto json = rfl::json::write(executed->statement);

    rust::Vec<PlannedQueryFragment> serialized;
    rust::Vec<int64_t> sourceIds;
    rust::Vec<int64_t> sinkIds;
    for (const auto& [host, fragments] : executed->fragments)
    {
        for (const auto& fragment : fragments)
        {
            const auto proto = QueryPlanSerializationUtil::serializeQueryPlan(fragment);
            const auto bytes = proto.SerializeAsString();
            const auto allOps = flatten(fragment);
            const auto sinks = getOperatorByType<SinkLogicalOperator>(fragment);
            const auto sources = getOperatorByType<SourceDescriptorLogicalOperator>(fragment);
            const auto numOps = static_cast<int32_t>(allOps.size());
            const auto numSinks = static_cast<int32_t>(sinks.size());
            const auto numSources = static_cast<int32_t>(sources.size());

            for (const auto& source : sources)
            {
                sourceIds.push_back(static_cast<int64_t>(source->getSourceDescriptor().getPhysicalSourceId().getRawValue()));
            }

            for (const auto& sink : sinks)
            {
                if (const auto& desc = sink->getSinkDescriptor(); desc.has_value())
                {
                    sinkIds.push_back(desc->getSinkId().getRawValue());
                }
            }

            rust::Vec<uint8_t> planBytes;
            planBytes.reserve(bytes.size());
            for (const auto byte : bytes)
            {
                planBytes.push_back(static_cast<uint8_t>(byte));
            }

            serialized.push_back(PlannedQueryFragment{
                .host_addr = rust::String(host.getRawValue()),
                .plan = std::move(planBytes),
                .num_operators = numOps - numSinks - numSources,
                .has_source = numSources > 0,
            });
        }
    }

    return PlannedStatement{
        .error = {},
        .json = rust::String(json),
        .fragments = std::move(serialized),
        .source_ids = std::move(sourceIds),
        .sink_ids = std::move(sinkIds),
    };
}
}

PlannedStatement plan_sql(const TransactionContext& ctx, rust::Str sql, rust::Str optimizerConfig, rust::Str defaultHost)
{
    CPPTRACE_TRY
    {
        return planStatement(ctx, sql, optimizerConfig, defaultHost);
    }
    CPPTRACE_CATCH(...)
    {
        /// An NES exception keeps its own code and trace; anything else, including an error raised by one of the Rust
        /// catalog callbacks, arrives as UnknownException carrying its message.
        return PlannedStatement{
            .error = make_error(wrapExternalException()),
            .json = {},
            .fragments = {},
            .source_ids = {},
            .sink_ids = {},
        };
    }
    std::unreachable();
}
}
