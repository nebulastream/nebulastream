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

#include <DefaultStatisticQueryGenerator.hpp>

#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreReaderLogicalOperator.hpp>
#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <fmt/format.h>

namespace NES
{

namespace
{
/// Field names carried through both queries; the lowering rules resolve them against the schema.
constexpr auto STAT_ID = "STATISTICID";
constexpr auto STAT_START = "STATISTICSTART";
constexpr auto STAT_END = "STATISTICEND";
constexpr auto STAT_VALUE = "VALUE";

/// A Generator "sequence" field that emits the constant `value` on every tuple (start == end).
std::string constantSequenceField(const uint64_t value)
{
    return fmt::format("SEQUENCE UINT64 {} {} 1", value, value);
}

/// A Generator heartbeat source whose four UINT64 fields (STATISTICID, STATISTICSTART, STATISTICEND, VALUE) are
/// constants. We generate them directly (rather than via a projection of constants) so the operators downstream
/// read real source fields — a pure-constant projection would access no input field and produce an empty scan.
LogicalPlan generatorWithConstants(
    const uint64_t statisticId, const uint64_t startTs, const uint64_t endTs, const uint64_t value, const std::string& workerHost)
{
    auto schema = std::vector<UnqualifiedUnboundField>{
                      UnqualifiedUnboundField{Identifier::parse(STAT_ID), DataType::Type::UINT64},
                      UnqualifiedUnboundField{Identifier::parse(STAT_START), DataType::Type::UINT64},
                      UnqualifiedUnboundField{Identifier::parse(STAT_END), DataType::Type::UINT64},
                      UnqualifiedUnboundField{Identifier::parse(STAT_VALUE), DataType::Type::UINT64}}
        | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();

    const auto generatorSchema = fmt::format(
        "{}\n{}\n{}\n{}",
        constantSequenceField(statisticId),
        constantSequenceField(startTs),
        constantSequenceField(endTs),
        constantSequenceField(value));

    std::unordered_map<Identifier, std::string> sourceConfig{
        {Identifier::parse("GENERATOR_SCHEMA"), generatorSchema},
        {Identifier::parse("GENERATOR_RATE_CONFIG"), "emit_rate 100"},
        {Identifier::parse("STOP_GENERATOR_WHEN_SEQUENCE_FINISHES"), "NONE"},
        /// "host" determines worker placement (consumed by the inline-source binding rule), not source behavior.
        {Identifier::parse("host"), workerHost}};

    return LogicalPlanBuilder::createLogicalPlan(
        Identifier::parse("Generator"), std::move(schema), std::move(sourceConfig), {{Identifier::parse("type"), "CSV"}});
}

/// Splices `newRoot` on top of `plan` (replicates LogicalPlanBuilder's private promoteOperatorToRoot).
LogicalPlan promoteToRoot(const LogicalPlan& plan, const LogicalOperator& newRoot)
{
    auto root = newRoot.withChildrenUnsafe(plan.getRootOperators());
    return LogicalPlan(plan.getQueryId(), {std::move(root)}, plan.getOriginalSql());
}

LogicalPlan addFifoFileSink(const LogicalPlan& plan, const std::string& fifoPath, const std::string& workerHost)
{
    return LogicalPlanBuilder::addInlineSink(
        Identifier::parse("File"),
        std::nullopt,
        {{Identifier::parse("FILE_PATH"), fifoPath},
         {Identifier::parse("APPEND"), "true"},
         {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
         /// "host" determines worker placement (consumed by the inline-sink binding rule), not sink behavior.
         {Identifier::parse("host"), workerHost}},
        {},
        plan);
}
}

DefaultStatisticQueryGenerator::DefaultStatisticQueryGenerator(std::string workerHost) : workerHost(std::move(workerHost))
{
}

LogicalPlan DefaultStatisticQueryGenerator::generateBuildQuery(
    const uint64_t statisticId, const uint64_t windowSizeMs, const std::string& fifoPath) const
{
    auto plan = generatorWithConstants(statisticId, /*startTs=*/0, /*endTs=*/windowSizeMs, /*value=*/42, workerHost);
    plan = promoteToRoot(
        plan, StatisticStoreWriterLogicalOperator::create(statisticId, std::string{STAT_START}, std::string{STAT_END}, std::string{STAT_VALUE}));
    return addFifoFileSink(plan, fifoPath, workerHost);
}

LogicalPlan DefaultStatisticQueryGenerator::generateProbeQuery(
    const uint64_t statisticId, const uint64_t startTs, const uint64_t endTs, const std::string& fifoPath) const
{
    auto plan = generatorWithConstants(statisticId, startTs, endTs, /*value=*/0, workerHost);
    plan = promoteToRoot(
        plan,
        StatisticStoreReaderLogicalOperator::create(
            std::string{STAT_ID}, std::string{STAT_START}, std::string{STAT_END}, std::string{STAT_VALUE}));
    return addFifoFileSink(plan, fifoPath, workerHost);
}

}
