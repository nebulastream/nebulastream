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

#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/OctetLengthLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Statistic/ScalarStatisticProbeLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Statistic/StatisticTypes.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <CollectionDomain.hpp>
#include <ErrorHandling.hpp>
#include <Metric.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

namespace
{

/// Only Average is implemented. The remaining metrics map to synopsis statistics -- equi-width histogram for
/// MinVal/MaxVal/Selectivity, count-min sketch for Cardinality, and so on -- none of which are part of this port.
StatisticType toStatisticType(const Metric metric)
{
    if (metric == Metric::Average)
    {
        return StatisticType::Avg;
    }
    throw NotImplemented(
        "Metric {} needs a synopsis statistic, which this port does not provide; only Average is supported", magic_enum::enum_name(metric));
}

/// The type a probe reconstructs the persisted scalar as. Only Avg is reachable today, but keeping the mapping
/// explicit means adding Count/Sum is a one-line change rather than a hunt.
DataType probeValueTypeFor(const StatisticType op)
{
    switch (op)
    {
        case StatisticType::Avg:
            return DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE);
        case StatisticType::Count:
            return DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
        case StatisticType::Sum:
            break;
    }
    throw NotImplemented("No probe value type is defined for statistic type {}", magic_enum::enum_name(op));
}

/// Turns the trigger's SQL condition into a LogicalFunction.
///
/// There is no public entry point that parses a bare expression, so the condition is wrapped in the smallest
/// complete query that can carry it and the Selection's predicate is lifted back out. The source and sink names
/// are never resolved -- parsing only needs the syntactic shape, and this plan is discarded once the predicate
/// has been taken from it. The INTO clause is not optional: without it the parser rejects the query outright.
///
/// Callers reach this only for conditions that are neither NEVER_SEND nor ALWAYS_SEND; those two are matched
/// before parsing, because they describe the plan rather than a row.
LogicalFunction parseCondition(const std::string& condition)
{
    const auto sql = fmt::format("SELECT * FROM _nes_stat_dummy_ WHERE {} INTO _nes_stat_dummy_sink_", condition);
    std::optional<LogicalPlan> parsed;
    try
    {
        parsed = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
    }
    catch (const std::exception& e)
    {
        throw InvalidQuerySyntax("Could not parse statistic trigger condition '{}': {}", condition, e.what());
    }

    /// A condition that parses but yields no Selection is one the parser folded away or read as something other
    /// than a predicate. Reporting it as invalid beats deploying a query that silently reports everything.
    const auto selections = getOperatorByType<SelectionLogicalOperator>(*parsed);
    if (selections.empty())
    {
        throw InvalidQuerySyntax("StatisticTuple trigger condition '{}' is not a predicate", condition);
    }
    return selections.front()->getPredicate();
}

/// Splits "host:port" as the statistic interface reports it. The sink wants the two halves separately.
std::pair<std::string, std::string> splitAddress(const std::string& address)
{
    const auto colon = address.rfind(':');
    if (colon == std::string::npos)
    {
        throw InvalidConfigParameter("StatisticTuple interface address '{}' is not in host:port form", address);
    }
    return {address.substr(0, colon), address.substr(colon + 1)};
}

LogicalPlan generateForDataDomain(
    const DataDomain& domain,
    const RequestStatisticBuildStatement& request,
    const StatisticTuple::StatisticId statisticId,
    const std::string& interfaceAddress)
{
    auto plan = LogicalPlanBuilder::createLogicalPlan(Identifier::parse(domain.logicalSourceName));

    const AggregationFieldAccess fieldAccess{
        TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse(domain.fieldName)}}};
    const auto statisticFunction = [&]() -> WindowAggregationLogicalFunction
    {
        switch (toStatisticType(request.metric))
        {
            case StatisticType::Count:
                return CountAggregationLogicalFunction{fieldAccess, false};
            case StatisticType::Sum:
                return SumAggregationLogicalFunction{fieldAccess};
            case StatisticType::Avg:
                return AvgAggregationLogicalFunction{fieldAccess};
        }
        throw NotImplemented("unsupported statistic metric");
    }();

    /// The result field is named from the id alone, which is how the fused StatisticStoreWriter finds the payload.
    plan = LogicalPlanBuilder::addWindowAggregation(
        plan,
        request.windowType,
        {WindowedAggregationLogicalOperator::ProjectedAggregation{statisticFunction, Identifier::parse(statisticDataFieldName(statisticId))}},
        {},
        request.timeCharacteristic);

    /// Project to scalar columns before the sink. Three things are going on here.
    ///
    /// The VARSIZED payload must not reach the sink. The sink transports formatted CSV, and the payload is a raw
    /// IEEE-754 double whose bytes can include commas and newlines, which corrupt the row framing. (This shows up
    /// only for some values: 20.0 and 200.0 happen to be byte-safe, arbitrary averages are not.)
    ///
    /// The statisticId has to travel with the report so the statistic interface can route it. The fused writer adds it to
    /// the Nautilus record but not to any schema, so it would not survive a pipeline boundary; projecting it as a
    /// constant puts it in the schema, where it does.
    ///
    /// And the payload has to stay *referenced*, which is what OCTET_LENGTH is doing here. The
    /// StatisticStoreWriter is fused into the aggregation's lowering, so it is invisible to the optimizer: an
    /// aggregation whose only output column nothing reads looks dead, and ProjectionPushdown duly emptied the
    /// aggregation list and took the writer's side effect with it. Reading the payload's length keeps the column
    /// live while staying scalar. It is also a useful assertion in its own right -- a scalar statistic is always
    /// 8 bytes.
    ///
    /// All of this runs after the writer, which sits below in the aggregation, so the store still sees the full
    /// record.
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::STATISTIC_ID}),
        LogicalFunction{ConstantValueLogicalFunction{uint64Type, std::to_string(statisticId.getRawValue())}});
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::START_TS}),
        LogicalFunction{UnboundFieldAccessLogicalFunction{Identifier::parse("start")}});
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::END_TS}),
        LogicalFunction{UnboundFieldAccessLogicalFunction{Identifier::parse("end")}});
    projections.emplace_back(
        Identifier::parse(std::string{StatisticFieldNames::PAYLOAD_BYTES}),
        LogicalFunction{
            OctetLengthLogicalFunction{LogicalFunction{UnboundFieldAccessLogicalFunction{Identifier::parse(statisticDataFieldName(statisticId))}}}});
    plan = LogicalPlanBuilder::addProjection(std::move(projections), /*asterisk=*/false, plan);

    /// The trigger's condition decides the whole shape of what follows, in three cases.
    ///
    /// NEVER_SEND is the write-only query: no probe, no network. Nothing consumes a report unless a callback was
    /// registered for it -- onStatisticReport routes reports to the registry's triggers and drops them when there
    /// are none -- so reporting here would pay a gRPC round-trip per closed window for a message that is
    /// immediately discarded. It terminates in a VoidSink instead. The statistic is still persisted: the writer is
    /// fused into the aggregation, far below the sink, so getStatistics reads it back exactly as before.
    ///
    /// Anything else reports, and reporting means probing. Both of the things a trigger needs -- the value handed
    /// to the callback, and the value a predicate is evaluated over -- exist only in the store; the build chain
    /// carries the statistic as an opaque VARSIZED payload it can neither compare nor report. So the query reads
    /// back what it just wrote. The probe takes the window bounds from the projected columns above and supplies
    /// STATISTICID and STATISTICVALUE itself, so its output is exactly what the sink reports.
    ///
    /// ALWAYS_SEND stops there: every closed window is worth reporting, so there is nothing to filter.
    /// Every other condition adds a Selection over the probed columns, and only matching windows are reported.
    const auto& trigger = request.conditionTrigger;
    const auto [host, port] = splitAddress(interfaceAddress);
    if (trigger.sendsNever())
    {
        return LogicalPlanBuilder::addAnonymousSink(Identifier::parse("Void"), std::nullopt, {{Identifier::parse("host"), host}}, {}, plan);
    }

    const auto probe = ScalarStatisticProbeLogicalOperator::create(
        statisticId,
        toStatisticType(request.metric),
        probeValueTypeFor(toStatisticType(request.metric)),
        Identifier::parse(std::string{StatisticFieldNames::START_TS}),
        Identifier::parse(std::string{StatisticFieldNames::END_TS}));
    plan = plan.withRootOperators({LogicalOperator{probe}.withChildrenUnsafe(plan.getRootOperators())});

    if (not trigger.sendsAlways())
    {
        plan = LogicalPlanBuilder::addSelection(parseCondition(trigger.condition), plan);
    }

    return LogicalPlanBuilder::addAnonymousSink(
        Identifier::parse("Grpc"),
        std::nullopt,
        {{Identifier::parse("grpc_host"), host},
         {Identifier::parse("grpc_port"), port},
         {Identifier::parse("OUTPUT_FORMAT"), "CSV"},
         {Identifier::parse("host"), host}},
        {},
        plan);
}

}

LogicalPlan DefaultStatisticQueryGenerator::generateQuery(
    const RequestStatisticBuildStatement& request, const StatisticTuple::StatisticId statisticId, const std::string& interfaceAddress) const
{
    return std::visit(
        [&]<typename DomainAlternative>(const DomainAlternative& domain) -> LogicalPlan
        {
            using DomainType = std::decay_t<DomainAlternative>;
            if constexpr (std::is_same_v<DomainType, DataDomain>)
            {
                return generateForDataDomain(domain, request, statisticId, interfaceAddress);
            }
            else if constexpr (std::is_same_v<DomainType, WorkloadDomain>)
            {
                throw NotImplemented(
                    "Collecting a statistic over the output of query {} operator {} is not implemented; the splice "
                    "machinery it needs is not part of this port.",
                    domain.queryId,
                    domain.operatorId);
            }
            else
            {
                throw NotImplemented(
                    "Collecting infrastructure statistics for worker {} is not implemented; it would need "
                    "infrastructure metric sources that do not exist.",
                    domain.hostId);
            }
        },
        request.domain);
}

}
