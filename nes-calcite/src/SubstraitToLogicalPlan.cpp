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

/// Substrait -> unbound LogicalPlan, built with LogicalPlanBuilder exactly like the ANTLR parser does, so that binding,
/// type inference, watermark and origin-id rules, placement and lowering all run unchanged afterwards.
///
/// Substrait references fields by position, our plans by name. Every converted relation therefore carries the names of
/// its output columns, and positional references are turned into UnboundFieldAccess on those names. Columns that
/// Substrait computes without a name (projections, aggregates) get fresh `calcite_<n>` names; the root projection
/// renames the output to the names Calcite reports for the query.
///
/// Literals are emitted untyped, like the ANTLR parser does, so type inference picks their type from context.

#include <Calcite/CalcitePlanner.hpp>
#include "SubstraitTypes.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/NullValueLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNullCheckLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <substrait/algebra.pb.h>
#include <substrait/plan.pb.h>
#include <ErrorHandling.hpp>
#include <nes_calcite_planner.pb.h>

namespace NES::Calcite
{
namespace
{

struct Relation
{
    LogicalPlan plan;
    std::vector<Identifier> names;
};

/// Calcite names unnamed columns like EXPR$0, which is not a legal unquoted identifier.
Identifier toIdentifier(const std::string& name)
{
    if (auto identifier = Identifier::tryParse(name))
    {
        return std::move(*identifier);
    }
    return Identifier::parse(fmt::format("\"{}\"", name));
}

LogicalFunction field(const Identifier& name)
{
    return UnboundFieldAccessLogicalFunction{name};
}

LogicalFunction rawLiteral(std::string value)
{
    return ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED), std::move(value));
}

}

namespace detail
{

/// Substrait has no unsigned integers; Calcite's UTINYINT..UBIGINT arrive as the user-defined types u8..u64 of this
/// extension (see Unsigned.java).
UserDefinedTypes unsignedTypes(const substrait::Plan& plan)
{
    static constexpr std::string_view UNSIGNED_URN = "extension:io.substrait:unsigned_integers";
    static const std::unordered_map<std::string, DataType::Type> names{
        {"u8", DataType::Type::UINT8}, {"u16", DataType::Type::UINT16}, {"u32", DataType::Type::UINT32}, {"u64", DataType::Type::UINT64}};
    std::unordered_set<uint32_t> unsignedUrns;
    for (const auto& urn : plan.extension_urns())
    {
        if (urn.urn() == UNSIGNED_URN)
        {
            unsignedUrns.insert(urn.extension_urn_anchor());
        }
    }
    UserDefinedTypes types;
    for (const auto& extension : plan.extensions())
    {
        if (extension.has_extension_type() and unsignedUrns.contains(extension.extension_type().extension_urn_reference()))
        {
            if (const auto found = names.find(extension.extension_type().name()); found != names.end())
            {
                types.emplace(extension.extension_type().type_anchor(), found->second);
            }
        }
    }
    return types;
}

DataType toDataType(const substrait::Type& type, const UserDefinedTypes& userDefinedTypes)
{
    const auto make = [](const DataType::Type nesType, const substrait::Type::Nullability nullability)
    {
        return DataTypeProvider::provideDataType(
            nesType,
            nullability == substrait::Type::NULLABILITY_NULLABLE ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    };
    switch (type.kind_case())
    {
        case substrait::Type::kBool:
            return make(DataType::Type::BOOLEAN, type.bool_().nullability());
        case substrait::Type::kI8:
            return make(DataType::Type::INT8, type.i8().nullability());
        case substrait::Type::kI16:
            return make(DataType::Type::INT16, type.i16().nullability());
        case substrait::Type::kI32:
            return make(DataType::Type::INT32, type.i32().nullability());
        case substrait::Type::kI64:
            return make(DataType::Type::INT64, type.i64().nullability());
        case substrait::Type::kFp32:
            return make(DataType::Type::FLOAT32, type.fp32().nullability());
        case substrait::Type::kFp64:
            return make(DataType::Type::FLOAT64, type.fp64().nullability());
        case substrait::Type::kString:
            return make(DataType::Type::VARSIZED, type.string().nullability());
        case substrait::Type::kVarchar:
            return make(DataType::Type::VARSIZED, type.varchar().nullability());
        case substrait::Type::kFixedChar:
            return make(DataType::Type::CHAR, type.fixed_char().nullability());
        case substrait::Type::kPrecisionTimestamp:
            return make(DataType::Type::UINT64, type.precision_timestamp().nullability());
        case substrait::Type::kUserDefined:
            if (const auto found = userDefinedTypes.find(type.user_defined().type_reference()); found != userDefinedTypes.end())
            {
                return make(found->second, type.user_defined().nullability());
            }
            throw UnsupportedQuery("Substrait type {} is not supported", type.ShortDebugString());
        default:
            throw UnsupportedQuery("Substrait type {} is not supported", type.ShortDebugString());
    }
}

std::unordered_map<uint32_t, std::string> functionNames(const substrait::Plan& plan)
{
    std::unordered_map<uint32_t, std::string> functions;
    for (const auto& extension : plan.extensions())
    {
        if (extension.has_extension_function())
        {
            /// Names carry their signature (gt:any_any); only the function matters to us.
            const auto& name = extension.extension_function().name();
            functions.emplace(extension.extension_function().function_anchor(), name.substr(0, name.find(':')));
        }
    }
    return functions;
}
}

namespace
{
using detail::toDataType;
using detail::unsignedTypes;
using detail::UserDefinedTypes;

class Converter
{
public:
    explicit Converter(
        const substrait::Plan& plan,
        const bool finiteInputs,
        std::optional<std::string> fallbackSource,
        std::unordered_map<std::string, std::string> catalogNames)
        : finiteInputs(finiteInputs), catalogNames(std::move(catalogNames))
    {
        if (plan.relations_size() == 1 and plan.relations(0).has_root())
        {
            anySource = findNamedTable(plan.relations(0).root().input());
        }
        /// A plan Calcite proved empty may read no source at all.
        if (not anySource)
        {
            anySource = std::move(fallbackSource);
        }
        userDefinedTypes = unsignedTypes(plan);
        for (const auto& extension : plan.extensions())
        {
            if (extension.has_extension_function())
            {
                /// Names carry their signature (gt:any_any); only the function matters to us.
                const auto& name = extension.extension_function().name();
                functions.emplace(extension.extension_function().function_anchor(), name.substr(0, name.find(':')));
            }
        }
    }

    LogicalPlan convertRoot(const substrait::RelRoot& root)
    {
        auto [plan, names] = convert(root.input());
        if (static_cast<size_t>(root.names_size()) != names.size())
        {
            throw UnsupportedQuery("Substrait root has {} names for {} columns", root.names_size(), names.size());
        }
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        std::unordered_set<std::string> used;
        for (int i = 0; i < root.names_size(); ++i)
        {
            /// SELECT may repeat an output name (SELECT a, a); our projections may not.
            auto name = toIdentifier(root.names(i));
            if (not used.insert(name.asCanonicalString()).second)
            {
                name = freshName();
            }
            projections.emplace_back(std::move(name), field(names.at(i)));
        }
        /// Always project: besides renaming, this drops helper columns that are not part of the result (see addConstantTime).
        return LogicalPlanBuilder::addProjection(std::move(projections), false, plan);
    }

private:
    std::unordered_map<uint32_t, std::string> functions;
    UserDefinedTypes userDefinedTypes;
    size_t nextName = 0;
    bool finiteInputs;
    /// Source names in the plan -> catalog names; names without entry are catalog names already.
    std::unordered_map<std::string, std::string> catalogNames;
    /// A source the query reads; constant rows (VALUES) are derived from it, see convertValues.
    std::optional<std::string> anySource;

    static std::optional<std::string> findNamedTable(const substrait::Rel& rel)
    {
        const auto first = [](const auto& inputs) -> std::optional<std::string>
        {
            for (const auto& input : inputs)
            {
                if (auto found = findNamedTable(input))
                {
                    return found;
                }
            }
            return std::nullopt;
        };
        switch (rel.rel_type_case())
        {
            case substrait::Rel::kRead:
                if (rel.read().has_named_table() and not rel.read().named_table().names().empty())
                {
                    return rel.read().named_table().names(rel.read().named_table().names_size() - 1);
                }
                return std::nullopt;
            case substrait::Rel::kFilter:
                return findNamedTable(rel.filter().input());
            case substrait::Rel::kProject:
                return findNamedTable(rel.project().input());
            case substrait::Rel::kSet:
                return first(rel.set().inputs());
            case substrait::Rel::kExtensionSingle:
                return findNamedTable(rel.extension_single().input());
            case substrait::Rel::kExtensionMulti:
                return first(rel.extension_multi().inputs());
            default:
                return std::nullopt;
        }
    }

    /// Exactly one row, whatever the (finite) source holds: a counting GLOBAL aggregation emits even for empty input.
    LogicalPlan oneRow()
    {
        if (not anySource)
        {
            throw UnsupportedQuery("constant rows (VALUES) need a query that reads at least one source");
        }
        auto source = LogicalPlanBuilder::createLogicalPlan(toIdentifier(catalogName(*anySource)));
        const auto time = addConstantTime(source);
        return LogicalPlanBuilder::addWindowAggregation(
            std::move(source),
            toWindowType(nes::calcite::WindowAggregate::GLOBAL, 0, 0),
            {{.function = CountAggregationLogicalFunction{
                  TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse("*")}}, true},
              .name = freshName()}},
            {},
            eventTime(time),
            true);
    }

    static LogicalFunction defaultValue(const substrait::Type& type)
    {
        switch (type.kind_case())
        {
            case substrait::Type::kBool:
                return rawLiteral("false");
            case substrait::Type::kString:
            case substrait::Type::kVarchar:
            case substrait::Type::kFixedChar:
                return rawLiteral("'0'");
            default:
                return rawLiteral("0");
        }
    }

    /// VALUES (finite inputs only), as projections of constants over oneRow(), one branch per row. Without rows, the one
    /// row is filtered away. Calcite produces these for (parts of) queries it proves empty, and as single dummy rows.
    Relation convertValues(const substrait::ReadRel& read)
    {
        if (not finiteInputs)
        {
            throw UnsupportedQuery("constant rows (VALUES) are only supported with finite inputs");
        }
        const auto& schema = read.base_schema();
        Relation result{.plan = oneRow(), .names = {}};
        std::unordered_set<std::string> used;
        for (const auto& name : schema.names())
        {
            /// Calcite's VALUES may repeat a column name, our projections may not.
            auto identifier = toIdentifier(name);
            result.names.push_back(used.insert(identifier.asCanonicalString()).second ? identifier : freshName());
        }
        const auto project = [&](const std::function<LogicalFunction(int)>& column, LogicalPlan input)
        {
            std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
            for (int i = 0; i < schema.names_size(); ++i)
            {
                projections.emplace_back(result.names.at(i), column(i));
            }
            return LogicalPlanBuilder::addProjection(std::move(projections), false, input);
        };

        const auto& rows = read.virtual_table().expressions();
        if (rows.empty())
        {
            result.plan = project(
                [&](const int i)
                {
                    const auto& type = schema.struct_().types(i);
                    return LogicalFunction{CastToTypeLogicalFunction(toDataType(type, userDefinedTypes), defaultValue(type))};
                },
                LogicalPlanBuilder::addSelection(rawLiteral("false"), result.plan));
            return result;
        }
        std::optional<LogicalPlan> unioned;
        for (const auto& row : rows)
        {
            auto rowPlan = project([&](const int i) { return convertExpression(row.fields(i), {}); }, oneRow());
            unioned = unioned ? LogicalPlanBuilder::addUnion(std::move(*unioned), std::move(rowPlan)) : std::move(rowPlan);
        }
        result.plan = std::move(*unioned);
        return result;
    }

    Identifier freshName() { return Identifier::parse(fmt::format("calcite_{}", nextName++)); }

    /// Calcite inlines a CTE at every use, so identical subtrees are converted (and executed) once per use. Sharing them
    /// as a DAG loses rows today: NebulaStream copies the shared operator later in the pipeline but keeps its identity.
    Relation convert(const substrait::Rel& rel)
    {
        switch (rel.rel_type_case())
        {
            case substrait::Rel::kRead:
                return applyEmit(
                    rel.read().has_virtual_table() ? convertValues(rel.read()) : convertRead(rel.read()), rel.read().common());
            case substrait::Rel::kFilter: {
                auto input = convert(rel.filter().input());
                auto condition = convertExpression(rel.filter().condition(), input.names);
                input.plan = LogicalPlanBuilder::addSelection(std::move(condition), input.plan);
                return applyEmit(std::move(input), rel.filter().common());
            }
            case substrait::Rel::kProject:
                return convertProject(rel.project());
            case substrait::Rel::kSet:
                return applyEmit(convertUnion(rel.set()), rel.set().common());
            case substrait::Rel::kExtensionSingle:
                return applyEmit(convertWindowAggregate(rel.extension_single()), rel.extension_single().common());
            case substrait::Rel::kExtensionMulti:
                return applyEmit(convertWindowJoin(rel.extension_multi()), rel.extension_multi().common());
            default:
                throw UnsupportedQuery("Substrait relation {} is not supported", rel.ShortDebugString());
        }
    }

    [[nodiscard]] std::string catalogName(const std::string& name) const
    {
        const auto found = catalogNames.find(name);
        return found != catalogNames.end() ? found->second : name;
    }

    Relation convertRead(const substrait::ReadRel& read) const
    {
        if (not read.has_named_table() or read.named_table().names().empty() or read.has_filter() or read.has_projection())
        {
            throw UnsupportedQuery("only reads of named sources are supported: {}", read.ShortDebugString());
        }
        const auto& tableNames = read.named_table().names();
        Relation result{.plan = LogicalPlanBuilder::createLogicalPlan(toIdentifier(catalogName(tableNames.at(tableNames.size() - 1)))), .names = {}};
        for (const auto& name : read.base_schema().names())
        {
            result.names.push_back(toIdentifier(name));
        }
        return result;
    }

    /// Without emit, a Substrait project outputs its input followed by the expressions; emit selects from that list.
    Relation convertProject(const substrait::ProjectRel& project)
    {
        auto input = convert(project.input());
        std::vector<LogicalFunction> candidates;
        std::vector<std::optional<Identifier>> candidateNames;
        for (const auto& name : input.names)
        {
            candidates.push_back(field(name));
            candidateNames.emplace_back(name);
        }
        for (const auto& expression : project.expressions())
        {
            candidates.push_back(convertExpression(expression, input.names));
            /// Calcite emits pass-through columns as expressions, too: keep their names instead of inventing new ones.
            const auto fieldAccess = candidates.back().tryGetAs<UnboundFieldAccessLogicalFunction>();
            candidateNames.emplace_back(
                fieldAccess.has_value() ? std::make_optional(fieldAccess->get().getFieldName()) : std::optional<Identifier>{});
        }

        std::vector<int32_t> selected;
        if (project.common().has_emit())
        {
            selected.assign(project.common().emit().output_mapping().begin(), project.common().emit().output_mapping().end());
        }
        else
        {
            for (int32_t i = 0; i < static_cast<int32_t>(candidates.size()); ++i)
            {
                selected.push_back(i);
            }
        }

        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        Relation result{.plan = input.plan, .names = {}};
        std::unordered_set<std::string> used;
        for (const auto index : selected)
        {
            auto name = candidateNames.at(index).value_or(freshName());
            if (not used.insert(name.asCanonicalString()).second)
            {
                name = freshName();
            }
            projections.emplace_back(name, candidates.at(index));
            result.names.push_back(std::move(name));
        }
        result.plan = LogicalPlanBuilder::addProjection(std::move(projections), false, input.plan);
        return result;
    }

    Relation convertUnion(const substrait::SetRel& set)
    {
        if (set.op() != substrait::SetRel::SET_OP_UNION_ALL or set.inputs_size() < 2)
        {
            throw UnsupportedQuery("only UNION ALL is supported: {}", set.ShortDebugString());
        }
        auto result = convert(set.inputs(0));
        for (int i = 1; i < set.inputs_size(); ++i)
        {
            auto right = rename(convert(set.inputs(i)), result.names);
            result.plan = LogicalPlanBuilder::addUnion(std::move(result.plan), std::move(right.plan));
        }
        return result;
    }

    Relation convertWindowAggregate(const substrait::ExtensionSingleRel& extension)
    {
        nes::calcite::WindowAggregate spec;
        if (not extension.detail().UnpackTo(&spec))
        {
            throw UnsupportedQuery("unknown Substrait extension {}", extension.detail().type_url());
        }
        auto input = convert(extension.input());
        const auto& names = input.names;

        const auto windowType = toWindowType(spec.kind(), spec.size_ms(), spec.slide_ms());
        const auto timeCharacteristic = eventTime(
            spec.kind() == nes::calcite::WindowAggregate::GLOBAL ? addConstantTime(input.plan) : names.at(spec.time_field()));

        Relation result{.plan = input.plan, .names = {Identifier::parse("start"), Identifier::parse("end")}};
        std::vector<UnboundFieldAccessLogicalFunction> keys;
        for (const auto key : spec.group_keys())
        {
            keys.emplace_back(names.at(key));
            result.names.push_back(names.at(key));
        }

        std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> aggregations;
        std::unordered_map<std::string, DataType> calciteTypes; /// per aggregation output, see the cast below
        for (const auto& aggregate : spec.aggregates())
        {
            const auto access = TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{
                UnboundFieldAccessLogicalFunction{aggregate.has_field() ? names.at(aggregate.field()) : Identifier::parse("*")}};
            auto name = freshName();
            result.names.push_back(name);
            if (const auto type = calciteType(aggregate.type(), aggregate.nullable()))
            {
                calciteTypes.emplace(name.asCanonicalString(), *type);
            }
            else if (aggregate.type() != "VARCHAR" and aggregate.type() != "CHAR") /// e.g. MIN of a string: our type is right
            {
                throw UnsupportedQuery("aggregations of type {} are not supported", aggregate.type());
            }
            switch (aggregate.function())
            {
                case nes::calcite::WindowAggregate::SUM:
                    aggregations.push_back({.function = SumAggregationLogicalFunction{access}, .name = std::move(name)});
                    break;
                case nes::calcite::WindowAggregate::COUNT:
                    aggregations.push_back(
                        {.function = CountAggregationLogicalFunction{access, not aggregate.has_field()}, .name = std::move(name)});
                    break;
                case nes::calcite::WindowAggregate::MIN:
                    aggregations.push_back({.function = MinAggregationLogicalFunction{access}, .name = std::move(name)});
                    break;
                case nes::calcite::WindowAggregate::MAX:
                    aggregations.push_back({.function = MaxAggregationLogicalFunction{access}, .name = std::move(name)});
                    break;
                case nes::calcite::WindowAggregate::AVG:
                    aggregations.push_back({.function = AvgAggregationLogicalFunction{access}, .name = std::move(name)});
                    break;
                default:
                    throw UnsupportedQuery("unknown window aggregate {}", aggregate.ShortDebugString());
            }
        }
        /// Calcite deduplicates IN/EXISTS subqueries with an aggregation without aggregates; ours needs at least one.
        if (aggregations.empty())
        {
            aggregations.push_back(
                {.function = CountAggregationLogicalFunction{
                     TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{Identifier::parse("*")}}, true},
                 .name = freshName()});
        }
        result.plan = LogicalPlanBuilder::addWindowAggregation(
            std::move(result.plan), windowType, std::move(aggregations), std::move(keys), timeCharacteristic, spec.emit_on_empty_input());

        /// Our aggregations type their results differently from Calcite (an unsigned COUNT, an unsigned SUM of untyped
        /// literals, a DOUBLE AVG of integers), so their results are cast to Calcite's types.
        if (not calciteTypes.empty())
        {
            std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
            for (const auto& name : result.names)
            {
                const auto type = calciteTypes.find(name.asCanonicalString());
                projections.emplace_back(
                    name,
                    type != calciteTypes.end() ? LogicalFunction{CastToTypeLogicalFunction(type->second, field(name))} : field(name));
            }
            result.plan = LogicalPlanBuilder::addProjection(std::move(projections), false, result.plan);
        }
        return result;
    }

    /// GLOBAL (finite inputs only): one ingestion-time tumbling window long enough to hold every row, so it closes at the
    /// end of the stream. 10^15 ms are ~30,000 years: beyond any ingestion timestamp, far from overflowing window ends.
    static constexpr uint64_t GLOBAL_WINDOW_SIZE_MS = 1'000'000'000'000'000;

    static Windowing::TimeBasedWindowType toWindowType(const nes::calcite::WindowAggregate::Kind kind, const uint64_t sizeMs, const uint64_t slideMs)
    {
        switch (kind)
        {
            case nes::calcite::WindowAggregate::TUMBLING:
                return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{sizeMs}}};
            case nes::calcite::WindowAggregate::SLIDING:
                return Windowing::TimeBasedWindowType{
                    Windowing::SlidingWindow{Windowing::TimeMeasure{sizeMs}, Windowing::TimeMeasure{slideMs}}};
            case nes::calcite::WindowAggregate::GLOBAL:
                return Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{GLOBAL_WINDOW_SIZE_MS}}};
            default:
                throw UnsupportedQuery("unknown window kind {}", static_cast<int>(kind));
        }
    }

    static Windowing::UnboundTimeCharacteristic eventTime(const Identifier& field)
    {
        return Windowing::UnboundEventTimeCharacteristic{
            .field = TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{UnboundFieldAccessLogicalFunction{field}}};
    }

    /// GLOBAL windows have no time: every row gets the constant event time 0 in an extra column, so the window [0, size)
    /// holds every row and only the end-of-stream watermark closes it. (Ingestion time is wall-clock dependent: nested
    /// GLOBAL operators then saw rows older than a watermark another input had already advanced, and failed.)
    /// The column is not part of the relation's names; the root projection drops it.
    Identifier addConstantTime(LogicalPlan& plan)
    {
        auto name = freshName();
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        projections.emplace_back(
            name,
            CastToTypeLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE), rawLiteral("0")));
        plan = LogicalPlanBuilder::addProjection(std::move(projections), true, plan);
        return name;
    }

    /// Join outputs keep the plain input names and reject clashes, so rename columns whose name is taken.
    Relation avoidNames(Relation relation, std::unordered_set<std::string> taken)
    {
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        Relation result{.plan = relation.plan, .names = {}};
        bool renamed = false;
        for (const auto& name : relation.names)
        {
            auto output = taken.insert(name.asCanonicalString()).second ? name : freshName();
            renamed = renamed or output != name;
            projections.emplace_back(output, field(name));
            result.names.push_back(std::move(output));
        }
        if (not renamed)
        {
            return relation;
        }
        result.plan = LogicalPlanBuilder::addProjection(std::move(projections), false, relation.plan);
        return result;
    }

    Relation convertWindowJoin(const substrait::ExtensionMultiRel& extension)
    {
        nes::calcite::WindowJoin spec;
        if (not extension.detail().UnpackTo(&spec) or extension.inputs_size() != 2)
        {
            throw UnsupportedQuery("unknown Substrait extension {}", extension.detail().type_url());
        }
        /// Without a condition it is a cross product (of each window), which our joins only offer as an inner join.
        if (not spec.has_condition() and spec.type() != nes::calcite::WindowJoin::INNER)
        {
            throw UnsupportedQuery("an outer join needs a join condition besides the window");
        }
        const auto start = Identifier::parse("start");
        const auto end = Identifier::parse("end");
        auto left = avoidNames(convert(extension.inputs(0)), {start.asCanonicalString(), end.asCanonicalString()});
        std::unordered_set<std::string> taken{start.asCanonicalString(), end.asCanonicalString()};
        for (const auto& name : left.names)
        {
            taken.insert(name.asCanonicalString());
        }
        auto right = avoidNames(convert(extension.inputs(1)), std::move(taken));
        const bool global = spec.kind() == nes::calcite::WindowAggregate::GLOBAL;
        const auto leftTime = global ? addConstantTime(left.plan) : left.names.at(spec.left_time_field());
        const auto rightTime = global ? addConstantTime(right.plan) : right.names.at(spec.right_time_field());

        Relation result{.plan = left.plan, .names = {start, end}};
        result.names.insert(result.names.end(), left.names.begin(), left.names.end());
        result.names.insert(result.names.end(), right.names.begin(), right.names.end());
        const std::vector<Identifier> inputNames{result.names.begin() + 2, result.names.end()};

        const auto joinType = [&]
        {
            switch (spec.type())
            {
                case nes::calcite::WindowJoin::LEFT:
                    return JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN;
                case nes::calcite::WindowJoin::RIGHT:
                    return JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN;
                case nes::calcite::WindowJoin::FULL:
                    return JoinLogicalOperator::JoinType::OUTER_FULL_JOIN;
                default:
                    return spec.has_condition() ? JoinLogicalOperator::JoinType::INNER_JOIN : JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT;
            }
        }();
        result.plan = LogicalPlanBuilder::addJoin(
            std::move(left.plan),
            std::move(right.plan),
            spec.has_condition() ? convertExpression(spec.condition(), inputNames) : rawLiteral("true"),
            toWindowType(spec.kind(), spec.size_ms(), spec.slide_ms()),
            joinType,
            eventTime(leftTime),
            eventTime(rightTime),
            spec.residual() | std::views::transform([&](const auto& residual) { return convertExpression(residual, inputNames); })
                | std::ranges::to<std::vector>());

        /// Calcite chose the implementation; DecideJoinTypesRule keeps a trait that is already set.
        auto join = result.plan.getRootOperators().at(0);
        auto traits = join.getTraitSet();
        tryInsert(
            traits,
            JoinImplementationTypeTrait{
                spec.implementation() == nes::calcite::WindowJoin::HASH_JOIN ? JoinImplementation::HASH_JOIN
                                                                             : JoinImplementation::NESTED_LOOP_JOIN});
        result.plan = result.plan.withRootOperators({join.withTraitSet(std::move(traits))});
        return result;
    }

    Relation applyEmit(Relation relation, const substrait::RelCommon& common)
    {
        if (not common.has_emit())
        {
            return relation;
        }
        std::vector<Identifier> names;
        for (const auto index : common.emit().output_mapping())
        {
            names.push_back(relation.names.at(index));
        }
        return select(std::move(relation), names);
    }

    /// Projects `names` from the relation, renaming duplicates.
    Relation select(Relation relation, const std::vector<Identifier>& names)
    {
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        Relation result{.plan = relation.plan, .names = {}};
        std::unordered_set<std::string> used;
        for (const auto& name : names)
        {
            auto output = used.insert(name.asCanonicalString()).second ? name : freshName();
            projections.emplace_back(output, field(name));
            result.names.push_back(std::move(output));
        }
        result.plan = LogicalPlanBuilder::addProjection(std::move(projections), false, relation.plan);
        return result;
    }

    /// Union inputs must agree on column names, Substrait only requires them to agree on positions.
    static Relation rename(Relation relation, const std::vector<Identifier>& names)
    {
        if (relation.names == names)
        {
            return relation;
        }
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        for (size_t i = 0; i < names.size(); ++i)
        {
            projections.emplace_back(names.at(i), field(relation.names.at(i)));
        }
        return {.plan = LogicalPlanBuilder::addProjection(std::move(projections), false, relation.plan), .names = names};
    }

    LogicalFunction convertExpression(const substrait::Expression& expression, const std::vector<Identifier>& names)
    {
        switch (expression.rex_type_case())
        {
            case substrait::Expression::kSelection: {
                const auto& selection = expression.selection();
                if (not selection.has_direct_reference() or not selection.direct_reference().has_struct_field()
                    or selection.direct_reference().struct_field().has_child() or not selection.has_root_reference())
                {
                    throw UnsupportedQuery("only plain field references are supported: {}", selection.ShortDebugString());
                }
                return field(names.at(selection.direct_reference().struct_field().field()));
            }
            case substrait::Expression::kLiteral:
                return convertLiteral(expression.literal());
            case substrait::Expression::kCast:
                return CastToTypeLogicalFunction(
                    toDataType(expression.cast().type(), userDefinedTypes), convertExpression(expression.cast().input(), names));
            case substrait::Expression::kScalarFunction:
                return convertFunction(expression.scalar_function(), names);
            case substrait::Expression::kIfThen: {
                /// CASE WHEN: our Conditional function takes [condition1, result1, ..., default].
                const auto& ifThen = expression.if_then();
                if (not ifThen.has_else_())
                {
                    throw UnsupportedQuery("CASE without ELSE (which is NULL) is not supported");
                }
                std::vector<LogicalFunction> children;
                for (const auto& clause : ifThen.ifs())
                {
                    children.push_back(convertExpression(clause.if_(), names));
                    children.push_back(convertExpression(clause.then(), names));
                }
                children.push_back(convertExpression(ifThen.else_(), names));
                if (auto conditional = LogicalFunctionProvider::tryProvide("Conditional", std::move(children)))
                {
                    return *conditional;
                }
                throw UnsupportedQuery("CASE needs the Conditional function plugin");
            }
            default:
                throw UnsupportedQuery("Substrait expression {} is not supported", expression.ShortDebugString());
        }
    }

    LogicalFunction convertLiteral(const substrait::Expression::Literal& literal) const
    {
        switch (literal.literal_type_case())
        {
            case substrait::Expression::Literal::kBoolean:
                return rawLiteral(literal.boolean() ? "true" : "false");
            case substrait::Expression::Literal::kI8:
                return rawLiteral(std::to_string(literal.i8()));
            case substrait::Expression::Literal::kI16:
                return rawLiteral(std::to_string(literal.i16()));
            case substrait::Expression::Literal::kI32:
                return rawLiteral(std::to_string(literal.i32()));
            case substrait::Expression::Literal::kI64:
                return rawLiteral(std::to_string(literal.i64()));
            case substrait::Expression::Literal::kFp32:
                return rawLiteral(floatLiteral(literal.fp32()));
            case substrait::Expression::Literal::kFp64:
                return rawLiteral(floatLiteral(literal.fp64()));
            case substrait::Expression::Literal::kPrecisionTimestamp:
                return rawLiteral(std::to_string(literal.precision_timestamp().value()));
            case substrait::Expression::Literal::kString:
                return stringLiteral(literal.string());
            case substrait::Expression::Literal::kVarChar:
                return stringLiteral(literal.var_char().value());
            case substrait::Expression::Literal::kFixedChar:
                return stringLiteral(literal.fixed_char());
            case substrait::Expression::Literal::kDecimal:
                /// We have no exact decimal type; computing with doubles instead would give different results.
                throw UnsupportedQuery("DECIMAL values are not supported: {}", decimalToString(literal.decimal()));
            case substrait::Expression::Literal::kNull:
                /// Calcite types every NULL by its context (e.g. the other side of a comparison), and so does Substrait.
                return NullValueLogicalFunction(toDataType(literal.null(), userDefinedTypes));
            default:
                throw UnsupportedQuery("Substrait literal {} is not supported", literal.ShortDebugString());
        }
    }

    /// Our type for a Calcite SqlTypeName, if there is one.
    static std::optional<DataType> calciteType(const std::string& sqlTypeName, const bool nullable)
    {
        static const std::unordered_map<std::string, DataType::Type> types{
            {"TINYINT", DataType::Type::INT8},
            {"SMALLINT", DataType::Type::INT16},
            {"INTEGER", DataType::Type::INT32},
            {"BIGINT", DataType::Type::INT64},
            {"REAL", DataType::Type::FLOAT32},
            {"FLOAT", DataType::Type::FLOAT64},
            {"DOUBLE", DataType::Type::FLOAT64},
            {"UTINYINT", DataType::Type::UINT8},
            {"USMALLINT", DataType::Type::UINT16},
            {"UINTEGER", DataType::Type::UINT32},
            {"UBIGINT", DataType::Type::UINT64},
            {"BOOLEAN", DataType::Type::BOOLEAN}};
        const auto found = types.find(sqlTypeName);
        if (found == types.end())
        {
            return std::nullopt;
        }
        return DataTypeProvider::provideDataType(found->second, nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    }

    /// 16-byte little-endian two's complement unscaled value, e.g. 12345 with scale 2 is 123.45.
    static std::string decimalToString(const substrait::Expression::Literal::Decimal& decimal)
    {
        __int128 unscaled = 0;
        const auto& bytes = decimal.value();
        for (int i = static_cast<int>(bytes.size()) - 1; i >= 0; --i)
        {
            unscaled = (unscaled << 8) | static_cast<unsigned char>(bytes[i]);
        }
        const bool negative = unscaled < 0;
        auto magnitude = static_cast<unsigned __int128>(negative ? -unscaled : unscaled);
        std::string digits;
        do
        {
            digits.insert(digits.begin(), static_cast<char>('0' + static_cast<int>(magnitude % 10)));
            magnitude /= 10;
        } while (magnitude != 0);
        const auto scale = static_cast<size_t>(decimal.scale());
        if (scale > 0)
        {
            digits.insert(digits.begin(), scale + 1 > digits.size() ? scale + 1 - digits.size() : 0, '0');
            digits.insert(digits.end() - static_cast<std::ptrdiff_t>(scale), '.');
        }
        return negative ? "-" + digits : digits;
    }

    /// Keeps a float a float: 49.0 must not print as "49", which type inference would read as an integer.
    static std::string floatLiteral(const double value)
    {
        auto text = fmt::format("{}", value);
        if (text.find_first_of(".eEni") == std::string::npos)
        {
            text += ".0";
        }
        return text;
    }

    static LogicalFunction stringLiteral(const std::string& value)
    {
        std::string quoted = "'";
        for (const char character : value)
        {
            quoted += character;
            if (character == '\'')
            {
                quoted += '\'';
            }
        }
        return rawLiteral(quoted + "'");
    }

    LogicalFunction convertFunction(const substrait::Expression::ScalarFunction& function, const std::vector<Identifier>& names)
    {
        const auto found = functions.find(function.function_reference());
        if (found == functions.end())
        {
            throw UnsupportedQuery("Substrait function reference {} is not declared", function.function_reference());
        }
        const auto& name = found->second;
        std::vector<LogicalFunction> args;
        for (const auto& argument : function.arguments())
        {
            if (not argument.has_value())
            {
                throw UnsupportedQuery("function {} has a non-value argument", name);
            }
            args.push_back(convertExpression(argument.value(), names));
        }

        const auto binary = [&]<typename Function>()
        {
            if (args.size() != 2)
            {
                throw UnsupportedQuery("function {} expects two arguments, got {}", name, args.size());
            }
            return LogicalFunction{Function(args.at(0), args.at(1))};
        };
        /// AND/OR are variadic in Substrait.
        const auto fold = [&]<typename Function>()
        {
            auto result = args.at(0);
            for (size_t i = 1; i < args.size(); ++i)
            {
                result = Function(result, args.at(i));
            }
            return result;
        };

        if (name == "add")
        {
            return binary.operator()<AddLogicalFunction>();
        }
        if (name == "subtract")
        {
            return binary.operator()<SubLogicalFunction>();
        }
        if (name == "multiply")
        {
            return binary.operator()<MulLogicalFunction>();
        }
        if (name == "divide")
        {
            return binary.operator()<DivLogicalFunction>();
        }
        if (name == "modulus")
        {
            return binary.operator()<ModuloLogicalFunction>();
        }
        if (name == "equal")
        {
            return binary.operator()<EqualsLogicalFunction>();
        }
        if (name == "not_equal")
        {
            return NegateLogicalFunction(binary.operator()<EqualsLogicalFunction>());
        }
        if (name == "lt")
        {
            return binary.operator()<LessLogicalFunction>();
        }
        if (name == "lte")
        {
            return binary.operator()<LessEqualsLogicalFunction>();
        }
        if (name == "gt")
        {
            return binary.operator()<GreaterLogicalFunction>();
        }
        if (name == "gte")
        {
            return binary.operator()<GreaterEqualsLogicalFunction>();
        }
        if (name == "and" and not args.empty())
        {
            return fold.operator()<AndLogicalFunction>();
        }
        if (name == "or" and not args.empty())
        {
            return fold.operator()<OrLogicalFunction>();
        }
        if (name == "not" and args.size() == 1)
        {
            return NegateLogicalFunction(args.at(0));
        }
        if (name == "negate" and args.size() == 1)
        {
            return SubLogicalFunction(rawLiteral("0"), args.at(0));
        }
        if (name == "abs" and args.size() == 1)
        {
            return AbsoluteLogicalFunction(args.at(0));
        }
        if (name == "is_null" and args.size() == 1)
        {
            return IsNullCheckLogicalFunction(args.at(0));
        }
        if (name == "is_not_null" and args.size() == 1)
        {
            return NegateLogicalFunction(IsNullCheckLogicalFunction(args.at(0)));
        }
        throw UnsupportedQuery("Substrait function {} is not supported", name);
    }
};

}

LogicalPlan toLogicalPlan(
    const substrait::Plan& plan,
    const bool finiteInputs,
    std::optional<std::string> fallbackSource,
    std::unordered_map<std::string, std::string> catalogNames)
{
    if (plan.relations_size() != 1 or not plan.relations(0).has_root())
    {
        throw UnsupportedQuery("expected exactly one Substrait root relation, got {}", plan.relations_size());
    }
    return Converter{plan, finiteInputs, std::move(fallbackSource), std::move(catalogNames)}.convertRoot(plan.relations(0).root());
}

}
