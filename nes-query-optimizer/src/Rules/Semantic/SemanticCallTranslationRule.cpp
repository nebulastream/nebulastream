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

#include <Rules/Semantic/SemanticCallTranslationRule.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/AsyncOutputLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Functions/SemanticAggAggregationLogicalFunction.hpp>
#include <Functions/SemanticMatchLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/JsonAggAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/SemanticFusionRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Util/UUID.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

Identifier nameOf(const UnqualifiedUnboundField& field)
{
    return static_cast<const Identifier&>(field.getFullyQualifiedName());
}

std::optional<RegisteredSemanticModel> semanticModelOf(const LogicalOperator& op)
{
    if (const auto map = op.tryGetAs<SemanticMapLogicalOperator>())
    {
        return map->get().getModel();
    }
    if (const auto filter = op.tryGetAs<SemanticFilterLogicalOperator>())
    {
        return filter->get().getModel();
    }
    return std::nullopt;
}

/// The child field a model's INPUT field reads, bound: the rule runs after type inference.
FieldAccessLogicalFunction
inputField(const LogicalOperator& child, const UnqualifiedUnboundField& input, const RegisteredSemanticModel& model)
{
    const auto field = child.getOutputSchema()[input.getFullyQualifiedName()];
    if (!field.has_value())
    {
        throw CannotInferSchema("Semantic model '{}' reads field '{}', which its input does not have", model.getName(), nameOf(input));
    }
    return FieldAccessLogicalFunction{*field};
}

/// Keeps the rows of `evaluated` whose verdicts are all `'true'`, then drops the verdicts again by naming the `kept` fields,
/// in that order.
LogicalOperator
selectOnVerdicts(const LogicalOperator& evaluated, const std::vector<Identifier>& verdicts, const std::vector<Identifier>& kept)
{
    std::optional<LogicalFunction> predicate;
    for (const auto& verdict : verdicts)
    {
        const LogicalFunction passed = EqualsLogicalFunction{
            FieldAccessLogicalFunction{*evaluated.getOutputSchema()[verdict]},
            ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::VARSIZED), "true"}};
        predicate = predicate ? LogicalFunction{AndLogicalFunction{*predicate, passed}} : passed;
    }
    const LogicalOperator filtered = SelectionLogicalOperator::create(evaluated, *predicate);

    std::vector<ProjectionLogicalOperator::UnboundProjection> restored;
    restored.reserve(kept.size());
    for (const auto& field : kept)
    {
        restored.emplace_back(field, FieldAccessLogicalFunction{*filtered.getOutputSchema()[field]});
    }
    return ProjectionLogicalOperator::create(filtered, restored, ProjectionLogicalOperator::Asterisk{false});
}

/// The SEM_CALL payload of a model, which reads its INPUT fields in declared order.
SemanticCallPayload payloadOf(const RegisteredSemanticModel& model)
{
    SemanticCallPayload payload{.model = model.getName(), .config = model.getConfig(), .inputFields = {}, .outputFields = {}};
    for (const auto& input : model.getSchema().inputs)
    {
        payload.inputFields.push_back(nameOf(input).asCanonicalString());
    }
    for (const auto& output : model.getSchema().outputs)
    {
        payload.outputFields.push_back(nameOf(output).asCanonicalString());
    }
    return payload;
}

/// SEM_SCAN and SEM_WINDOW: one SEM_CALL per row whose first argument is the key, which the async framework orders the
/// calls by; WINDOW's second is the timestamp. Each OUTPUT field gets one of the call's values.
LogicalOperator translateKeyed(const RegisteredSemanticModel& model, const LogicalOperator& child)
{
    const auto& keyed = *model.getConfig().keyed;
    auto payload = payloadOf(model);
    /// The state is per translated operator: two queries over the same model must not share it.
    payload.stateId = UUIDToString(generateUUID());
    const auto varsized = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    const auto fieldOrNothing = [&](const std::string& name) -> LogicalFunction
    {
        if (name.empty())
        {
            return ConstantValueLogicalFunction{varsized, ""};
        }
        return inputField(child, UnqualifiedUnboundField{Identifier::parse(name), DataType::Type::VARSIZED}, model);
    };
    std::vector<LogicalFunction> callArguments{ConstantValueLogicalFunction{varsized, encodeSemanticCallPayload(payload)}};
    callArguments.push_back(fieldOrNothing(keyed.keyBy));
    if (keyed.kind == SemanticKeyedConfig::Kind::WINDOW)
    {
        callArguments.push_back(fieldOrNothing(keyed.timestampField));
    }
    for (const auto& input : model.getSchema().inputs)
    {
        callArguments.emplace_back(inputField(child, input, model));
    }
    const auto call = LogicalFunctionProvider::provide("SEM_CALL", std::move(callArguments));
    std::vector<ProjectionLogicalOperator::UnboundProjection> evaluation;
    size_t index = 0;
    for (const auto& output : model.getSchema().outputs)
    {
        evaluation.emplace_back(nameOf(output), index == 0 ? call : LogicalFunction{AsyncOutputLogicalFunction{call, index}});
        ++index;
    }
    return ProjectionLogicalOperator::create(child, evaluation, ProjectionLogicalOperator::Asterisk{true});
}

/// The operators a semantic operator over `child` turns into. They produce the semantic operator's output schema: the
/// child's fields followed by the model's OUTPUT fields.
LogicalOperator translate(const RegisteredSemanticModel& model, const LogicalOperator& child)
{
    if (model.getConfig().keyed)
    {
        return translateKeyed(model, child);
    }
    /// BATCH_SIZE rows per prompt are formed by the async framework (`AsyncFunction::maxBatchSize`).
    const auto& steps = model.getConfig().steps;
    const auto& outputs = model.getSchema().outputs;

    const auto payload = payloadOf(model);
    /// SEM_CALL's first argument is its payload, the others are the model's INPUT fields.
    std::vector<LogicalFunction> callArguments{
        ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::VARSIZED), encodeSemanticCallPayload(payload)}};
    for (const auto& input : model.getSchema().inputs)
    {
        callArguments.emplace_back(inputField(child, input, model));
    }
    const auto call = LogicalFunctionProvider::provide("SEM_CALL", std::move(callArguments));
    const auto value = [&call](const size_t step) -> LogicalFunction
    { return step == 0 ? call : LogicalFunction{AsyncOutputLogicalFunction{call, step}}; };

    /// Every value of the call is computed in one projection, the map outputs under their OUTPUT names and the filter
    /// verdicts under helper names. Equal calls are evaluated once, so this is one request per row and one async stage.
    /// A selection whose predicate called SEM_CALL itself would get a stage of its own, and a projection above it would
    /// call again for any value the predicate did not read: the lifting rule reuses a predicate's call only for the very
    /// value it read.
    std::vector<ProjectionLogicalOperator::UnboundProjection> evaluation;
    std::vector<Identifier> verdicts;
    size_t mapStep = 0;
    for (size_t step = 0; step < steps.size(); ++step)
    {
        if (steps[step].kind == SemanticStep::Kind::MAP)
        {
            INVARIANT(mapStep < std::ranges::size(outputs), "Semantic model '{}' has more MAP steps than OUTPUT fields", model.getName());
            evaluation.emplace_back(nameOf(*std::ranges::next(std::ranges::begin(outputs), mapStep++)), value(step));
            continue;
        }
        verdicts.push_back(Identifier::parse(fmt::format("__SEM_VERDICT_{}", verdicts.size())));
        evaluation.emplace_back(verdicts.back(), value(step));
    }
    LogicalOperator evaluated = ProjectionLogicalOperator::create(child, evaluation, ProjectionLogicalOperator::Asterisk{true});
    if (verdicts.empty())
    {
        return evaluated;
    }

    std::vector<Identifier> kept;
    for (const auto& field : child.getOutputSchema())
    {
        kept.push_back(field.getLastName());
    }
    for (const auto& output : outputs)
    {
        kept.push_back(nameOf(output));
    }
    return selectOnVerdicts(evaluated, verdicts, kept);
}

/// The conjuncts of a predicate, nested ANDs flattened.
void collectConjuncts(const LogicalFunction& function, std::vector<LogicalFunction>& conjuncts)
{
    if (function.tryGetAs<AndLogicalFunction>())
    {
        for (const auto& child : function.getChildren())
        {
            collectConjuncts(child, conjuncts);
        }
        return;
    }
    conjuncts.push_back(function);
}

bool containsSemanticMatch(const LogicalFunction& function)
{
    return std::ranges::any_of(
        BFSRange{function}, [](const LogicalFunction& node) { return node.tryGetAs<SemanticMatchLogicalFunction>().has_value(); });
}

/// SEM_JOIN, see the class comment. `children` are the join's children as the visitor returns them.
LogicalOperator translateJoin(const JoinLogicalOperator& join, std::vector<LogicalOperator> children, const SemanticModelCatalog* catalog)
{
    if (join.getJoinType() != JoinLogicalOperator::JoinType::INNER_JOIN)
    {
        throw UnsupportedQuery(
            "SEM_MATCH is supported in an INNER JOIN only, but the join is {}", magic_enum::enum_name(join.getJoinType()));
    }
    std::vector<LogicalFunction> conjuncts;
    collectConjuncts(join.getJoinFunction(), conjuncts);
    std::vector<SemanticMatchLogicalFunction> matches;
    std::optional<LogicalFunction> rest;
    for (const auto& conjunct : conjuncts)
    {
        if (const auto match = conjunct.tryGetAs<SemanticMatchLogicalFunction>())
        {
            matches.push_back(match.value().get());
            continue;
        }
        /// The verdict is computed above the join, so it can only stand for a conjunct of the whole condition.
        if (containsSemanticMatch(conjunct))
        {
            throw UnsupportedQuery("SEM_MATCH must be a top-level conjunct of the join condition, but it is part of {}", conjunct);
        }
        rest = rest ? LogicalFunction{AndLogicalFunction{*rest, conjunct}} : conjunct;
    }

    /// The other conjuncts stay in the join and are rebound to its children by name; without any, every pair of the window
    /// is a candidate.
    const LogicalOperator remaining = JoinLogicalOperator::create(
        std::array{children.at(0), children.at(1)},
        rest.value_or(ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::BOOLEAN), "true"}),
        join.getWindowType(),
        join.getJoinType(),
        join.getJoinTimeCharacteristics());

    std::vector<ProjectionLogicalOperator::UnboundProjection> evaluation;
    std::vector<Identifier> verdicts;
    for (const auto& match : matches)
    {
        if (catalog == nullptr || !catalog->hasModel(match.getModelName()))
        {
            throw UnknownSemanticModelName("Semantic model '{}' is not registered", match.getModelName());
        }
        const auto model = catalog->load(match.getModelName());
        const auto& config = model.getConfig();
        if (!model.hasFilterStep() || config.steps.size() != 1 || std::ranges::size(model.getSchema().inputs) != 2)
        {
            throw InvalidSemanticModel(
                "SEM_MATCH needs a filter model (no OUTPUT) with two INPUT fields, but semantic model '{}' has {} INPUT and {} OUTPUT "
                "fields",
                model.getName(),
                std::ranges::size(model.getSchema().inputs),
                std::ranges::size(model.getSchema().outputs));
        }
        auto payload = payloadOf(model);
        payload.join = true;
        std::vector<LogicalFunction> callArguments{
            ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::VARSIZED), encodeSemanticCallPayload(payload)},
            match.getLeft().withInferredDataType(remaining.getOutputSchema()),
            match.getRight().withInferredDataType(remaining.getOutputSchema())};
        verdicts.push_back(Identifier::parse(fmt::format("__SEM_VERDICT_{}", verdicts.size())));
        evaluation.emplace_back(verdicts.back(), LogicalFunctionProvider::provide("SEM_CALL", std::move(callArguments)));
    }
    const LogicalOperator evaluated = ProjectionLogicalOperator::create(remaining, evaluation, ProjectionLogicalOperator::Asterisk{true});

    std::vector<Identifier> kept;
    for (const auto& field : remaining.getOutputSchema())
    {
        kept.push_back(field.getLastName());
    }
    return selectOnVerdicts(evaluated, verdicts, kept);
}

bool containsSemanticAgg(const WindowedAggregationLogicalOperator& window)
{
    return std::ranges::any_of(
        window.getWindowAggregation(),
        [](const auto& aggregation)
        { return aggregation.function.template tryGetAs<SemanticAggAggregationLogicalFunction>().has_value(); });
}

/// SEM_AGG, see the class comment. `children` are the window's children as the visitor returns them.
LogicalOperator translateAggregation(
    const WindowedAggregationLogicalOperator& window, std::vector<LogicalOperator> children, const SemanticModelCatalog* catalog)
{
    std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> aggregations;

    struct Helper
    {
        Identifier field;
        std::string payload;
        Identifier name;
    };

    std::vector<Helper> helpers;
    for (const auto& aggregation : window.getWindowAggregation())
    {
        const auto semanticAgg = aggregation.function.tryGetAs<SemanticAggAggregationLogicalFunction>();
        if (!semanticAgg)
        {
            aggregations.push_back(aggregation);
            continue;
        }
        const auto& modelName = semanticAgg.value()->getModelName();
        if (catalog == nullptr || !catalog->hasModel(modelName))
        {
            throw UnknownSemanticModelName("Semantic model '{}' is not registered", modelName);
        }
        const auto model = catalog->load(modelName);
        if (model.hasFilterStep() || model.getConfig().steps.size() != 1 || std::ranges::size(model.getSchema().inputs) != 1
            || std::ranges::size(model.getSchema().outputs) != 1)
        {
            throw InvalidSemanticModel(
                "SEM_AGG needs a map model with one INPUT and one OUTPUT field, but semantic model '{}' has {} INPUT and {} OUTPUT "
                "fields{}",
                model.getName(),
                std::ranges::size(model.getSchema().inputs),
                std::ranges::size(model.getSchema().outputs),
                model.hasFilterStep() ? " and no OUTPUT clause" : "");
        }
        auto payload = payloadOf(model);
        payload.agg = true;
        const auto helper = Identifier::parse(fmt::format("__SEM_AGG_{}", helpers.size()));
        helpers.push_back({.field = helper, .payload = encodeSemanticCallPayload(payload), .name = aggregation.name});
        aggregations.push_back({.function = JsonAggAggregationLogicalFunction{semanticAgg.value()->getInputFunction()}, .name = helper});
    }

    /// The window computes JSON_AGG in place of every SEM_AGG; the other aggregations stay as they are.
    const LogicalOperator aggregated = WindowedAggregationLogicalOperator::create(
        children.at(0), window.getGroupingKeysWithName(), aggregations, window.getWindowType(), window.getCharacteristic());

    std::vector<ProjectionLogicalOperator::UnboundProjection> evaluation;
    evaluation.reserve(helpers.size());
    for (const auto& helper : helpers)
    {
        evaluation.emplace_back(
            helper.name,
            LogicalFunctionProvider::provide(
                "SEM_CALL",
                {ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::VARSIZED), helper.payload},
                 FieldAccessLogicalFunction{*aggregated.getOutputSchema()[helper.field]}}));
    }
    const LogicalOperator evaluated = ProjectionLogicalOperator::create(aggregated, evaluation, ProjectionLogicalOperator::Asterisk{true});

    /// Drops the helpers again: the window's output with each SEM_AGG under its own name.
    std::vector<ProjectionLogicalOperator::UnboundProjection> restored;
    for (const auto& field : evaluated.getOutputSchema())
    {
        if (std::ranges::any_of(helpers, [&field](const Helper& helper) { return helper.field == field.getLastName(); }))
        {
            continue;
        }
        restored.emplace_back(field.getLastName(), FieldAccessLogicalFunction{field});
    }
    return ProjectionLogicalOperator::create(evaluated, restored, ProjectionLogicalOperator::Asterisk{false});
}

}

LogicalPlan SemanticCallTranslationRule::apply(const LogicalPlan& queryPlan) const
{
    /// The up-context records whether a subtree was rewritten, so that only the spine above gets relinked.
    using TranslationVisitor = PlanVisitor<std::monostate, std::monostate, bool>;
    TranslationVisitor visitor{
        [this](const LogicalOperator& op, std::vector<LogicalOperator> children, std::unordered_map<LogicalOperator, bool> upContexts)
            -> TranslationVisitor::UpResult
        {
            if (const auto model = semanticModelOf(op))
            {
                PRECONDITION(children.size() == 1, "Expected {} to have one child, but has {}", op.getName(), children.size());
                return {translate(*model, children.front()), true};
            }
            if (const auto window = op.tryGetAs<WindowedAggregationLogicalOperator>(); window && containsSemanticAgg(window->get()))
            {
                return {translateAggregation(window->get(), std::move(children), semanticModelCatalog.get()), true};
            }
            if (const auto join = op.tryGetAs<JoinLogicalOperator>(); join && containsSemanticMatch(join->get().getJoinFunction()))
            {
                return {translateJoin(join->get(), std::move(children), semanticModelCatalog.get()), true};
            }
            if (std::ranges::none_of(upContexts, [](const auto& entry) { return entry.second; }))
            {
                return {op, false};
            }
            return {op.withChildren(std::move(children)), true};
        }};
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemanticCallTranslationRule::needs() const
{
    /// The translation reads the child's schema, so that it can drop the filter verdicts again with an explicit projection.
    return {typeid(SemanticFusionRule), typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemanticCallTranslationRule::neededBy() const
{
    /// AsyncFunctionLiftingRule, which comes after the barrier, lifts the calls this rule emits.
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType SemanticCallTranslationRule::create(PlanRuleRegistryArguments arguments)
{
    return SemanticCallTranslationRule{arguments.semanticModelCatalog};
}

}
