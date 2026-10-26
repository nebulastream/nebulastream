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
#include <Functions/SemanticMatchLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/SemanticFusionRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
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

/// The operators a semantic operator over `child` turns into. They produce the semantic operator's output schema: the
/// child's fields followed by the model's OUTPUT fields.
LogicalOperator translate(const RegisteredSemanticModel& model, const LogicalOperator& child)
{
    /// Batching several rows into one prompt is left to the SQL interface (see the async framework's design doc).
    if (model.getConfig().batchSize != 1)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}' sets BATCH_SIZE {}, but SEM_CALL sends one row per prompt", model.getName(), model.getConfig().batchSize);
    }
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
    const LogicalOperator evaluated = ProjectionLogicalOperator::create(child, evaluation, ProjectionLogicalOperator::Asterisk{true});
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
        if (config.batchSize != 1)
        {
            throw InvalidSemanticModel(
                "Semantic model '{}' sets BATCH_SIZE {}, but SEM_CALL sends one pair per prompt", model.getName(), config.batchSize);
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
