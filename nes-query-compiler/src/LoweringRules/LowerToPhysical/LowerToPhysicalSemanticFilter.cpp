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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalSemanticFilter.hpp>

#include <cstdlib>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <SemanticBackendFactory.hpp>
#include <SemanticFilterPhysicalOperator.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// Lowering runs on the worker that executes the query, which is where the credential lives: the
/// catalog entry only names the environment variable, so the secret never enters the coordinator's
/// catalog, the serialized plan or the wire. Resolved once per deployment, not per request.
std::optional<std::string> resolveApiKey(const RegisteredSemanticModel& model)
{
    const auto& variable = model.getConfig().apiKeyEnvVar;
    if (!variable.has_value())
    {
        return std::nullopt;
    }
    const char* const value = std::getenv(variable->c_str());
    if (value == nullptr)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}' reads its API key from environment variable {}, which is not set on this worker",
            model.getName(),
            *variable);
    }
    return std::string{value};
}

}

LoweringRuleResultSubgraph LowerToPhysicalSemanticFilter::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<SemanticFilterLogicalOperator>(), "Expected a SemanticFilterLogicalOperator");
    const auto semanticFilterOp = logicalOperator.getAs<SemanticFilterLogicalOperator>();
    const auto& model = semanticFilterOp.get().getModel();

    const auto toIdList = [](const auto& fields)
    {
        return fields
            | std::views::transform([](const UnqualifiedUnboundField& field)
                                    { return static_cast<QualifiedIdentifier>(field.getFullyQualifiedName()); })
            | std::ranges::to<std::vector>();
    };
    for (const auto& field : model.getSchema().inputs)
    {
        PRECONDITION(
            field.getDataType().isType(DataType::Type::VARSIZED),
            "SemanticFilter INPUT field '{}' must be VARSIZED",
            field.getFullyQualifiedName());
    }
    /// Only a fused filter declares OUTPUT fields, one per MAP step.
    for (const auto& field : model.getSchema().outputs)
    {
        PRECONDITION(
            field.getDataType().isType(DataType::Type::VARSIZED),
            "SemanticFilter OUTPUT field '{}' must be VARSIZED",
            field.getFullyQualifiedName());
    }

    /// One backend per worker thread, created when the pipeline starts: an HTTP backend owns a
    /// non-thread-safe curl handle.
    SemanticBackendProvider backendProvider
        = [config = model.getConfig(), apiKey = resolveApiKey(model)] { return SemanticBackendFactory::create(config, apiKey); };
    auto physicalOperator = SemanticFilterPhysicalOperator(
        std::move(backendProvider), model.getConfig(), toIdList(model.getSchema().inputs), toIdList(model.getSchema().outputs));

    NES_DEBUG("Lowering SemanticFilter operator for model '{}' to SemanticFilterPhysicalOperator", model.getName())

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto physicalOutputSchema = createPhysicalOutputSchema(logicalOperator.getTraitSet());
    const auto physicalInputSchema = createPhysicalOutputSchema(semanticFilterOp.get().getChildren().at(0).getTraitSet());

    /// Stage 1 needs no OperatorHandler: the operator owns its per-thread backends, as InferModel
    /// owns its per-thread runtimes.
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        physicalInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}

}
