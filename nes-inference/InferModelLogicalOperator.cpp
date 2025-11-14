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

#include <filesystem>
#include <memory>
#include <ostream>
#include <ranges>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>

#include <InferModelLogicalOperator.hpp>
#include <Model.hpp>

#include "LogicalOperatorRegistry.hpp"

namespace NES::InferModel
{

InferModelLogicalOperator::InferModelLogicalOperator(Nebuli::Inference::Model model, std::vector<LogicalFunction> inputFields)
    : model(std::move(model)), inputFields(std::move(inputFields))
{
}

const Nebuli::Inference::Model& InferModelLogicalOperator::getModel() const
{
    return model;
}

const std::vector<LogicalFunction>& InferModelLogicalOperator::getInputFields() const
{
    return inputFields;
}

bool InferModelLogicalOperator::operator==(const InferModelLogicalOperator& rhs) const
{
    return model == rhs.model && inputFields == rhs.inputFields && getInputSchemas() == rhs.getInputSchemas()
        && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

void InferModelLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& inputSchema : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }
    FunctionList funcList;
    for (const auto& inputField : inputFields)
    {
        *funcList.add_functions() = inputField.serialize();
    }
    (*serializableOperator.mutable_config())["inputFields"] = descriptorConfigTypeToProto(funcList);

    serializeModel(model, *(*serializableOperator.mutable_config())["MODEL"].mutable_model());
    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

InferModelLogicalOperator InferModelLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet InferModelLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelLogicalOperator InferModelLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Expected exactly one child");
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<LogicalOperator> InferModelLogicalOperator::getChildren() const
{
    return children;
}

std::vector<Schema> InferModelLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
}
Schema InferModelLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::string InferModelLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    return fmt::format(
        "INFER_MODEL(opId: {}, inputFields: [{}])",
        opId,
        fmt::join(std::views::transform(inputFields, [&](const auto& field) { return field.explain(verbosity); }), ", "));
}

std::string_view InferModelLogicalOperator::getName() const noexcept
{
    return NAME;
}

InferModelLogicalOperator InferModelLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    PRECONDITION(inputSchemas.size() == 1, "Expected exactly one input schema");

    auto copy = *this;
    const auto& firstSchema = inputSchemas.at(0);
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Selection operator");
        }
    }
    copy.inputFields = std::views::transform(inputFields, [&](const auto& field) { return field.withInferredDataType(firstSchema); })
        | std::ranges::to<std::vector>();
    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;

    if (inputFields.size() != model.getInputs().size())
    {
        throw CannotInferSchema("Model expects {} inputs, but received {}", copy.inputFields.size(), model.getInputs().size());
    }

    for (const auto& [field, expectedType] : std::views::zip(copy.inputFields, model.getInputs()))
    {
        if (field.getDataType() != expectedType)
        {
            throw CannotInferSchema("Model Expected '{}', but received {}", expectedType, field.getDataType());
        }
    }

    for (const auto& [name, type] : model.getOutputs())
    {
        if (copy.outputSchema.getFieldByName(name))
        {
            /// The assigned field is part of the current schema.
            /// Thus we check if it has the correct type.
            NES_TRACE("Infer Model Logical Operator: the field {} is already in the schema, so we updated its type.", name);
            auto _ = copy.outputSchema.replaceTypeOfField(name, type);
        }
        else
        {
            /// The assigned field is not part of the current schema.
            /// Thus we extend the schema by the new attribute.
            NES_TRACE("Infer Model Logical Operator: the field {} is not part of the schema, so we added it.", name);
            copy.outputSchema.addField(name, type);
        }
    }
    return copy;
}

}

NES::LogicalOperatorRegistryReturnType
NES::LogicalOperatorGeneratedRegistrar::RegisterInferenceModelLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto functions = std::get<FunctionList>(arguments.config.at("inputFields")).functions()
        | std::views::transform([](const auto& serializedFunction)
                                { return FunctionSerializationUtil::deserializeFunction(serializedFunction); })
        | std::ranges::to<std::vector>();

    auto model = Nebuli::Inference::deserializeModel(std::get<SerializableModel>(arguments.config.at("MODEL")));

    auto logicalOperator = InferModel::InferModelLogicalOperator(model, functions);
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}
