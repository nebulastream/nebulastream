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

#include <LogicalInferModelOperator.hpp>
#include <Model.hpp>

#include "LogicalOperatorRegistry.hpp"

namespace NES::InferModel
{

LogicalInferModelOperator::LogicalInferModelOperator(Nebuli::Inference::Model model, std::vector<LogicalFunction> inputFields)
    : model(std::move(model)), inputFields(std::move(inputFields))
{
}

std::string LogicalInferModelOperator::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "INFER_MODEL(opId: {}, inputFields: [{}])",
        id,
        fmt::join(std::views::transform(inputFields, [&](const auto& field) { return field.explain(verbosity); }), ", "));
}
SerializableOperator LogicalInferModelOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(getName());
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    for (const auto& inputSchema : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    for (const auto& originList : getInputOriginIds())
    {
        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originList)
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);


    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }
    FunctionList funcList;
    for (const auto& inputField : inputFields)
    {
        *funcList.add_functions() = inputField.serialize();
    }
    (*serializableOperator.mutable_config())["inputFields"] = Configurations::descriptorConfigTypeToProto(funcList);

    serializeModel(model, *(*serializableOperator.mutable_config())["MODEL"].mutable_model());
    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperator LogicalInferModelOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
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
    auto logicalOperator = InferModel::LogicalInferModelOperator(model, functions);

    if (auto& id = arguments.id)
    {
        logicalOperator.id = *id;
    }

    auto logicalOp = logicalOperator.withInputOriginIds(arguments.inputOriginIds)
                         .withOutputOriginIds(arguments.outputOriginIds)
                         .get<InferModel::LogicalInferModelOperator>()
                         .setInputSchema(arguments.inputSchemas)
                         .setOutputSchema(arguments.outputSchema);
    return logicalOp;
}
