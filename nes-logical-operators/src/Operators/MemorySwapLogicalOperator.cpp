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

#include <Operators/MemorySwapLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{
/// currently we assume inputSchema is equal to outputSchema

MemorySwapLogicalOperator::MemorySwapLogicalOperator(
    const Schema& inputSchema, MemoryLayoutType inputMemoryLayoutType, MemoryLayoutType outputMemoryLayoutType)
    : inputSchema(inputSchema)
    , outputSchema(inputSchema)
    , inputMemoryLayoutType(std::move(inputMemoryLayoutType))
    , outputMemoryLayoutType(std::move(outputMemoryLayoutType))
{
}

MemorySwapLogicalOperator::MemorySwapLogicalOperator(
    const Schema& inputSchema,
    MemoryLayoutType inputMemoryLayoutType,
    MemoryLayoutType outputMemoryLayoutType,
    ParserConfig inputFormatterConfig)
    : inputSchema(inputSchema)
    , outputSchema(inputSchema)
    , inputMemoryLayoutType(std::move(inputMemoryLayoutType))
    , outputMemoryLayoutType(std::move(outputMemoryLayoutType))
    , inputFormatterConfig(std::move(inputFormatterConfig))
{
}

ParserConfig MemorySwapLogicalOperator::getInputFormatterConfig() const
{
    return inputFormatterConfig;
}

std::string_view MemorySwapLogicalOperator::getName() const noexcept
{
    return NAME;
}

MemoryLayoutType MemorySwapLogicalOperator::getInputMemoryLayoutType() const
{
    return inputMemoryLayoutType;
}

MemoryLayoutType MemorySwapLogicalOperator::getOutputMemoryLayoutType() const
{
    return outputMemoryLayoutType;
}

bool MemorySwapLogicalOperator::operator==(const MemorySwapLogicalOperator& rhs) const
{
    return inputMemoryLayoutType == rhs.inputMemoryLayoutType && outputMemoryLayoutType == rhs.outputMemoryLayoutType
        && getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
};

std::string MemorySwapLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "MemorySwap(opId: {}, inputMemoryLayoutType: {}, outputMemoryLayoutType: {}, traitSet: {})",
            opId,
            magic_enum::enum_name(inputMemoryLayoutType),
            magic_enum::enum_name(outputMemoryLayoutType),
            traitSet.explain(verbosity));
    }
    return fmt::format("MemorySwap({} to {})", magic_enum::enum_name(inputMemoryLayoutType), magic_enum::enum_name(outputMemoryLayoutType));
}

MemorySwapLogicalOperator MemorySwapLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotDeserialize("MemorySwap should have at least one input");
    }

    const auto& firstSchema = inputSchemas.at(0);
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for MemorySwap operator");
        }
    }

    copy.inputMemoryLayoutType = inputMemoryLayoutType;
    copy.outputMemoryLayoutType = outputMemoryLayoutType;

    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    return copy;
}

TraitSet MemorySwapLogicalOperator::getTraitSet() const
{
    return traitSet;
}

MemorySwapLogicalOperator MemorySwapLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

MemorySwapLogicalOperator MemorySwapLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> MemorySwapLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema MemorySwapLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> MemorySwapLogicalOperator::getChildren() const
{
    return children;
}

void MemorySwapLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& input : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(input, schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }
    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterMemorySwapLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    const auto inputLayoutVariant = arguments.config.at(MemorySwapLogicalOperator::ConfigParameters::INPUT_MEMORY_LAYOUT_TYPE);
    const auto outputLayoutVariant = arguments.config.at(MemorySwapLogicalOperator::ConfigParameters::OUTPUT_MEMORY_LAYOUT_TYPE);

    if (std::holds_alternative<EnumWrapper>(inputLayoutVariant) && std::holds_alternative<EnumWrapper>(outputLayoutVariant))
    {
        auto inputLayout = std::get<EnumWrapper>(inputLayoutVariant).asEnum<MemoryLayoutType>().value();
        auto outputLayout = std::get<EnumWrapper>(outputLayoutVariant).asEnum<MemoryLayoutType>().value();

        auto logicalOperator = MemorySwapLogicalOperator(Schema{}, inputLayout, outputLayout);
        return logicalOperator.withInferredSchema(arguments.inputSchemas);
    }

    throw UnknownLogicalOperator();
}

}
