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

#include <Operators/Windows/BatchingLogicalOperator.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/CountBasedWindow.hpp>
#include <WindowTypes/Types/CountBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

BatchingLogicalOperator::BatchingLogicalOperator(std::shared_ptr<Windowing::WindowType> windowType)
    : windowType(std::move(windowType))
{
}

std::string_view BatchingLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool BatchingLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const BatchingLogicalOperator*>(&rhs))
    {
        return *getWindowType() == *rhsOperator->getWindowType()
            and getOutputSchema() == rhsOperator->outputSchema
            and getInputOriginIds() == rhsOperator->getInputOriginIds()
            and getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}

std::string BatchingLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Batching(opId: {}, outputOriginIds: [{}], windowType: {})",
            id,
            fmt::join(outputOriginIds, ", "),
            getWindowType()->toString());
    }
    return fmt::format("Batching()");
}

LogicalOperator BatchingLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    const auto& inputSchema = inputSchemas.at(0);
    auto copy = *this;
    copy.outputSchema = Schema{copy.outputSchema.memoryLayoutType};

    for (const auto& field : inputSchema.getFields())
    {
        copy.outputSchema.addField(field.name, field.dataType);
    }
    copy.windowType->inferStamp(inputSchema);
    return copy;
}

TraitSet BatchingLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator BatchingLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> BatchingLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema BatchingLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> BatchingLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> BatchingLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator BatchingLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Batching should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids;
    return copy;
}

LogicalOperator BatchingLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> BatchingLogicalOperator::getChildren() const
{
    return children;
}

std::shared_ptr<Windowing::WindowType> BatchingLogicalOperator::getWindowType() const
{
    return windowType;
}

SerializableOperator BatchingLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
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
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    WindowInfos windowInfo;
    if (auto countBasedWindowType = std::dynamic_pointer_cast<Windowing::CountBasedWindowType>(windowType))
    {
        if (auto countBasedWindow = std::dynamic_pointer_cast<Windowing::CountBasedWindow>(windowType))
        {
            auto* countBased = windowInfo.mutable_count_based_window();
            countBased->set_size(countBasedWindow->getSize());
        }
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterBatchingLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    PRECONDITION(arguments.inputSchemas.size() == 1, "Expected one input schema, but got {}", arguments.inputSchemas.size());
    auto windowInfoVariant = arguments.config[BatchingLogicalOperator::ConfigParameters::WINDOW_INFOS];

    std::shared_ptr<Windowing::WindowType> windowType;
    if (std::holds_alternative<WindowInfos>(windowInfoVariant))
    {
        auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
        if (windowInfoProto.has_count_based_window())
        {
            windowType = std::make_shared<Windowing::CountBasedWindow>(windowInfoProto.count_based_window().size());
        }
    }
    if (!windowType)
    {
        throw UnknownLogicalOperator();
    }

    auto logicalOperator = BatchingLogicalOperator(windowType);

    if (auto& id = arguments.id)
    {
        logicalOperator.id = *id;
    }

    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);
}

}
