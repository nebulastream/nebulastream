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

#include <string>
#include <string_view>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Statistics/LogicalStatisticStoreReadOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

std::string_view LogicalStatisticStoreReadOperator::getName() const noexcept
{
    return NAME;
}

bool LogicalStatisticStoreReadOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const LogicalStatisticStoreReadOperator*>(&rhs))
    {
        return true;
    }
    return false;
};

LogicalOperator LogicalStatisticStoreReadOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(inputSchemas.size() == 1, "StatisticStoreWrite should have one input but got {}", inputSchemas.size());

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Map operator");
        }
    }

    auto copy = *this;

    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;

    return copy;
}

LogicalOperator LogicalStatisticStoreReadOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet LogicalStatisticStoreReadOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator LogicalStatisticStoreReadOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> LogicalStatisticStoreReadOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema LogicalStatisticStoreReadOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> LogicalStatisticStoreReadOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> LogicalStatisticStoreReadOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator LogicalStatisticStoreReadOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Selection should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator LogicalStatisticStoreReadOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> LogicalStatisticStoreReadOperator::getChildren() const
{
    return children;
}

std::string LogicalStatisticStoreReadOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("{}(opId: {})", NAME, id);
    }
    return fmt::format("{}", NAME);
}

SerializableOperator LogicalStatisticStoreReadOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i])
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
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

DescriptorConfig::Config
LogicalStatisticStoreReadOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(config), NAME);
}


LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterStatStoreReadLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto logicalOperator = LogicalStatisticStoreReadOperator();
    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);
}

}
