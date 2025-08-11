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

#include <Operators/Windows/Aggregations/Synopsis/Sample/ReservoirProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

ReservoirProbeLogicalOperator::ReservoirProbeLogicalOperator(FieldAccessLogicalFunction asField)
    : asField(asField), sampleSchema(std::nullopt)
{
}

std::string_view ReservoirProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ReservoirProbeLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    const auto* other = dynamic_cast<const ReservoirProbeLogicalOperator*>(&rhs);
    return other != nullptr;
};

LogicalOperator ReservoirProbeLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(inputSchemas.size() == 1, "ReservoirProbe should have one input schema but got {}", inputSchemas.size());

    auto copy = *this;

    copy.inputSchema = inputSchemas[0];

    auto asFieldName = copy.asField.getFieldName();
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
    {
        copy.asField = copy.asField.withFieldName(asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        copy.asField = copy.asField.withFieldName(copy.inputSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator() + asFieldName)
                               .get<FieldAccessLogicalFunction>();
    }

    copy.outputSchema = Schema{inputSchemas[0].memoryLayoutType};
    for (auto field : copy.inputSchema)
    {
        auto fieldWithoutStream = field.name.substr(field.name.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        auto asFieldWithoutStream = asFieldName.substr(asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        if (fieldWithoutStream != asFieldWithoutStream)
        {
            copy.outputSchema.addField(field.name, field.dataType);
        }
    }
    /// Accessing the last time the stream was not yet "sampled" to get the sample's schema.
    /// TODO This might not be a great solution.
    auto aggSchema = children.front().get<WindowedAggregationLogicalOperator>().getInputSchemas().front();
    copy.sampleSchema = Schema{inputSchemas[0].memoryLayoutType};
    for (auto field : aggSchema.getFields())
    {
        if (not copy.outputSchema.contains(field.name))
        {
            copy.outputSchema.addField(field.name, field.dataType);
        }
        copy.sampleSchema.value().addField(field.name, field.dataType);
    }

    return copy;
}

TraitSet ReservoirProbeLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator ReservoirProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> ReservoirProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema ReservoirProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> ReservoirProbeLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> ReservoirProbeLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator ReservoirProbeLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "ReservoirProbe should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator ReservoirProbeLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> ReservoirProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string ReservoirProbeLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("RESERVOIR_PROBE(opId: {})", id);
    }
    return fmt::format("RESERVOIR_PROBE");
}

SerializableOperator ReservoirProbeLogicalOperator::serialize() const
{
    /// TODO Not yet adapted to reservoirProbe.
    SerializableOperator serializableOperator;
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterReservoirProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    /// TODO Implement this function when its use is implemented.
    (void)arguments;

    throw UnknownLogicalOperator();
}

}
