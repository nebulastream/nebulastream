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

#include <memory>
#include <string_view>
#include <utility>
#include <variant>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>

namespace NES
{

SinkLogicalOperator::SinkLogicalOperator(std::string sinkName) : sinkName(std::move(sinkName)) {};

bool SinkLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SinkLogicalOperator*>(&rhs))
    {
        return this->sinkName == rhsOperator->sinkName and *this->sinkDescriptor == *rhsOperator->sinkDescriptor;
    }
    return false;
};

std::string SinkLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << fmt::format("SINK(opId: {}, sinkName: {}, sinkDescriptor: [", id, sinkName);
    if (sinkDescriptor) {
        ss << "type=" << sinkDescriptor->sinkType 
           << ", addTimestamp=" << sinkDescriptor->addTimestamp
           << ", schema=" << sinkDescriptor->schema.toString()
           << "]";
    } else {
        ss << "]";
    }
    if (!inputOriginIds.empty())
    {
        ss << ", input originId:" << inputOriginIds[0];
    }
    else
    {
        ss << ", input originId: none";
    }
    ss << ")";
    return ss.str();
}

std::string_view SinkLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalOperator SinkLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 1, "Sink should have only one input");
    const auto& inputSchema = inputSchemas[0];
    copy.sinkDescriptor->schema = inputSchema;
    copy.inputSchema = inputSchema;
    copy.outputSchema = inputSchema;
    return copy;
}

Optimizer::TraitSet SinkLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator SinkLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SinkLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SinkLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> SinkLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> SinkLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator SinkLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Sink should have one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator SinkLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> SinkLogicalOperator::getChildren() const
{
    return children;
}

void SinkLogicalOperator::setOutputSchema(Schema schema)
{
    outputSchema = std::move(schema);
}

std::string SinkLogicalOperator::getSinkName() const
{
    return sinkName;
}

SerializableOperator SinkLogicalOperator::serialize() const
{
    SerializableOperator_SinkLogicalOperator proto;
    INVARIANT(sinkDescriptor, "Sink descriptor should not be null");
    proto.mutable_sinkdescriptor()->CopyFrom(sinkDescriptor->serialize());

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());

    serializableOperator.mutable_sink()->CopyFrom(proto);
    return serializableOperator;
}

}
