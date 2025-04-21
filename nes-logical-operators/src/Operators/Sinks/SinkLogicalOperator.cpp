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

#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <memory>
#include <string_view>
#include <utility>
#include <ErrorHandling.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

bool SinkLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SinkLogicalOperator*>(&rhs)) {
        return this->sinkName == rhsOperator->sinkName and this->sinkDescriptor == rhsOperator->sinkDescriptor;
    }
    return false;
};

std::string SinkLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << fmt::format("SINK(opId: {}, sinkName: {}, sinkDescriptor: ", id, sinkName);
    ss << sinkDescriptor;
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
    SerializableOperator serializedOperator;

    const auto serializedSink = new SerializableOperator_SinkLogicalOperator();

    const auto serializedSinkDescriptor
        = new SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor();
    SchemaSerializationUtil::serializeSchema(std::move(this->outputSchema), serializedSinkDescriptor->mutable_sinkschema());
    serializedSinkDescriptor->set_sinktype(sinkDescriptor->sinkType);
    serializedSinkDescriptor->set_addtimestamp(sinkDescriptor->addTimestamp);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sinkDescriptor->config)
    {
        auto* kv = serializedSinkDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    serializedSink->set_allocated_sinkdescriptor(serializedSinkDescriptor);

    serializedOperator.set_allocated_sink(serializedSink);
    return serializedOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSinkLogicalOperator(LogicalOperatorRegistryArguments)
{
    throw UnsupportedOperation();
}

}
