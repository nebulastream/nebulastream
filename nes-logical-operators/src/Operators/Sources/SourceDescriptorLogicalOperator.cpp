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

#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(
    std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor) : sourceDescriptor(std::move(sourceDescriptor))
{
}

std::string_view SourceDescriptorLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalOperator SourceDescriptorLogicalOperator::withInferredSchema(std::vector<Schema>) const
{
    PRECONDITION(false, "Schema is already given by SourceDescriptor. No call ot InferSchema needed");
}

bool SourceDescriptorLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SourceDescriptorLogicalOperator*>(&rhs)) {
        return getSourceDescriptor()== rhsOperator->getSourceDescriptor();
    }
    return false;
}

std::string SourceDescriptorLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ", descriptor address:" << sourceDescriptor;
    ss << ", schema: " << sourceDescriptor->schema.toString();
    if (!inputOriginIds.empty())
    {
        ss << ", originId:" << inputOriginIds[0];
    }
    else
    {
        ss << ", originId: none";
    }
    ss << ")";
    return ss.str();
}

Optimizer::TraitSet SourceDescriptorLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator SourceDescriptorLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SourceDescriptorLogicalOperator::getInputSchemas() const
{
    return {sourceDescriptor->schema};
};

Schema SourceDescriptorLogicalOperator::getOutputSchema() const
{
    return sourceDescriptor->schema;
}

std::vector<std::vector<OriginId>> SourceDescriptorLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> SourceDescriptorLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator SourceDescriptorLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Source should have one input");
    PRECONDITION(ids[0].size() == 1, "Source should have one originId");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator SourceDescriptorLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> SourceDescriptorLogicalOperator::getChildren() const
{
    return children;
}

std::shared_ptr<Sources::SourceDescriptor> SourceDescriptorLogicalOperator::getSourceDescriptor() const
{
    return sourceDescriptor;
}

Sources::SourceDescriptor& SourceDescriptorLogicalOperator::getSourceDescriptorRef() const
{
    return *sourceDescriptor;
}

[[nodiscard]] SerializableOperator SourceDescriptorLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    const auto serializedSource = new SerializableOperator_SourceDescriptorLogicalOperator();

    const auto serializedSourceDescriptor
        = new SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor();

    SchemaSerializationUtil::serializeSchema(sourceDescriptor->schema, serializedSourceDescriptor->mutable_sourceschema());
    serializedSourceDescriptor->set_logicalsourcename(sourceDescriptor->logicalSourceName);
    serializedSourceDescriptor->set_sourcetype(sourceDescriptor->sourceType);

    /// Serialize parser config.
    auto* const serializedParserConfig = ParserConfig().New();
    serializedParserConfig->set_type(sourceDescriptor->parserConfig.parserType);
    serializedParserConfig->set_tupledelimiter(sourceDescriptor->parserConfig.tupleDelimiter);
    serializedParserConfig->set_fielddelimiter(sourceDescriptor->parserConfig.fieldDelimiter);
    serializedSourceDescriptor->set_allocated_parserconfig(serializedParserConfig);

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sourceDescriptor->config)
    {
        auto* kv = serializedSourceDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    serializedSource->set_allocated_sourcedescriptor(serializedSourceDescriptor);
    serializedOperator.set_allocated_source(serializedSource);

    return serializedOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSourceDescriptorLogicalOperator(LogicalOperatorRegistryArguments)
{
    throw UnsupportedOperation();
}

}
