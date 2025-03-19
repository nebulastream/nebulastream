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

namespace NES
{
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(
    Sources::SourceDescriptor sourceDescriptor) : Operator(), UnaryLogicalOperator(), sourceDescriptor(std::move(sourceDescriptor))
{
}
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(
    Sources::SourceDescriptor sourceDescriptor, const OriginId originId)
    : Operator(), UnaryLogicalOperator(), sourceDescriptor(std::move(sourceDescriptor)), originIds(originId)
{
}

std::string_view SourceDescriptorLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool SourceDescriptorLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const SourceDescriptorLogicalOperator*>(&rhs)->id == id;
}

bool SourceDescriptorLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SourceDescriptorLogicalOperator*>(&rhs)) {
        return getSourceDescriptor()== rhsOperator->getSourceDescriptor();
    }
    return false;
}

std::string SourceDescriptorLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SOURCE(opId: " << id << ": originid: " << originIds.toString() << ", " << sourceDescriptor << ")";
    return ss.str();
}

Sources::SourceDescriptor SourceDescriptorLogicalOperator::getSourceDescriptor() const
{
    return sourceDescriptor;
}

Optimizer::OriginIdTrait& SourceDescriptorLogicalOperator::getOriginIds()()
{
    return originIds;
}

bool SourceDescriptorLogicalOperator::inferSchema()
{
    setInputSchema(sourceDescriptor.schema);
    setOutputSchema(sourceDescriptor.schema);
    return true;
}

std::unique_ptr<Operator> SourceDescriptorLogicalOperator::clone() const
{
    auto sourceDescriptorPtrCopy = sourceDescriptor;
    auto result = std::make_unique<SourceDescriptorLogicalOperator>(std::move(sourceDescriptorPtrCopy));
    result->get(originIds);
    result->inferSchema();
    return result;
}

std::vector<OriginId> SourceDescriptorLogicalOperator::getOutputOriginIds() const
{
    return originIds;
}

[[nodiscard]] SerializableOperator SourceDescriptorLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    const auto serializedSource = new SerializableOperator_SourceDescriptorLogicalOperator();

    const auto serializedSourceDescriptor
        = new SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor();

    SchemaSerializationUtil::serializeSchema(sourceDescriptor.schema, serializedSourceDescriptor->mutable_sourceschema());
    serializedSourceDescriptor->set_logicalsourcename(sourceDescriptor.logicalSourceName);
    serializedSourceDescriptor->set_sourcetype(sourceDescriptor.sourceType);

    /// Serialize parser config.
    auto* const serializedParserConfig = ParserConfig().New();
    serializedParserConfig->set_type(sourceDescriptor.parserConfig.parserType);
    serializedParserConfig->set_tupledelimiter(sourceDescriptor.parserConfig.tupleDelimiter);
    serializedParserConfig->set_fielddelimiter(sourceDescriptor.parserConfig.fieldDelimiter);
    serializedSourceDescriptor->set_allocated_parserconfig(serializedParserConfig);

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sourceDescriptor.config)
    {
        auto* kv = serializedSourceDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    serializedSource->set_allocated_sourcedescriptor(serializedSourceDescriptor);
    serializedOperator.set_allocated_source(serializedSource);

    return serializedOperator;
}

}
