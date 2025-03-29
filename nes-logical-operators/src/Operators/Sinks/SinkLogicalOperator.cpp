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

#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include <memory>
#include <utility>
#include <ErrorHandling.hpp>
#include "Operators/UnaryLogicalOperator.hpp"
#include <Plans/Operator.hpp>
#include "Sinks/SinkDescriptor.hpp"
#include "Util/Logger/Logger.hpp"

namespace NES
{

bool SinkLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const SinkLogicalOperator*>(&rhs)->id == id;
}

bool SinkLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SinkLogicalOperator*>(&rhs)) {
        return this->sinkName == rhsOperator->sinkName
            and ((this->sinkDescriptor) ? this->sinkDescriptor == rhsOperator->sinkDescriptor : true);
    }
    return false;
};

std::string SinkLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << fmt::format("SINK(opId: {}, sinkName: {}, sinkDescriptor: ", id, sinkName);
    ((sinkDescriptor) ? (ss << sinkDescriptor) : ss << "(null))");
    ss << ")";
    return ss.str();
}
bool SinkLogicalOperator::inferSchema()
{
    const auto result = UnaryLogicalOperator::inferSchema();

    if (result && sinkDescriptor)
    {
        sinkDescriptor->schema = this->outputSchema;
    }

    return result && sinkDescriptor;
}

const Sinks::SinkDescriptor& SinkLogicalOperator::getSinkDescriptorRef() const
{
    if (this->sinkDescriptor)
    {
        return *sinkDescriptor;
    }
    throw UnknownSinkType("Tried to access the SinkDescriptor of a SinkLogicalOperator that does not have a SinkDescriptor yet.");
}
std::shared_ptr<Sinks::SinkDescriptor> SinkLogicalOperator::getSinkDescriptor() const
{
    return sinkDescriptor;
}

std::shared_ptr<Operator> SinkLogicalOperator::clone() const
{
    ///We pass invalid worker id here because the properties will be copied later automatically.
    auto sinkDescriptorPtrCopy = sinkDescriptor;
    auto copy = std::make_shared<SinkLogicalOperator>(sinkName);
    copy->sinkDescriptor = std::move(sinkDescriptorPtrCopy);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
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

}
