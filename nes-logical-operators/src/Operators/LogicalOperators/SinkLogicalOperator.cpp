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

#include "Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp"
#include <memory>
#include <utility>
#include "ErrorHandling.hpp"
#include "Operators/LogicalOperators/UnaryLogicalOperator.hpp"
#include "Operators/Operator.hpp"
#include "Sinks/SinkDescriptor.hpp"
#include "Util/Logger/Logger.hpp"

namespace NES
{

bool SinkLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const SinkLogicalOperator*>(&rhs)->getId() == id;
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
    auto copy = std::make_shared<SinkLogicalOperator>(sinkName, id);
    copy->sinkDescriptor = std::move(sinkDescriptorPtrCopy);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}
