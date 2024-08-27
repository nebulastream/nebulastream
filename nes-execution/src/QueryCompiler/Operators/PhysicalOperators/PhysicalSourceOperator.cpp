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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalSourceOperator::PhysicalSourceOperator(
    OperatorId id,
    OriginId originId,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , sourceDescriptor(std::move(sourceDescriptor))
    , originId(originId)
{
}

std::shared_ptr<PhysicalSourceOperator> PhysicalSourceOperator::create(
    OperatorId id,
    OriginId originId,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
{
    return std::make_shared<PhysicalSourceOperator>(id, originId, inputSchema, outputSchema, std::move(sourceDescriptor));
}

std::shared_ptr<PhysicalSourceOperator>
PhysicalSourceOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
{
    return create(getNextOperatorId(), INVALID_ORIGIN_ID, std::move(inputSchema), std::move(outputSchema), std::move(sourceDescriptor));
}

OriginId PhysicalSourceOperator::getOriginId()
{
    return originId;
}

void PhysicalSourceOperator::setOriginId(OriginId originId)
{
    this->originId = originId;
}

std::unique_ptr<Sources::SourceDescriptor> PhysicalSourceOperator::getSourceDescriptor()
{
    return std::move(sourceDescriptor);
}

std::string PhysicalSourceOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalSourceOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << sourceDescriptor;
    out << "originId: " << originId;
    out << std::endl;
    return out.str();
}
OperatorPtr PhysicalSourceOperator::copy()
{
    auto exception = InvalidUseOfOperatorFunction();
    exception.what() += "PhysicalSourceOperator does not support copy, because holds a unique pointer to a SourceDescriptor.";
    throw exception;
}

} /// namespace NES::QueryCompilation::PhysicalOperators
