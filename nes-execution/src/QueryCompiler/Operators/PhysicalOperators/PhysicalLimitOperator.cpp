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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalLimitOperator::PhysicalLimitOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t limit)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), limit(limit)
{
}

PhysicalOperatorPtr
PhysicalLimitOperator::create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, uint64_t limit)
{
    return std::make_shared<PhysicalLimitOperator>(id, inputSchema, outputSchema, limit);
}

uint64_t PhysicalLimitOperator::getLimit()
{
    return limit;
}

PhysicalOperatorPtr PhysicalLimitOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t limit)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), limit);
}

std::string PhysicalLimitOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalLimitOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << "limit: " << limit;
    out << std::endl;
    return out.str();
}

std::shared_ptr<Operator> PhysicalLimitOperator::copy()
{
    return create(id, inputSchema, outputSchema, limit);
}

}
