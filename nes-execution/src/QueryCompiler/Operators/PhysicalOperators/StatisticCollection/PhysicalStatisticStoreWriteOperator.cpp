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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalStatisticStoreWriteOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalStatisticStoreWriteOperator::PhysicalStatisticStoreWriteOperator(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
{
}

std::shared_ptr<PhysicalOperator> PhysicalStatisticStoreWriteOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema)
{
    return std::make_shared<PhysicalStatisticStoreWriteOperator>(id, inputSchema, outputSchema);
}

std::shared_ptr<PhysicalOperator> PhysicalStatisticStoreWriteOperator::create(
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema));
}

std::string PhysicalStatisticStoreWriteOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalStatisticStoreWriteOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << std::endl;
    return out.str();
}

std::shared_ptr<Operator> PhysicalStatisticStoreWriteOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema);
    result->addAllProperties(properties);
    return result;
}

}
