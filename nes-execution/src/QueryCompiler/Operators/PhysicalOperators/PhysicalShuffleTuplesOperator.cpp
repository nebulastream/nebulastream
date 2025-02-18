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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalShuffleTuplesOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalShuffleTuplesOperator::PhysicalShuffleTuplesOperator(
    OperatorId id, const std::shared_ptr<Schema>& inputSchema, const float& unorderedness)
    : Operator(id), PhysicalUnaryOperator(id, inputSchema, inputSchema), unorderedness(unorderedness)
{
}

std::shared_ptr<PhysicalOperator> PhysicalShuffleTuplesOperator::create(std::shared_ptr<Schema> inputSchema, const float& unorderedness)
{
    return create(getNextOperatorId(), std::move(inputSchema), unorderedness);
}
std::shared_ptr<PhysicalOperator>
PhysicalShuffleTuplesOperator::create(OperatorId id, const std::shared_ptr<Schema>& inputSchema, const float& unorderedness)
{
    return std::make_shared<PhysicalShuffleTuplesOperator>(id, inputSchema, unorderedness);
}

float PhysicalShuffleTuplesOperator::getUnorderedness() const
{
    return unorderedness;
}

std::string PhysicalShuffleTuplesOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalShuffleTuplesOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

std::shared_ptr<Operator> PhysicalShuffleTuplesOperator::copy()
{
    auto result = create(id, inputSchema, unorderedness);
    result->addAllProperties(properties);
    return result;
}

}
