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
#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
namespace NES::QueryCompilation::PhysicalOperators
{

std::shared_ptr<PhysicalOperator> PhysicalDemultiplexOperator::create(OperatorId id, const Schema& inputSchema)
{
    return std::make_shared<PhysicalDemultiplexOperator>(id, inputSchema);
}
std::shared_ptr<PhysicalOperator> PhysicalDemultiplexOperator::create(const Schema& inputSchema)
{
    return create(getNextOperatorId(), std::move(inputSchema));
}

PhysicalDemultiplexOperator::PhysicalDemultiplexOperator(OperatorId id, const Schema& inputSchema)
    : Operator(id), PhysicalUnaryOperator(id, inputSchema, inputSchema)
{
}

std::string PhysicalDemultiplexOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalDemultiplexOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

std::shared_ptr<Operator> PhysicalDemultiplexOperator::copy()
{
    return create(id, inputSchema);
}

}
