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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalUnaryOperator::PhysicalUnaryOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema)
    : Operator(id), PhysicalOperator(id), UnaryOperator(id)
{
    UnaryOperator::setInputSchema(std::move(inputSchema));
    UnaryOperator::setOutputSchema(std::move(outputSchema));
}

std::string PhysicalUnaryOperator::toString() const
{
    std::stringstream out;
    out << UnaryOperator::toString();
    return out.str();
}

}
