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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalOperatorPtr PhysicalUnionOperator::create(OperatorId id, const SchemaPtr& schema)
{
    return create(id, schema, schema, schema);
}

PhysicalOperatorPtr
PhysicalUnionOperator::create(OperatorId id, const SchemaPtr& leftSchema, const SchemaPtr& rightSchema, const SchemaPtr& outputSchema)
{
    return std::make_shared<PhysicalUnionOperator>(id, leftSchema, rightSchema, outputSchema);
}

PhysicalOperatorPtr PhysicalUnionOperator::create(const SchemaPtr& schema)
{
    return create(getNextOperatorId(), schema);
}

PhysicalUnionOperator::PhysicalUnionOperator(
    OperatorId id, const SchemaPtr& leftSchema, const SchemaPtr& rightSchema, const SchemaPtr& outputSchema)
    : Operator(id), PhysicalBinaryOperator(id, leftSchema, rightSchema, outputSchema)
{
}

std::string PhysicalUnionOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalUnionOperator:\n";
    return out.str();
}
std::shared_ptr<Operator> PhysicalUnionOperator::copy()
{
    return create(id, leftInputSchema, rightInputSchema, outputSchema);
}

}
