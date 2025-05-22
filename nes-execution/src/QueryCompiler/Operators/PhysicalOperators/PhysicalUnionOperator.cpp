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
#include <ostream>
#include <sstream>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

std::shared_ptr<PhysicalOperator> PhysicalUnionOperator::create(OperatorId id, const std::shared_ptr<Schema>& schema)
{
    return create(id, schema, schema, schema);
}

std::shared_ptr<PhysicalOperator> PhysicalUnionOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& leftSchema,
    const std::shared_ptr<Schema>& rightSchema,
    const std::shared_ptr<Schema>& outputSchema)
{
    return std::make_shared<PhysicalUnionOperator>(id, leftSchema, rightSchema, outputSchema);
}

std::shared_ptr<PhysicalOperator> PhysicalUnionOperator::create(const std::shared_ptr<Schema>& schema)
{
    return create(getNextOperatorId(), schema);
}

PhysicalUnionOperator::PhysicalUnionOperator(
    OperatorId id,
    const std::shared_ptr<Schema>& leftSchema,
    const std::shared_ptr<Schema>& rightSchema,
    const std::shared_ptr<Schema>& outputSchema)
    : Operator(id), PhysicalBinaryOperator(id, leftSchema, rightSchema, outputSchema)
{
}

std::ostream& PhysicalUnionOperator::toDebugString(std::ostream& os) const
{
    return os << "\nPhysicalUnionOperator:\n";
}

std::ostream& PhysicalUnionOperator::toQueryPlanString(std::ostream& os) const
{
    return os << "PhysicalUnionOperator";
}

std::shared_ptr<Operator> PhysicalUnionOperator::copy()
{
    return create(id, leftInputSchema, rightInputSchema, outputSchema);
}

}
