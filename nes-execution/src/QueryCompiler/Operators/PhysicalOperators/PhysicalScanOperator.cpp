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
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalScanOperator::PhysicalScanOperator(OperatorId id, const std::shared_ptr<Schema>& outputSchema)
    : Operator(id), PhysicalUnaryOperator(id, outputSchema, outputSchema)
{
}

std::shared_ptr<PhysicalOperator> PhysicalScanOperator::create(const std::shared_ptr<Schema>& outputSchema)
{
    return create(getNextOperatorId(), std::move(outputSchema));
}

std::shared_ptr<PhysicalOperator> PhysicalScanOperator::create(OperatorId id, const std::shared_ptr<Schema>& outputSchema)
{
    return std::make_shared<PhysicalScanOperator>(id, outputSchema);
}

std::ostream& PhysicalScanOperator::toDebugString(std::ostream& os) const
{
    os << "\nPhysicalScanOperator:\n";
    return PhysicalUnaryOperator::toDebugString(os);
}

std::ostream& PhysicalScanOperator::toQueryPlanString(std::ostream& os) const
{
    os << "PhysicalScanOperator:";
    return PhysicalUnaryOperator::toQueryPlanString(os);
}

std::shared_ptr<Operator> PhysicalScanOperator::copy()
{
    auto result = create(id, outputSchema);
    result->addAllProperties(properties);
    return result;
}

}
