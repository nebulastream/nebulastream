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
#include <vector>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalProjectOperator::PhysicalProjectOperator(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    std::vector<std::shared_ptr<NodeFunction>> functions)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), functions(std::move(functions))
{
}

std::shared_ptr<PhysicalOperator> PhysicalProjectOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::vector<std::shared_ptr<NodeFunction>>& functions)
{
    return std::make_shared<PhysicalProjectOperator>(id, inputSchema, outputSchema, functions);
}

std::shared_ptr<PhysicalOperator> PhysicalProjectOperator::create(
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::vector<std::shared_ptr<NodeFunction>>& functions)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(functions));
}

std::vector<std::shared_ptr<NodeFunction>> PhysicalProjectOperator::getFunctions()
{
    return functions;
}

std::ostream& PhysicalProjectOperator::toDebugString(std::ostream& os) const
{
    os << "\nPhysicalProjectOperator:\n";
    return PhysicalUnaryOperator::toDebugString(os);
}

std::ostream& PhysicalProjectOperator::toQueryPlanString(std::ostream& os) const
{
    os << "PhysicalProjectOperator:";
    return PhysicalUnaryOperator::toQueryPlanString(os);
}

std::shared_ptr<Operator> PhysicalProjectOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getFunctions());
    result->addAllProperties(properties);
    return result;
}

}
