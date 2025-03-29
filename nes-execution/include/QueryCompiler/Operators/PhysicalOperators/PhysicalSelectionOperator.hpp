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
#pragma once

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical selection operator.
 */
class PhysicalSelectionOperator : public PhysicalUnaryOperator
{
public:
    PhysicalSelectionOperator(
        OperatorId id, std::shared_ptr<Schema> inputSchema, std::shared_ptr<Schema> outputSchema, std::shared_ptr<NodeFunction> predicate);
    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        const std::shared_ptr<NodeFunction>& function);
    static std::shared_ptr<PhysicalOperator> create(
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        const std::shared_ptr<NodeFunction>& function);
    std::shared_ptr<Operator> copy() override;
    std::shared_ptr<NodeFunction> getPredicate();

protected:
    std::string toString() const override;

private:
    std::shared_ptr<NodeFunction> predicate;
};
}
