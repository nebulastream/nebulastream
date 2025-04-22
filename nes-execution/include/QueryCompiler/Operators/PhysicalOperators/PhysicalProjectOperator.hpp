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

#include <memory>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical projection operator.
 */
class PhysicalProjectOperator : public PhysicalUnaryOperator
{
public:
    PhysicalProjectOperator(OperatorId id, Schema inputSchema, Schema outputSchema, std::vector<std::shared_ptr<NodeFunction>> functions);
    static std::shared_ptr<PhysicalOperator>
    create(OperatorId id, Schema inputSchema, Schema outputSchema, const std::vector<std::shared_ptr<NodeFunction>>& functions);
    static std::shared_ptr<PhysicalOperator>
    create(Schema inputSchema, Schema outputSchema, const std::vector<std::shared_ptr<NodeFunction>>& functions);
    /**
     * @brief returns the list of fields that remain in the output schema.
     * @return  std::vector<std::shared_ptr<NodeFunction>>
     */
    std::vector<std::shared_ptr<NodeFunction>> getFunctions();
    std::shared_ptr<Operator> copy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    std::vector<std::shared_ptr<NodeFunction>> functions;
};

}
