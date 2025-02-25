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

#include <Functions/NodeFunction.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical projection operator.
 */
class PhysicalProjectOperator : public PhysicalUnaryOperator
{
public:
    PhysicalProjectOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<NodeFunctionPtr> functions);
    static PhysicalOperatorPtr
    create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const std::vector<NodeFunctionPtr>& functions);
    static PhysicalOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<NodeFunctionPtr> functions);
    /**
     * @brief returns the list of fields that remain in the output schema.
     * @return  std::vector<NodeFunctionPtr>
     */
    std::vector<NodeFunctionPtr> getFunctions();
    std::shared_ptr<Operator> copy() override;

protected:
    std::string toString() const override;

private:
    std::vector<NodeFunctionPtr> functions;
};

}
