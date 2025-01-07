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

#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical Map operator.
 */
class PhysicalMapOperator : public PhysicalUnaryOperator
{
public:
    PhysicalMapOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, NodeFunctionFieldAssignmentPtr mapFunction);
    static PhysicalOperatorPtr
    create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const NodeFunctionFieldAssignmentPtr& mapFunction);
    static PhysicalOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, NodeFunctionFieldAssignmentPtr mapFunction);

    std::shared_ptr<Operator> copy() override;

    /**
     * @brief Returns the function of this map operator
     * @return NodeFunctionFieldAssignmentPtr
     */
    NodeFunctionFieldAssignmentPtr getMapFunction();

protected:
    std::string toString() const override;

    const NodeFunctionFieldAssignmentPtr mapFunction;
};
}
