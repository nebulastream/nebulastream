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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Map operator.
 */
class PhysicalMapOperator : public PhysicalUnaryOperator {
  public:
    PhysicalMapOperator(OperatorId id,
                        SchemaPtr inputSchema,
                        SchemaPtr outputSchema,
                        FieldAssignmentExpressionNodePtr mapExpression);
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const FieldAssignmentExpressionNodePtr& mapExpression);
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, FieldAssignmentExpressionNodePtr mapExpression);
    std::string toString() const override;
    OperatorNodePtr copy() override;

    /**
     * @brief Returns the expression of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    FieldAssignmentExpressionNodePtr getMapExpression();

  protected:
    const FieldAssignmentExpressionNodePtr mapExpression;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPOPERATOR_HPP_
