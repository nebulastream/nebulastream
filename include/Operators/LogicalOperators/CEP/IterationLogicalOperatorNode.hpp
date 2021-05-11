/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef ITERATION_LOGICAL_OPERATOR_NODE_HPP
#define ITERATION_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief iteration operator, which contains /TODO: the number of expected iterations (minimal,maximal)
 */
class IterationLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    explicit IterationLogicalOperatorNode(std::vector<ExpressionNodePtr> expressions, OperatorId id);
    ~IterationLogicalOperatorNode() = default;

    //TODO 1 operator specific function ?
    /**
    * @brief returns the number of iterations //TODO (n) = exactly, (n,0) min n, (0,m) = max and (n,m) = min to max
    * @return  std::vector<ExpressionNodePtr>
   */
    std::vector<ExpressionNodePtr> getNumberOfIterations();

    /**
     * @brief check if two operators have the same output schema
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    bool equal(const NodePtr rhs) const override;
    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    void inferStringSignature() override;

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    OperatorNodePtr copy() override;

  private:
    std::vector<ExpressionNodePtr> iterations;
};

}// namespace NES
#endif//ITERATION_LOGICAL_OPERATOR_NODE_HPP