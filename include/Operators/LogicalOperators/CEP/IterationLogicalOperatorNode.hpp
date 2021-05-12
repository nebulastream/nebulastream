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
 * @brief iteration operator, which contains the number of expected iterations (minimal,maximal)
 * all possible cases: (n) = exactly, (n,0) min n, (0,m) = max and (n,m) = min to max
 */
class IterationLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    explicit IterationLogicalOperatorNode(std::uint64_t minIterations, std::uint64_t maxIterations, OperatorId id);
    ~IterationLogicalOperatorNode() = default;

    /**
    * @brief returns the minimal amount of iterations
    * @return amount of iterations
    */
    std::uint64_t getMinIterations();

    /**
   * @brief returns the maximal amount of iterations
   * @return amount of iterations
   */
    std::uint64_t getMaxIterations();

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
    std::uint64_t minIterations;
    std::uint64_t maxIterations;
};

}// namespace NES
#endif//ITERATION_LOGICAL_OPERATOR_NODE_HPP

