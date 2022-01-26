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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_CEP_ITERATIONLOGICALOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_CEP_ITERATIONLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief iteration operator, which contains the number of expected iterations (minimal,maximal) for an event of one stream
 * i.e., how often a duplicate event can (need to) appear before it fits the pattern
 * all possible cases:
 *  1. (n) = exactly n times an event appears per time window
 *  2. (n,0) = at least n times an event appears per time window, regardless how much more
 *  3. (0,m) = maximal m times an event appears per time window
 *  4. (n,m) = at least n times an event appears and not more than m times per time window
 */
class IterationLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    explicit IterationLogicalOperatorNode(uint64_t minIterations, uint64_t maxIterations, OperatorId id);
    ~IterationLogicalOperatorNode() override = default;

    /**
    * @brief returns the minimal amount of iterations
    * @return amount of iterations
    */
    uint64_t getMinIterations() const noexcept;

    /**
   * @brief returns the maximal amount of iterations
   * @return amount of iterations
   */
    uint64_t getMaxIterations() const noexcept;

    /**
     * @brief check if two operators of the same class are equivalent, here: equal number of minIterations and maxIterations
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    std::string toString() const override;
    void inferStringSignature() override;
    bool inferSchema() override;
    OperatorNodePtr copy() override;

  private:
    const uint64_t minIterations;
    const uint64_t maxIterations;
};

}// namespace NES
#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_CEP_ITERATIONLOGICALOPERATORNODE_HPP_
