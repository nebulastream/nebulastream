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
#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_

#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {
/**
 * @brief Logical Binary operator, defines two output schemas
 */
class LogicalBinaryOperatorNode : public LogicalOperatorNode, public BinaryOperatorNode {
  public:
    explicit LogicalBinaryOperatorNode(OperatorId id);

    /**
     * Add operator part of left side
     * @param operatorId : operatorId of the left operator
     */
    void addLeftUpStreamOperatorId(OperatorId operatorId);

    /**
     * Get ids of the operators that are in the left
     * @return left operator ids
     */
    std::vector<OperatorId> getLeftUpStreamOperatorIds();

    /**
     * Add operator part of the right side
     * @param operatorId : operatorId of the right operator
     */
    void addRightUpStreamOperatorId(OperatorId operatorId);

    /**
     * Get ids of the operators that are in the right
     * @return right operator ids
     */
    std::vector<OperatorId> getRightUpStreamOperatorIds();

    /**
     * @brief remove operator id from left upstream operator ids
     * @param operatorId: operator id to remove
     */
    void removeLeftUpstreamOperatorId(OperatorId operatorId);

    /**
     * @brief remove operator id from right upstream operator ids
     * @param operatorId: operator id to remove
     */
    void removeRightUpstreamOperatorId(OperatorId operatorId);

    /**
     * Is the operator in left upstream operator
     * @param operatorId : operator id to check
     * @return true if exists else false
     */
    bool isLeftUpstreamOperatorId(OperatorId operatorId);

    /**
     * Is the operator in right upstream operator
     * @param operatorId : operator id to check
     * @return true if exists else false
     */
    bool isRightUpstreamOperatorId(OperatorId operatorId);

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;

    void inferInputOrigins() override;

    /**
     * @brief Get all left input operators.
     * @return std::vector<OperatorNodePtr>
     */
    std::vector<OperatorNodePtr> getLeftUpstreamOperators();

    /**
    * @brief Get all right input operators.
    * @return std::vector<OperatorNodePtr>
    */
    std::vector<OperatorNodePtr> getRightUpstreamOperators();

  private:

    /**
     * Get me all operator ids matching the operators in the list
     * @param operatorIds : Id of operators to fetch
     * @return vector of operators
     */
    std::vector<OperatorNodePtr> getOperatorsByIds(const std::vector<OperatorId>& operatorIds);

    //To store ids of the left and the right operators (Note: there can be more than one operator in the left or the right side
    // after source expansion)
    std::vector<OperatorId> leftUpStreamOperatorIds;
    std::vector<OperatorId> rightUpStreamOperatorIds;
};
}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_
