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
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;

    /**
     * @brief Get all left input operators.
     * @return std::vector<OperatorNodePtr>
     */
    std::vector<OperatorNodePtr> getLeftOperators();

    /**
    * @brief Get all right input operators.
    * @return std::vector<OperatorNodePtr>
    */
    std::vector<OperatorNodePtr> getRightOperators();

  private:
    std::vector<OperatorNodePtr> getOperatorsBySchema(const SchemaPtr& schema);
};
}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALBINARYOPERATORNODE_HPP_
