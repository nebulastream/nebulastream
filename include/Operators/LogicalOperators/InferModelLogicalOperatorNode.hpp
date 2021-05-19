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

#ifndef INFER_MODEL_LOGICAL_OPERATOR_NODE_HPP
#define INFER_MODEL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief Infer model operator
 */
class InferModelLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    InferModelLogicalOperatorNode(std::string model, std::vector<ExpressionItem> inputFields, std::vector<ExpressionItem> outputFields, OperatorId id);
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool equal(const NodePtr rhs) const override;
    bool isIdentical(NodePtr rhs) const override;
    bool inferSchema() override;
    void inferStringSignature() override;


  private:
    std::string model;
    std::vector<ExpressionItem> inputFields;
    std::vector<ExpressionItem> outputFields;
};

}// namespace NES

#endif// INFER_MODEL_LOGICAL_OPERATOR_NODE_HPP
