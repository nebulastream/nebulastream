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

#ifndef INFER_MODEL_LOGICAL_OPERATOR_NODE_HPP
#define INFER_MODEL_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

namespace NES {

/**
 * @brief Infer model operator
 */
class InferModelLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    InferModelLogicalOperatorNode(std::string model, std::vector<ExpressionItemPtr> inputFields, std::vector<ExpressionItemPtr> outputFields, OperatorId id);
    std::string toString() const override;
    OperatorNodePtr copy() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    bool inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) override;
    void inferStringSignature() override;
    const std::string& getModel() const;
    const std::string getDeployedModelPath() const;
    const std::vector<ExpressionItemPtr>& getInputFieldsAsPtr();
    const std::vector<ExpressionItemPtr>& getOutputFieldsAsPtr();

  private:
    std::string model;
    std::vector<ExpressionItemPtr> inputFieldsPtr;
    std::vector<ExpressionItemPtr> outputFieldsPtr;
    void updateToFullyQualifiedFieldName(FieldAccessExpressionNodePtr field);
};

}// namespace NES

#endif// INFER_MODEL_LOGICAL_OPERATOR_NODE_HPP
