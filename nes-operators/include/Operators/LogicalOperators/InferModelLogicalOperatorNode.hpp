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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_INFERMODELLOGICALOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_INFERMODELLOGICALOPERATORNODE_HPP_

#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES::InferModel {

/**
 * @brief Infer model operator
 */
class InferModelLogicalOperatorNode : public LogicalUnaryOperatorNode {

  public:
    InferModelLogicalOperatorNode(std::string model,
                                  std::vector<ExpressionNodePtr> inputFields,
                                  std::vector<ExpressionNodePtr> outputFields,
                                  OperatorId id);

    /**
     * @brief creates a string representation of this node
     * @return the string representation
     */
    std::string toString() const override;

    /**
     * @brief copies the current operator node
     * @return a copy of this node
     */
    OperatorNodePtr copy() override;

    /**
     * @brief compares this operator node with another
     * @param rhs the other operator node
     * @return true if both are equal or false if both are not equal
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    /**
     * @brief checks if the operator node is equal and also has the same id, so it is the identical node
     * @param rhs the other operator node
     * @return true if identical, false otherwise
     */
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;

    /**
     * @brief infers the schema of the this operator node
     * @param typeInferencePhaseContext
     * @return true on success, false otherwise
     */
    bool inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) override;

    /**
     * @brief infers the signature of this operator node
     */
    void inferStringSignature() override;

    /**
     * @brief getter for the model
     * @return model
     */
    const std::string& getModel() const;

    /**
     * @brief getter for the path to the deployed model
     * @return path to model
     */
    const std::string getDeployedModelPath() const;

    /**
     * @brief getter for inputFieldsPtr
     * @return inputFieldsPtr
     */
    const std::vector<ExpressionNodePtr>& getInputFields() const;

    /**
     * @brief getter for outputFieldsPtr
     * @return outputFieldsPtr
     */
    const std::vector<ExpressionNodePtr>& getOutputFields() const;

  private:
    /**
     * @brief updates the field to a fully qualified one.
     * @param field
     */
    void updateToFullyQualifiedFieldName(FieldAccessExpressionNodePtr field);

  private:
    std::string model;
    std::vector<ExpressionNodePtr> inputFields;
    std::vector<ExpressionNodePtr> outputFields;
};

}// namespace NES::InferModel

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_INFERMODELLOGICALOPERATORNODE_HPP_
