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

#include <memory>
#include <string>
#include <vector>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Model.hpp>

namespace NES::InferModel
{

/**
 * @brief Infer model operator
 */
class LogicalInferModelOperator : public LogicalUnaryOperator
{
public:
    LogicalInferModelOperator(OperatorId id, NES::Nebuli::Inference::Model, std::vector<std::shared_ptr<NodeFunction>> inputFields);

    /**
     * @brief copies the current operator node
     * @return a copy of this node
     */
    std::shared_ptr<Operator> copy() override;

    /**
     * @brief compares this operator node with another
     * @param rhs the other operator node
     * @return true if both are equal or false if both are not equal
     */
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;

    /**
     * @brief checks if the operator node is equal and also has the same id, so it is the identical node
     * @param rhs the other operator node
     * @return true if identical, false otherwise
     */
    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;

    /**
     * @brief infers the schema of the this operator node
     * @param typeInferencePhaseContext
     * @return true on success, false otherwise
     */
    bool inferSchema() override;

    /**
     * @brief getter for inputFieldsPtr
     * @return inputFieldsPtr
     */
    const std::vector<std::shared_ptr<NodeFunction>>& getInputFields() const;


    void inferStringSignature() override { }

    const Nebuli::Inference::Model& getModel() const { return model; }

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override { return os << "InferModel"; }

private:
    /**
     * @brief updates the field to a fully qualified one.
     * @param field
     */
    void updateToFullyQualifiedFieldName(const std::shared_ptr<NodeFunctionFieldAccess>& field) const;

    Nebuli::Inference::Model model;
    std::vector<std::shared_ptr<NodeFunction>> inputFields;
};


}
