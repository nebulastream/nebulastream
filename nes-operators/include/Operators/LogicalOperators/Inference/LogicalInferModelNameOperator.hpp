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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES::InferModel
{

/**
 * @brief Infer model operator
 */
class LogicalInferModelNameOperator : public LogicalUnaryOperator
{
    std::string modelName;
    std::vector<std::shared_ptr<NodeFunction>> inputFields;

public:
    LogicalInferModelNameOperator(
        OperatorId id,
        std::string modelName,
        std::vector<std::shared_ptr<NodeFunction>> inputFields)
        : Operator(id)
        , LogicalUnaryOperator(id)
        , modelName(std::move(modelName))
        , inputFields(std::move(inputFields))
    {
    }

    const std::string& getModelName() const { return modelName; }

    std::shared_ptr<Operator> copy() override;
    bool equal(const std::shared_ptr<Node>& rhs) const override;
    bool isIdentical(const std::shared_ptr<Node>& rhs) const override;
    void updateToFullyQualifiedFieldName(const std::shared_ptr<NodeFunctionFieldAccess>& field) const;
    bool inferSchema() override;
    void inferStringSignature() override;
    const std::vector<std::shared_ptr<NodeFunction>>& getInputFields() const;

protected:
    std::string toString() const override;
};
}