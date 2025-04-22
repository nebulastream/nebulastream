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

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>
#include <SerializableOperator.pb.h>

namespace NES::InferModel
{

class InferModelLogicalOperator : public UnaryLogicalOperator
{
public:
    InferModelLogicalOperator(
        std::string model,
        std::vector<std::shared_ptr<LogicalFunction>> inputFields,
        std::vector<std::shared_ptr<LogicalFunction>> outputFields,
        OperatorId id);

    std::shared_ptr<Operator> clone() const override;

    /// @brief compares this operator node with another
    /// @param rhs the other operator node
    /// @return true if both are equal or false if both are not equal
    [[nodiscard]] bool operator==(const Operator& rhs) const override;

    /// @brief checks if the operator node is equal and also has the same id, so it is the identical node
    /// @param rhs the other operator node
    /// @return true if identical, false otherwise
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;

    bool inferSchema() override;

    const std::string& getModel() const;
    const std::string getDeployedModelPath() const;
    const std::vector<std::shared_ptr<LogicalFunction>>& getInputFields() const;
    const std::vector<std::shared_ptr<LogicalFunction>>& getOutputFields() const;

    [[maybe_unused]] [[nodiscard]] SerializableOperator serialize() const override
    {
        /// TODO: noop for now. Will change after we added the new inference operator
        return SerializableOperator{};
    }

protected:
    std::string toString() const override;

private:
    /// @brief updates the field to a fully qualified one.
    void updateToFullyQualifiedFieldName(std::shared_ptr<FieldAccessLogicalFunction> field) const;

    std::string model;
    std::vector<std::shared_ptr<LogicalFunction>> inputFields;
    std::vector<std::shared_ptr<LogicalFunction>> outputFields;
};

}
