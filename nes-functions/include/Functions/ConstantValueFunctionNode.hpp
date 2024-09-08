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

#include <Functions/FunctionNode.hpp>
namespace NES
{

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

/**
 * @brief This function node represents a constant value and a fixed data type.
 * Thus the samp of this function is always fixed.
 */
class ConstantValueFunctionNode : public FunctionNode
{
public:
    /**
     * @brief Factory method to create a ConstantValueFunctionNode.
     */
    static FunctionNodePtr create(ValueTypePtr const& constantValue);
    ~ConstantValueFunctionNode() noexcept override = default;

    /**
     * @brief Returns the constant value.
     */
    ValueTypePtr getConstantValue() const;

    /**
     * @brief On a constant value function infer stamp has not to perform any action as its result type is always constant.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
     * @brief Creates a string of the value and the type.
     * @return
     */
    std::string toString() const override;

    /**
     * @brief Compares if another node is equal to this constant value function.
     * @param otherNode
     * @return true if they are equal
     */
    bool equal(NodePtr const& rhs) const override;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit ConstantValueFunctionNode(const ConstantValueFunctionNode* other);

private:
    explicit ConstantValueFunctionNode(ValueTypePtr const& constantValue);
    /// Value of this function
    ValueTypePtr constantValue;
};
using ConstantValueExpressionNodePtr = std::shared_ptr<ConstantValueExpressionNode>;
} /// namespace NES
