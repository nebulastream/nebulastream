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
namespace NES
{

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

/**
 * @brief This function node represents a constant value and a fixed data type.
 * Thus the samp of this function is always fixed.
 */
class NodeFunctionConstantValue : public NodeFunction
{
public:
    /**
     * @brief Factory method to create a NodeFunctionConstantValue.
     */
    static NodeFunctionPtr create(ValueTypePtr const& constantValue);
    ~NodeFunctionConstantValue() noexcept override = default;

    ValueTypePtr getConstantValue() const;

    /**
     * @brief On a constant value function infer stamp has not to perform any action as its result type is always constant.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    std::string toString() const override;
    bool equal(NodePtr const& rhs) const override;
    bool validateBeforeLowering() const override;

    /**
    * @brief Create a deep copy of this function node.
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionConstantValue(const NodeFunctionConstantValue* other);

private:
    explicit NodeFunctionConstantValue(ValueTypePtr const& constantValue);
    /// Value of this function
    ValueTypePtr constantValue;
};
using NodeFunctionConstantValuePtr = std::shared_ptr<NodeFunctionConstantValue>;
}
