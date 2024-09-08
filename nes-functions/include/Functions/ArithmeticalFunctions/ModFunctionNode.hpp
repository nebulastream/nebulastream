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
#include <Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp>
namespace NES
{
/**
 * @brief This node represents an MODER function.
 */
class ModFunctionNode final : public ArithmeticalBinaryFunctionNode
{
public:
    explicit ModFunctionNode(DataTypePtr stamp);
    ~ModFunctionNode() noexcept override = default;
    /**
         * @brief Create a new MODULO function
         */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
         * @brief Determine returned datatype (-> UInt64/Double/ Throw exception for invalid inputs). Override ArithmeticalBinaryFunctionNode::inferStamp to tighten bounds.
         * @comment E.g. if the divisor in the modulo operation is a Int8, we can set the results to be Int8.
         * @comment More general: We set upperbound = max(abs(lowerbound_of_divisor), abs(upperbound_of_divisor)) and the lowerbound to the negation of the same maxiumum. This follows the mathematical definition and implementation in C.
         * @param typeInferencePhaseContext
         * @param schema: the current schema.
         */
    void inferStamp(SchemaPtr schema) override;

    /**
        * @brief Create a deep copy of this function node.
        * @return FunctionNodePtr
        */
    FunctionNodePtr copy() override;

private:
    explicit ModFunctionNode(ModFunctionNode* other);
};

} /// namespace NES
