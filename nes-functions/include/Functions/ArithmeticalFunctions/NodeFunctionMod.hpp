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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
namespace NES
{

class NodeFunctionMod final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionMod(DataType stamp);
    ~NodeFunctionMod() noexcept override = default;
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Determine returned datatype (-> UInt64/Double/NodeFunction Throw exception for invalid inputs). Override ArithmeticalBinary::inferStamp to tighten bounds.
     * @comment E.g. if the divisor in the modulo operation is a Int8, we can set the results to be Int8.
     * @comment More general: We set upperbound = max(abs(lowerbound_of_divisor), abs(upperbound_of_divisor)) and the lowerbound to the negation of the same maxiumum. This follows the mathematical definition and implementation in C.
     * @param typeInferencePhaseContext
     * @param schema: the current schema.
     */
    void inferStamp(Schema& schema) override;
    NodeFunctionPtr deepCopy() override;

private:
    explicit NodeFunctionMod(NodeFunctionMod* other);
};

}
