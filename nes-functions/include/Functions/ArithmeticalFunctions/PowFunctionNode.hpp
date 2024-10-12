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
 * @brief This node represents an POWER function.
 */
class PowFunctionNode final : public ArithmeticalBinaryFunctionNode
{
public:
    explicit PowFunctionNode(DataTypePtr stamp);
    ~PowFunctionNode() noexcept override = default;
    /**
     * @brief Create a new POWER function
     */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Determine returned datatype (-> UInt64/Double/ Throw exception for invalid inputs). Override ArithmeticalBinaryFunctionNode::inferStamp to increase bounds.
     * @comment E.g. SQL Server has a very unintuitive behaviour of always returning the datatype of the base (Int/Float). C++ always returns a float. We decide to return a float, except when both base and exponent are an Integer; and we set high bounds as POWER is an exponential function.
     * @param typeInferencePhaseContext
     * @param schema: the current schema.
     */
    void inferStamp(SchemaPtr schema) override;
    bool validateBeforeLowering() const override;
    FunctionNodePtr deepCopy() override;

private:
    explicit PowFunctionNode(PowFunctionNode* other);
};

} /// namespace NES
