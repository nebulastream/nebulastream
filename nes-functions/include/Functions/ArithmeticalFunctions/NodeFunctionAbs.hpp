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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
namespace NES
{
/**
 * @brief This node represents an ABS (absolut value) function.
 */
class NodeFunctionAbs final : public NodeFunctionArithmeticalUnary
{
public:
    explicit NodeFunctionAbs(DataTypePtr stamp);
    ~NodeFunctionAbs() noexcept override = default;
    [[nodiscard]] static NodeFunctionPtr create(NodeFunctionPtr const& child);
========
#include <Functions/ArithmeticalFunctions/ArithmeticalUnaryFunctionNode.hpp>
namespace NES
{
/**
 * @brief This node represents an ROUND (absolut value) function.
 */
class RoundFunctionNode final : public ArithmeticalUnaryFunctionNode
{
public:
    explicit RoundFunctionNode(DataTypePtr stamp);
    ~RoundFunctionNode() noexcept override = default;
    /**
     * @brief Create a new ROUND function
     */
    [[nodiscard]] static FunctionNodePtr create(FunctionNodePtr const& child);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/RoundFunctionNode.hpp
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp
    NodeFunctionPtr deepCopy() override;

private:
    explicit NodeFunctionAbs(NodeFunctionAbs* other);
========

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

private:
    explicit RoundFunctionNode(RoundFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/RoundFunctionNode.hpp
};

}
