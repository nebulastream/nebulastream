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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionRound.hpp
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionRound.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
namespace NES
{
class NodeFunctionRound final : public NodeFunctionArithmeticalUnary
{
public:
    explicit NodeFunctionRound(DataTypePtr stamp);
    ~NodeFunctionRound() noexcept override = default;
    [[nodiscard]] static NodeFunctionPtr create(NodeFunctionPtr const& child);
========
#include <Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp>
========
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp
namespace NES
{

/**
 * @brief This node represents a division function.
 */
class NodeFunctionDiv final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionDiv(DataTypePtr stamp);
    ~NodeFunctionDiv() noexcept override = default;
    /**
     * @brief Create a new DIV function
     */
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionRound.hpp
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/DivFunctionNode.hpp
========
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    bool validate() const override;

    /**
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionRound.hpp
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;
    NodeFunctionPtr deepCopy() override;

private:
    explicit NodeFunctionRound(NodeFunctionRound* other);
========
    * @brief Create a deep copy of this function node.
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

private:
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionRound.hpp
    explicit DivFunctionNode(DivFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/DivFunctionNode.hpp
========
    explicit NodeFunctionDiv(NodeFunctionDiv* other);
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp
};

}
