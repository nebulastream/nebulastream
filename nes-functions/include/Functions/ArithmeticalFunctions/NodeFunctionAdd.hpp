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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
========
#include <Functions/ArithmeticalFunctions/ArithmeticalBinaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/AddFunctionNode.hpp
namespace NES
{
/**
 * @brief This node represents an ADD function.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp
class NodeFunctionAdd final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionAdd(DataTypePtr stamp);
    ~NodeFunctionAdd() noexcept override = default;
    /**
     * @brief Create a new ADD function
     */
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    NodeFunctionPtr deepCopy() override;

private:
    explicit NodeFunctionAdd(NodeFunctionAdd* other);
========
class AddFunctionNode final : public ArithmeticalBinaryFunctionNode
{
public:
    explicit AddFunctionNode(DataTypePtr stamp);
    ~AddFunctionNode() noexcept override = default;
    /**
     * @brief Create a new ADD function
     */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    bool validate() const override;
    FunctionNodePtr deepCopy() override;

private:
    explicit AddFunctionNode(AddFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/ArithmeticalFunctions/AddFunctionNode.hpp
};

}
