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
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionGreater.hpp
#include <Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp>
========
#include <Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/GreaterFunctionNode.hpp
namespace NES
{

/**
 * @brief This node represents a greater comparision between the two children.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionGreater.hpp
class NodeFunctionGreater : public NodeFunctionLogicalBinary
{
public:
    NodeFunctionGreater() noexcept;
    ~NodeFunctionGreater() override = default;
    /**
    * @brief Create a new greater function
    */
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionGreater(NodeFunctionGreater* other);
========
class GreaterFunctionNode : public LogicalBinaryFunctionNode
{
public:
    GreaterFunctionNode() noexcept;
    ~GreaterFunctionNode() override = default;
    /**
    * @brief Create a new greater function
    */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
bool validate() const override;
    FunctionNodePtr deepCopy() override;

protected:
    explicit GreaterFunctionNode(GreaterFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/GreaterFunctionNode.hpp
};
} /// namespace NES
