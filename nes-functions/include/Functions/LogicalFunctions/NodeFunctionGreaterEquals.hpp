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
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp
#include <Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp>
========
#include <Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/GreaterEqualsFunctionNode.hpp
namespace NES
{

/**
 * @brief This node represents a greater comparison between the two children.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionGreaterEquals.hpp
class NodeFunctionGreaterEquals : public NodeFunctionLogicalBinary
{
public:
    NodeFunctionGreaterEquals() noexcept;
    ~NodeFunctionGreaterEquals() override = default;
    /**
    * @brief Create a new greater then function
    */
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionGreaterEquals(NodeFunctionGreaterEquals* other);
========
class GreaterEqualsFunctionNode : public LogicalBinaryFunctionNode
{
public:
    GreaterEqualsFunctionNode() noexcept = default;
    ~GreaterEqualsFunctionNode() override = default;
    /**
    * @brief Create a new greater then function
    */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit GreaterEqualsFunctionNode(GreaterEqualsFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/GreaterEqualsFunctionNode.hpp
};
} /// namespace NES
