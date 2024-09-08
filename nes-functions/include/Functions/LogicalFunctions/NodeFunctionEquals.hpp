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
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionEquals.hpp
#include <Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp>
========
#include <Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/EqualsFunctionNode.hpp
namespace NES
{

/**
 * @brief This node represents an equals comparision between the two children.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionEquals.hpp
class NodeFunctionEquals : public NodeFunctionLogicalBinary
{
public:
    NodeFunctionEquals() noexcept;
    ~NodeFunctionEquals() override = default;
    /**
    * @brief Create a new equals function
    */
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
========
class EqualsFunctionNode : public LogicalBinaryFunctionNode
{
public:
    EqualsFunctionNode() noexcept = default;
    ~EqualsFunctionNode() override = default;
    /**
    * @brief Create a new equals function
    */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/EqualsFunctionNode.hpp
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    bool validateBeforeLowering() const override;

    /**
    * @brief Create a deep copy of this function node.
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionEquals.hpp
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionEquals(NodeFunctionEquals* other);
========
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit EqualsFunctionNode(EqualsFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/EqualsFunctionNode.hpp
};
} /// namespace NES
