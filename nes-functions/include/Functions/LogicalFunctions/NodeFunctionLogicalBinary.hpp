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
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp
#include <Functions/LogicalFunctions/NodeFunctionLogical.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Util/Common.hpp>
========
#include <Functions/BinaryFunctionNode.hpp>
#include <Functions/LogicalFunctions/LogicalFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp
namespace NES
{
/**
 * @brief This node represents a logical binary function.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp
class NodeFunctionLogicalBinary : public NodeFunctionBinary, public LogicalNodeFunction
{
public:
========
class LogicalBinaryFunctionNode : public BinaryFunctionNode, public LogicalFunctionNode
{
public:
    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override = 0;

>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    bool validateBeforeLowering() const override;

protected:
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp
    NodeFunctionLogicalBinary(std::string name);
    ~NodeFunctionLogicalBinary() override = default;
    explicit NodeFunctionLogicalBinary(NodeFunctionLogicalBinary* other);
========
    LogicalBinaryFunctionNode();
    ~LogicalBinaryFunctionNode() override = default;
    explicit LogicalBinaryFunctionNode(LogicalBinaryFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp
};
} /// namespace NES
