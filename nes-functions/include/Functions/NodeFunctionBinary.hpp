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
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionBinary.hpp
#include <Functions/NodeFunction.hpp>
========
#include <Functions/FunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/BinaryFunctionNode.hpp
namespace NES
{
/**
 * @brief A binary function is represents functions with two children.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionBinary.hpp
class NodeFunctionBinary : public NodeFunction
{
public:
    ~NodeFunctionBinary() noexcept override = default;
========
class BinaryFunctionNode : public FunctionNode
{
public:
    ~BinaryFunctionNode() noexcept override = default;
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/BinaryFunctionNode.hpp

    /**
     * @brief set the children node of this function.
     */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionBinary.hpp
    void setChildren(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
========
    void setChildren(FunctionNodePtr const& left, FunctionNodePtr const& right);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/BinaryFunctionNode.hpp

    /**
     * @brief gets the left children.
     */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionBinary.hpp
    NodeFunctionPtr getLeft() const;
========
    FunctionNodePtr getLeft() const;
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/BinaryFunctionNode.hpp

    /**
     * @brief gets the right children.
     */
<<<<<<<< HEAD:nes-functions/include/Functions/NodeFunctionBinary.hpp
    NodeFunctionPtr getRight() const;

protected:
    explicit NodeFunctionBinary(DataTypePtr stamp, std::string name);
    explicit NodeFunctionBinary(NodeFunctionBinary* other);
========
    FunctionNodePtr getRight() const;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override = 0;

protected:
    explicit BinaryFunctionNode(DataTypePtr stamp);
    explicit BinaryFunctionNode(BinaryFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/BinaryFunctionNode.hpp
};

} /// namespace NES
