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
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionSub.hpp
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
namespace NES
{
========
#include <Functions/LogicalFunctions/NodeFunctionLogical.hpp>
#include <Functions/NodeFunctionUnary.hpp>
namespace NES
{
/**
 * @brief This node represents a logical unary function.
 */
class NodeFunctionLogicalUnary : public NodeFunctionUnary, public LogicalFunctionNode
{
protected:
    NodeFunctionLogicalUnary(std::string name);
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp

class NodeFunctionSub final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionSub(DataTypePtr stamp);
    ~NodeFunctionSub() noexcept override = default;
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
    * @brief Create a deep copy of this function node.
    * @return NodeFunctionPtr
    */
<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionSub.hpp
    NodeFunctionPtr deepCopy() override;

private:
    explicit NodeFunctionSub(NodeFunctionSub* other);
========
    NodeFunctionPtr deepCopy() override = 0;

protected:
    explicit NodeFunctionLogicalUnary(NodeFunctionLogicalUnary* other);
>>>>>>>> a212ae69ac (chore(Functions) Renamed FunctionNode --> NodeFunction):nes-functions/include/Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp
};

}
