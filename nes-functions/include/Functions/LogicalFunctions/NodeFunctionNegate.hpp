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
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionNegate.hpp
#include <Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp>
========
#include <Functions/LogicalFunctions/LogicalUnaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/NegateFunctionNode.hpp
namespace NES
{

/**
 * @brief This node negates its child function.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionNegate.hpp
class NodeFunctionNegate : public NodeFunctionLogicalUnary
{
public:
    NodeFunctionNegate();
    ~NodeFunctionNegate() override = default;
========
class NegateFunctionNode : public LogicalUnaryFunctionNode
{
public:
    NegateFunctionNode();
    ~NegateFunctionNode() override = default;
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/NegateFunctionNode.hpp

    /**
     * @brief Create a new negate function
     */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionNegate.hpp
    static NodeFunctionPtr create(NodeFunctionPtr const& child);
========
    static FunctionNodePtr create(FunctionNodePtr const& child);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/NegateFunctionNode.hpp

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    bool validateBeforeLowering() const override;

    /**
     * @brief Infers the stamp of this logical negate function node.
     * We assume that the children of this function is a predicate.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this function node.
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionNegate.hpp
    * @return NodeFunctionPtr
    */
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionNegate(NodeFunctionNegate* other);
========
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit NegateFunctionNode(NegateFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/NegateFunctionNode.hpp
};
} /// namespace NES
