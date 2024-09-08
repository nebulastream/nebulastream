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
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionOr.hpp
#include <Functions/LogicalFunctions/NodeFunctionLogicalBinary.hpp>
========
#include <Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/OrFunctionNode.hpp
namespace NES
{

/**
 * @brief This node represents an OR combination between the two children.
 */
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionOr.hpp
class NodeFunctionOr : public NodeFunctionLogicalBinary
{
public:
    NodeFunctionOr();
    ~NodeFunctionOr() override = default;
    /**
    * @brief Create a new OR function
    */
    static NodeFunctionPtr create(NodeFunctionPtr const& left, NodeFunctionPtr const& right);
========
class OrFunctionNode : public LogicalBinaryFunctionNode
{
public:
    OrFunctionNode();
    ~OrFunctionNode() override = default;
    /**
    * @brief Create a new OR function
    */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/OrFunctionNode.hpp
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    /**
     * @brief Infers the stamp of this logical OR function node.
     * We assume that both children of an OR function are predicates.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;
<<<<<<<< HEAD:nes-functions/include/Functions/LogicalFunctions/NodeFunctionOr.hpp
    bool validateBeforeLowering() const override;
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionOr(NodeFunctionOr* other);
========
    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit OrFunctionNode(OrFunctionNode* other);
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/include/Functions/LogicalFunctions/OrFunctionNode.hpp
};
} /// namespace NES
