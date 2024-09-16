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
#include <Functions/LogicalFunctions/LogicalUnaryFunctionNode.hpp>
namespace NES
{

/**
 * @brief This node negates its child function.
 */
class NegateFunctionNode : public LogicalUnaryFunctionNode
{
public:
    NegateFunctionNode();
    ~NegateFunctionNode() override = default;

    /**
     * @brief Create a new negate function
     */
    static FunctionNodePtr create(FunctionNodePtr const& child);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    /**
     * @brief Infers the stamp of this logical negate function node.
     * We assume that the children of this function is a predicate.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override;

protected:
    explicit NegateFunctionNode(NegateFunctionNode* other);
};
} /// namespace NES
