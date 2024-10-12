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
#include <Functions/LogicalFunctions/LogicalBinaryFunctionNode.hpp>
namespace NES
{

/**
 * @brief This node represents a less then comparision between the two children.
 */
class LessEqualsFunctionNode : public LogicalBinaryFunctionNode
{
public:
    LessEqualsFunctionNode() = default;
    ~LessEqualsFunctionNode() override = default;
    /**
    * @brief Create a new less then function
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
    explicit LessEqualsFunctionNode(LessEqualsFunctionNode* other);
};
} /// namespace NES
