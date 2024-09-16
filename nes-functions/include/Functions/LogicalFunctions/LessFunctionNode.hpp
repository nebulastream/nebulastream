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
 * @brief This node represents a less comparision between the two children.
 */
class LessFunctionNode : public LogicalBinaryFunctionNode
{
public:
    LessFunctionNode();
    ~LessFunctionNode() override = default;
    /**
    * @brief Create a new less function
    */
    static FunctionNodePtr create(FunctionNodePtr const& left, FunctionNodePtr const& right);

    bool equal(NodePtr const& rhs) const override;

    std::string toString() const override;
bool validate() const override;
    FunctionNodePtr deepCopy() override;

protected:
    explicit LessFunctionNode(LessFunctionNode* other);
};
} /// namespace NES
