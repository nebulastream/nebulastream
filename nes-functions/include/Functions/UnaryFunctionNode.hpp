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
#include <Functions/FunctionNode.hpp>
namespace NES
{
/**
 * @brief A unary function is used to represent functions with one child.
 */
class UnaryFunctionNode : public FunctionNode
{
public:
    explicit UnaryFunctionNode(DataTypePtr stamp, std::string name);

    /**
     * @brief set the child node of this function.
     * @param child FunctionNodePtr
     */
    void setChild(const FunctionNodePtr& child);

    /**
     * @brief returns the child of this function
     * @return
     */
    FunctionNodePtr child() const;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr deepCopy() override = 0;

protected:
    explicit UnaryFunctionNode(UnaryFunctionNode* other);
};
} /// namespace NES
