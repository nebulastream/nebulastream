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
 * @brief A binary function is represents functions with two children.
 */
class BinaryFunctionNode : public FunctionNode
{
public:
    ~BinaryFunctionNode() noexcept override = default;

    /**
     * @brief set the children node of this function.
     */
    void setChildren(FunctionNodePtr const& left, FunctionNodePtr const& right);

    /**
     * @brief gets the left children.
     */
    FunctionNodePtr getLeft() const;

    /**
     * @brief gets the right children.
     */
    FunctionNodePtr getRight() const;

    /**
    * @brief Create a deep copy of this function node.
    * @return FunctionNodePtr
    */
    FunctionNodePtr copy() override = 0;

protected:
    explicit BinaryFunctionNode(DataTypePtr stamp);
    explicit BinaryFunctionNode(BinaryFunctionNode* other);
};

} /// namespace NES
