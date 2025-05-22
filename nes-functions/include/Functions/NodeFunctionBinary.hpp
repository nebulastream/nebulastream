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
#include <memory>
#include <string>
#include <Functions/NodeFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
/**
 * @brief A binary function is represents functions with two children.
 */
class NodeFunctionBinary : public NodeFunction
{
public:
    ~NodeFunctionBinary() noexcept override = default;

    /**
     * @brief set the children node of this function.
     */
    void setChildren(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right);

    /**
     * @brief gets the left children.
     */
    std::shared_ptr<NodeFunction> getLeft() const;

    /**
     * @brief gets the right children.
     */
    std::shared_ptr<NodeFunction> getRight() const;

protected:
    explicit NodeFunctionBinary(std::shared_ptr<DataType> stamp, std::string name);
    explicit NodeFunctionBinary(NodeFunctionBinary* other);
};

}
