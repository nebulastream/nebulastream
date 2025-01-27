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
#include <vector>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include "Nodes/Node.hpp"
namespace NES
{
/**
 * @brief A case function has at least one when function and one default function.
 * All when functions are evaluated and the first one with a true condition is returned.
 */
class NodeFunctionCase : public NodeFunction
{
public:
    explicit NodeFunctionCase(DataTypePtr stamp);
    ~NodeFunctionCase() noexcept override = default;

    /**
     * @brief Create a new Case function node.
     * @param whenExps : a vector of when function nodes.
     * @param defaultExp : the function to select, if no when function evaluates to a value
     */
    static NodeFunctionPtr create(const std::vector<NodeFunctionPtr>& whenExps, const NodeFunctionPtr& defaultExp);

    /**
     * @brief set the children nodes of this function.
     * @param whenExps : a vector of when function nodes.
     * @param defaultExp : the function to select, if no when function evaluates to a value
     */
    void setChildren(const std::vector<NodeFunctionPtr>& whenExps, const NodeFunctionPtr& defaultExp);

    /**
     * @brief gets the vector of when children.
     */
    std::vector<NodeFunctionPtr> getWhenChildren() const;

    /**
     * @brief gets the node representing the default child.
     */
    NodeFunctionPtr getDefaultExp() const;

    void inferStamp(const Schema& schema) override;

    [[nodiscard]] bool equal(const NodePtr& rhs) const final;

    bool validateBeforeLowering() const override;
    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionCase(NodeFunctionCase* other);

    [[nodiscard]] std::string toString() const override;
};

}
