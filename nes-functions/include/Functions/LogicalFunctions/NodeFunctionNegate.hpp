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
#include <Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp>
namespace NES
{

/**
 * @brief This node negates its child function.
 */
class NodeFunctionNegate : public NodeFunctionLogicalUnary
{
public:
    NodeFunctionNegate();
    ~NodeFunctionNegate() override = default;

    /**
     * @brief Create a new negate function
     */
    static NodeFunctionPtr create(NodeFunctionPtr const& child);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    bool validateBeforeLowering() const override;

    /// We assume that the children of this function is a predicate.
    void inferStamp(const Schema& schema) override;

    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionNegate(NodeFunctionNegate* other);

    [[nodiscard]] std::string toString() const override;
};
}
