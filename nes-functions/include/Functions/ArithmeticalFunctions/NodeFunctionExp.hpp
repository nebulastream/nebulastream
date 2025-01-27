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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include "Functions/NodeFunction.hpp"
#include "Nodes/Node.hpp"
namespace NES
{
class NodeFunctionExp final : public NodeFunctionArithmeticalUnary
{
public:
    explicit NodeFunctionExp(DataTypePtr stamp);
    ~NodeFunctionExp() noexcept override = default;
    [[nodiscard]] static NodeFunctionPtr create(const NodeFunctionPtr& child);
    [[nodiscard]] bool equal(const NodePtr& rhs) const override;
    NodeFunctionPtr deepCopy() override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    explicit NodeFunctionExp(NodeFunctionExp* other);
};

}
