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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include "Functions/NodeFunction.hpp"
#include "Nodes/Node.hpp"
namespace NES
{

class NodeFunctionDiv final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionDiv(DataTypePtr stamp);
    ~NodeFunctionDiv() noexcept override = default;
    static NodeFunctionPtr create(const NodeFunctionPtr& left, const NodeFunctionPtr& right);
    [[nodiscard]] bool equal(const NodePtr& rhs) const override;
    NodeFunctionPtr deepCopy() override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    explicit NodeFunctionDiv(NodeFunctionDiv* other);
};

}
