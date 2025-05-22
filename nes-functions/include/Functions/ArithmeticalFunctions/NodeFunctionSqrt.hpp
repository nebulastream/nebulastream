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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalUnary.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class NodeFunctionSqrt final : public NodeFunctionArithmeticalUnary
{
public:
    explicit NodeFunctionSqrt(std::shared_ptr<DataType> stamp);
    ~NodeFunctionSqrt() noexcept override = default;
    [[nodiscard]] static std::shared_ptr<NodeFunction> create(const std::shared_ptr<NodeFunction>& child);
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;

    std::shared_ptr<NodeFunction> deepCopy() override;

protected:
    explicit NodeFunctionSqrt(NodeFunctionSqrt* other);

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
};

}
