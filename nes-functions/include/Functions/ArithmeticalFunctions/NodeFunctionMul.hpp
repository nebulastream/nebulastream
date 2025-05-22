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
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class NodeFunctionMul final : public NodeFunctionArithmeticalBinary
{
public:
    explicit NodeFunctionMul(std::shared_ptr<DataType> stamp);
    ~NodeFunctionMul() noexcept override = default;
    static std::shared_ptr<NodeFunction> create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right);
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    std::shared_ptr<NodeFunction> deepCopy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;

private:
    explicit NodeFunctionMul(NodeFunctionMul* other);
};

}
