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
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{

/// This node represents an When function.
/// It is used as part of a case function, if it evaluates the left child to true, the right child will be returned.
class NodeFunctionWhen final : public NodeFunctionBinary
{
public:
    explicit NodeFunctionWhen(std::shared_ptr<DataType> stamp);
    ~NodeFunctionWhen() noexcept override = default;

    static std::shared_ptr<NodeFunction> create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right);

    void inferStamp(const Schema& schema) override;

    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;

    bool validateBeforeLowering() const override;
    std::shared_ptr<NodeFunction> deepCopy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;

private:
    explicit NodeFunctionWhen(NodeFunctionWhen* other);
};

}
