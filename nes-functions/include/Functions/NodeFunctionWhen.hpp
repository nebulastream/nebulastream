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
#include <API/Schema.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include "Functions/NodeFunction.hpp"
#include "Nodes/Node.hpp"
namespace NES
{

/// This node represents an When function.
/// It is used as part of a case function, if it evaluates the left child to true, the right child will be returned.
class NodeFunctionWhen final : public NodeFunctionBinary
{
public:
    explicit NodeFunctionWhen(DataTypePtr stamp);
    ~NodeFunctionWhen() noexcept override = default;

    static NodeFunctionPtr create(const NodeFunctionPtr& left, const NodeFunctionPtr& right);

    void inferStamp(const Schema& schema) override;

    [[nodiscard]] bool equal(const NodePtr& rhs) const override;

    bool validateBeforeLowering() const override;
    NodeFunctionPtr deepCopy() override;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    explicit NodeFunctionWhen(NodeFunctionWhen* other);
};

}
