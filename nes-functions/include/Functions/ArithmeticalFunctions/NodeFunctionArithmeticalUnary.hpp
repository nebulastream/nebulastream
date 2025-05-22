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
#include <API/Schema.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmetical.hpp>
#include <Functions/NodeFunctionUnary.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class NodeFunctionArithmeticalUnary : public NodeFunctionUnary, public NodeFunctionArithmetical
{
public:
    /// Infers the stamp of this arithmetical function node.
    /// Currently the type inference is equal for all arithmetical function and expects numerical data types as operands.
    void inferStamp(const Schema& schema) override;
    bool validateBeforeLowering() const override;
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;

protected:
    explicit NodeFunctionArithmeticalUnary(std::shared_ptr<DataType> stamp, std::string name);
    explicit NodeFunctionArithmeticalUnary(NodeFunctionArithmeticalUnary* other);
    ~NodeFunctionArithmeticalUnary() noexcept override = default;
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
};

}
