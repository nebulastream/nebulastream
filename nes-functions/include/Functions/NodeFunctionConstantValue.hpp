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
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus the samp of this function is always fixed.
class NodeFunctionConstantValue : public NodeFunction
{
public:
    ///Factory method to create a NodeFunctionConstantValue.
    static std::shared_ptr<NodeFunction> create(const std::shared_ptr<DataType>& type, std::string value);
    ~NodeFunctionConstantValue() noexcept override = default;

    std::string getConstantValue() const;

    /// On a constant value function infer stamp has not to perform any action as its result type is always constant.
    /// @param schema
    void inferStamp(const Schema& schema) override;

    bool equal(const std::shared_ptr<Node>& rhs) const override;
    bool validateBeforeLowering() const override;
    std::shared_ptr<NodeFunction> deepCopy() override;

protected:
    explicit NodeFunctionConstantValue(const NodeFunctionConstantValue* other);

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    explicit NodeFunctionConstantValue(const std::shared_ptr<DataType>& type, std::string&& value);
    /// Value of this function
    std::string constantValue;
};
}
