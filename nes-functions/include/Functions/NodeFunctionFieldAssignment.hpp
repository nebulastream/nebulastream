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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

/// A FieldAssignmentFunction represents the assignment of an function result to a specific field.
class NodeFunctionFieldAssignment : public NodeFunctionBinary
{
public:
    explicit NodeFunctionFieldAssignment(std::shared_ptr<DataType> stamp);
    static std::shared_ptr<NodeFunctionFieldAssignment>
    create(const std::shared_ptr<NodeFunctionFieldAccess>& fieldAccess, const std::shared_ptr<NodeFunction>& nodeFunction);

    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    bool validateBeforeLowering() const override;

    std::shared_ptr<NodeFunctionFieldAccess> getField() const;

    std::shared_ptr<NodeFunction> getAssignment() const;

    void inferStamp(const Schema& schema) override;

    std::shared_ptr<NodeFunction> deepCopy() override;

protected:
    explicit NodeFunctionFieldAssignment(NodeFunctionFieldAssignment* other);

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
};
}
