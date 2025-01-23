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
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
namespace NES
{

class NodeFunctionFieldAssignment;
using NodeFunctionFieldAssignmentPtr = std::shared_ptr<NodeFunctionFieldAssignment>;
/**
 * @brief A FieldAssignmentFunction represents the assignment of an function result to a specific field.
 */
class NodeFunctionFieldAssignment : public NodeFunctionBinary
{
public:
    explicit NodeFunctionFieldAssignment(DataTypePtr stamp);

    static NodeFunctionFieldAssignmentPtr create(const NodeFunctionFieldAccessPtr& fieldAccess, const NodeFunctionPtr& NodeFunctionPtr);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    bool validateBeforeLowering() const override;

    NodeFunctionFieldAccessPtr getField() const;

    /// returns the functions, which calculates the new value.
    NodeFunctionPtr getAssignment() const;

    void inferStamp(const Schema& schema) override;

    NodeFunctionPtr deepCopy() override;

protected:
    explicit NodeFunctionFieldAssignment(NodeFunctionFieldAssignment* other);

    [[nodiscard]] std::string toString() const override;
};
}
