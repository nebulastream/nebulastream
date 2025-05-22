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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nodes/Node.hpp>

namespace NES
{


class NodeFunctionFieldRename : public NodeFunction
{
public:
    static std::shared_ptr<NodeFunction> create(const std::shared_ptr<NodeFunctionFieldAccess>& originalField, std::string newFieldName);

    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    bool validateBeforeLowering() const override;
    std::string getNewFieldName() const;

    void inferStamp(const Schema& schema) override;

    std::shared_ptr<NodeFunction> deepCopy() override;

    std::shared_ptr<NodeFunctionFieldAccess> getOriginalField() const;

protected:
    explicit NodeFunctionFieldRename(const std::shared_ptr<NodeFunctionFieldRename>& other);

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    NodeFunctionFieldRename(const std::shared_ptr<NodeFunctionFieldAccess>& originalField, std::string newFieldName);

    std::shared_ptr<NodeFunctionFieldAccess> originalField;
    std::string newFieldName;
};

}
