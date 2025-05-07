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
#include <Functions/NodeFunctionUnary.hpp>
namespace NES
{
/**
 * @brief This node represents an ABS (absolut value) function.
 */
class NodeFunctionImageManip final : public NodeFunction
{
public:
    NodeFunctionImageManip(std::shared_ptr<DataType> stamp, std::string type);
    ~NodeFunctionImageManip() noexcept override = default;

    const std::string& getFunctionName() const;
    [[nodiscard]] static std::shared_ptr<NodeFunction> create(std::vector<std::shared_ptr<NodeFunction>> children);
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    void inferStamp(const Schema& schema) override;

    std::shared_ptr<NodeFunction> deepCopy() override;

    bool validateBeforeLowering() const override;

protected:
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;

private:
    std::string functionName;
    explicit NodeFunctionImageManip(NodeFunctionImageManip* other);
};

}
