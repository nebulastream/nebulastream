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

#include <optional>
#include <string>
#include <vector>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class UnionRenamePhysicalOperator final : public PhysicalOperatorConcept
{
public:
    UnionRenamePhysicalOperator(std::vector<std::string> inputFields, std::vector<std::string> outputFields);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    std::vector<std::string> inputFields;
    std::vector<std::string> outputFields;
    std::optional<PhysicalOperator> child;
};
}
