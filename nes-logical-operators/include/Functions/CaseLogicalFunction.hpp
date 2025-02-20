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
#include <Functions/LogicalFunction.hpp>
namespace NES
{

/// @brief A case function has at least one when function and one default function.
/// All when functions are evaluated and the first one with a true condition is returned.
class CaseLogicalFunction final : public LogicalFunction
{
public:
    static constexpr std::string_view NAME = "Case";

    explicit CaseLogicalFunction(std::vector<std::shared_ptr<LogicalFunction>> const& whenExps, std::shared_ptr<LogicalFunction> const& defaultExp);
    ~CaseLogicalFunction() noexcept override;

    bool validateBeforeLowering() const;

    [[nodiscard]] SerializableFunction serialize() const override;

    void setChildren(std::vector<std::shared_ptr<LogicalFunction>> const& whenExps, std::shared_ptr<LogicalFunction> const& defaultExp);
    std::vector<std::shared_ptr<LogicalFunction>> getWhenChildren() const;
    std::shared_ptr<LogicalFunction> getDefaultExp() const;
    [[nodiscard]] std::span<const std::shared_ptr<LogicalFunction>> getChildren() const override;

    void inferStamp(const Schema& schema) override;
    [[nodiscard]] bool operator==(std::shared_ptr<LogicalFunction> const& rhs) const override;
    std::shared_ptr<LogicalFunction> clone() const override;

private:
    explicit CaseLogicalFunction(const CaseLogicalFunction& other);
    [[nodiscard]] std::string toString() const override;

    std::vector<std::shared_ptr<LogicalFunction>> whenChildren;
    std::shared_ptr<LogicalFunction> defaultChild;

    std::vector<std::shared_ptr<LogicalFunction>> allChildren;
};
}
FMT_OSTREAM(NES::CaseLogicalFunction);

