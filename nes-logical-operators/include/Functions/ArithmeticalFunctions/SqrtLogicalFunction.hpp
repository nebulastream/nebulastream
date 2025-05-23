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
#include <Functions/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
class SqrtLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "Sqrt";

    SqrtLogicalFunction(LogicalFunction child);
    SqrtLogicalFunction(const SqrtLogicalFunction& other);
    ~SqrtLogicalFunction() noexcept override = default;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] std::shared_ptr<DataType> getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(std::shared_ptr<DataType> dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

private:
    std::shared_ptr<DataType> dataType;
    LogicalFunction child;
};
}
FMT_OSTREAM(NES::SqrtLogicalFunction);
