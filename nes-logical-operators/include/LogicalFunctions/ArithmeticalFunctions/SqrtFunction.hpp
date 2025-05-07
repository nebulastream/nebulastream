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
#include <LogicalFunctions/Function.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES::Logical
{
class SqrtFunction final : public FunctionConcept
{
public:
    static constexpr std::string_view NAME = "Sqrt";

    SqrtFunction(Function child);
    SqrtFunction(const SqrtFunction& other);
    ~SqrtFunction() noexcept override = default;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const FunctionConcept& rhs) const override;

    [[nodiscard]] std::shared_ptr<DataType> getStamp() const override;
    [[nodiscard]] Function withStamp(std::shared_ptr<DataType> stamp) const override;
    [[nodiscard]] Function withInferredStamp(const Schema& schema) const override;

    [[nodiscard]] std::vector<Function> getChildren() const override;
    [[nodiscard]] Function withChildren(const std::vector<Function>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

private:
    std::shared_ptr<DataType> stamp;
    Function child;
};
}
FMT_OSTREAM(NES::Logical::SqrtFunction);
