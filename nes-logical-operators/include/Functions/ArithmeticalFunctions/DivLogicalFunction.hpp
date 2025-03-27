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

#include <Abstract/LogicalFunction.hpp>

namespace NES
{
class DivLogicalFunction final : public LogicalFunctionConcept
{
public:
    ~DivLogicalFunction() noexcept override = default;
    static constexpr std::string_view NAME = "Div";

    DivLogicalFunction(LogicalFunction left, LogicalFunction right);
    DivLogicalFunction(const DivLogicalFunction& other);

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] const DataType& getStamp() const override;
    [[nodiscard]] LogicalFunction withStamp(std::shared_ptr<DataType> stamp) const override;
    [[nodiscard]] LogicalFunction withInferredStamp(Schema schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren()  const override;
    [[nodiscard]] LogicalFunction withChildren(std::vector<LogicalFunction> children) const override;

    [[nodiscard]] std::string getType() const override;
    [[nodiscard]] std::string toString() const override;

private:
    std::shared_ptr<DataType> stamp;
    LogicalFunction left;
    LogicalFunction right;
};
}
FMT_OSTREAM(NES::DivLogicalFunction);
