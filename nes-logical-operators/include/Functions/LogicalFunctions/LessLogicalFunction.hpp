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

class LessLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "Less";

    LessLogicalFunction(LogicalFunction left, LogicalFunction right);
    LessLogicalFunction(const LessLogicalFunction& other);
    ~LessLogicalFunction() override = default;

    [[nodiscard]] SerializableFunction serialize() const override;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] std::shared_ptr<DataType> getStamp() const override;
    [[nodiscard]] LogicalFunction withStamp(std::shared_ptr<DataType> stamp) const override;
    [[nodiscard]] LogicalFunction withInferredStamp(Schema schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(std::vector<LogicalFunction> children) const override;

    [[nodiscard]] std::string getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

private:
    LogicalFunction left, right;
    std::shared_ptr<DataType> stamp;
};
}
FMT_OSTREAM(NES::LessLogicalFunction);
