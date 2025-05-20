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
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
class AndLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "And";

    AndLogicalFunction(LogicalFunction left, LogicalFunction right);
    AndLogicalFunction(const AndLogicalFunction& other);
    ~AndLogicalFunction() override = default;

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
    LogicalFunction left, right;
};

}
FMT_OSTREAM(NES::AndLogicalFunction);
