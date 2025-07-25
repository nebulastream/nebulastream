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
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus, the dataType of this function is always fixed.
class ConstantValueLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "ConstantValue";

    ConstantValueLogicalFunction(DataType dataType, std::string constantValueAsString);

    [[nodiscard]] std::string getConstantValue() const;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] DataType getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> CONSTANT_VALUE_AS_STRING{
            "constantValueAsString",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(CONSTANT_VALUE_AS_STRING, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(CONSTANT_VALUE_AS_STRING);
    };

private:
    const std::string constantValue;
    DataType dataType;
};
}
FMT_OSTREAM(NES::ConstantValueLogicalFunction);
