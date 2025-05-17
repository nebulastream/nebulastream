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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus, the dataType of this function is always fixed.
class ConstantValueLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "ConstantValue";

    ConstantValueLogicalFunction(std::shared_ptr<DataType> dataType, std::string constantValueAsString);
    ConstantValueLogicalFunction(const ConstantValueLogicalFunction& other);
    ~ConstantValueLogicalFunction() noexcept override = default;

    [[nodiscard]] std::string getConstantValue() const;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] std::shared_ptr<DataType> getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(std::shared_ptr<DataType> dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> CONSTANT_VALUE_AS_STRING{
            "constantValueAsString",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(CONSTANT_VALUE_AS_STRING, config); }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(CONSTANT_VALUE_AS_STRING);
    };

private:
    const std::string constantValue;
    std::shared_ptr<DataType> dataType;
};
}
FMT_OSTREAM(NES::ConstantValueLogicalFunction);
