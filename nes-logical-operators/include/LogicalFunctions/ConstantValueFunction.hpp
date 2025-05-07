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
#include <Configurations/Descriptor.hpp>
#include <LogicalFunctions/Function.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES::Logical
{

/// This function node represents a constant value and a fixed data type.
/// Thus, the stamp of this function is always fixed.
class ConstantValueFunction final : public FunctionConcept
{
public:
    static constexpr std::string_view NAME = "ConstantValue";

    ConstantValueFunction(std::shared_ptr<DataType> stamp, std::string constantValueAsString);
    ConstantValueFunction(const ConstantValueFunction& other);
    ~ConstantValueFunction() noexcept override = default;

    std::string getConstantValue() const;

    [[nodiscard]] bool operator==(const FunctionConcept& rhs) const override;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] std::shared_ptr<DataType> getStamp() const override;
    [[nodiscard]] Function withStamp(std::shared_ptr<DataType> stamp) const override;
    [[nodiscard]] Function withInferredStamp(const Schema& schema) const override;

    [[nodiscard]] std::vector<Function> getChildren() const override;
    [[nodiscard]] Function withChildren(const std::vector<Function>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    /// Serialization
    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> CONSTANT_VALUE_AS_STRING{
            "constantValueAsString", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(CONSTANT_VALUE_AS_STRING, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(CONSTANT_VALUE_AS_STRING);
    };

private:
    const std::string constantValue;
    std::shared_ptr<DataType> stamp;
};
}
FMT_OSTREAM(NES::Logical::ConstantValueFunction);
