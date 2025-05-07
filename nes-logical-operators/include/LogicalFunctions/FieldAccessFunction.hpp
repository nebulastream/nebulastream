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

/// @brief A FieldAccessFunction reads a specific field of the current record.
/// It can be created typed or untyped.
class FieldAccessFunction : public FunctionConcept
{
public:
    static constexpr std::string_view NAME = "FieldAccess";

    FieldAccessFunction(std::string fieldName);
    FieldAccessFunction(std::shared_ptr<DataType> stamp, std::string fieldName);

    [[nodiscard]] std::string getFieldName() const;
    [[nodiscard]] Function withFieldName(std::string fieldName) const;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const FunctionConcept& rhs) const override;
    friend bool operator==(const FieldAccessFunction& lhs, const FieldAccessFunction& rhs);
    friend bool operator!=(const FieldAccessFunction& lhs, const FieldAccessFunction& rhs);

    [[nodiscard]] std::shared_ptr<DataType> getStamp() const override;
    [[nodiscard]] Function withStamp(std::shared_ptr<DataType> stamp) const override;
    [[nodiscard]] Function withInferredStamp(const Schema& schema) const override;

    [[nodiscard]] std::vector<Function> getChildren() const override;
    [[nodiscard]] Function withChildren(const std::vector<Function>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    /// Serialization
    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> FIELD_NAME{
            "fieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(FIELD_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(FIELD_NAME);
    };

private:
    std::string fieldName;
    std::shared_ptr<DataType> stamp;
};

}
FMT_OSTREAM(NES::Logical::FieldAccessFunction);
