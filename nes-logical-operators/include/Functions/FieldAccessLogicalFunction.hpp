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
#include <Configurations/Descriptor.hpp>

namespace NES
{

/// @brief A FieldAccessFunction reads a specific field of the current record.
/// It can be created typed or untyped.
class FieldAccessLogicalFunction : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "FieldAccess";

    FieldAccessLogicalFunction(std::string fieldName);
    FieldAccessLogicalFunction(std::shared_ptr<DataType> stamp, std::string fieldName);
    FieldAccessLogicalFunction(const FieldAccessLogicalFunction& other);
    FieldAccessLogicalFunction& operator=(const FieldAccessLogicalFunction& other) = default;

    bool inferStamp(Schema schema) override;

    [[nodiscard]] std::string getFieldName() const;
    [[nodiscard]] LogicalFunction withFieldName(std::string fieldName) const;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const;

    const DataType& getStamp() const override { return *stamp; };
    void setStamp(std::shared_ptr<DataType> stamp) override { this->stamp = stamp; };
    std::vector<LogicalFunction> getChildren() const override { throw UnsupportedOperation(); };
    std::string getType() const override { return std::string(NAME); }

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> FIELD_NAME{
            "fieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(FIELD_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(FIELD_NAME);
    };

    std::string toString() const override;

private:
    std::string fieldName;
    std::shared_ptr<DataType> stamp;
};

}
FMT_OSTREAM(NES::FieldAccessLogicalFunction);
