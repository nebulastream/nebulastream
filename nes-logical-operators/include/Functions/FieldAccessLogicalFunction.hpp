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
class FieldAccessLogicalFunction : public LogicalFunction
{
public:
    static constexpr std::string_view NAME = "FieldAccess";

    explicit FieldAccessLogicalFunction(std::string fieldName);
    explicit FieldAccessLogicalFunction(std::shared_ptr<DataType> stamp, std::string fieldName);

    void inferStamp(const Schema& schema) override;

    [[nodiscard]] std::string getFieldName() const;
    [[nodiscard]] LogicalFunction withFieldName(std::string fieldName) const;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(std::shared_ptr<LogicalFunction> const& rhs) const override;
    [[nodiscard]] std::shared_ptr<LogicalFunction> clone() const override;
    [[nodiscard]] std::span<const std::shared_ptr<LogicalFunction>> getChildren() const override;

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
    explicit FieldAccessLogicalFunction(const FieldAccessLogicalFunction& other);
    std::string toString() const override;
    std::string fieldName;
};

}
FMT_OSTREAM(NES::FieldAccessLogicalFunction);
