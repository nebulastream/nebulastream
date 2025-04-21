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

#include <Functions/FieldAccessLogicalFunction.hpp>

namespace NES
{
/// @brief A RenameLogicalFunction allows us to rename an attribute value via `as` in the query
class RenameLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "Rename";

    RenameLogicalFunction(const FieldAccessLogicalFunction& originalField, std::string newFieldName);
    RenameLogicalFunction(const RenameLogicalFunction& other);

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] std::string getNewFieldName() const;
    [[nodiscard]] const FieldAccessLogicalFunction& getOriginalField() const;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;

    [[nodiscard]] std::shared_ptr<DataType> getStamp() const override;
    [[nodiscard]] LogicalFunction withStamp(std::shared_ptr<DataType> stamp) const override;
    [[nodiscard]] LogicalFunction withInferredStamp(Schema schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(std::vector<LogicalFunction> children) const override;

    [[nodiscard]] std::string getType() const override;
    [[nodiscard]] std::string toString() const override;

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> NEW_FIELD_NAME{
            "newFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(NEW_FIELD_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(NEW_FIELD_NAME);
    };

private:
    FieldAccessLogicalFunction child;
    std::shared_ptr<DataType> stamp;
    std::string newFieldName;
};
}
FMT_OSTREAM(NES::RenameLogicalFunction);
