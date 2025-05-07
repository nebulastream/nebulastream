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

#include <LogicalFunctions/FieldAccessFunction.hpp>

namespace NES::Logical
{
/// @brief A RenameFunction allows us to rename an attribute value via `as` in the query
class RenameFunction final : public FunctionConcept
{
public:
    static constexpr std::string_view NAME = "Rename";

    RenameFunction(const FieldAccessFunction& originalField, std::string newFieldName);
    RenameFunction(const RenameFunction& other);

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] std::string getNewFieldName() const;
    [[nodiscard]] const FieldAccessFunction& getOriginalField() const;

    [[nodiscard]] bool operator==(const FunctionConcept& rhs) const override;

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
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> NEW_FIELD_NAME{
            "newFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(NEW_FIELD_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(NEW_FIELD_NAME);
    };

private:
    std::shared_ptr<DataType> stamp;
    FieldAccessFunction child;
    std::string newFieldName;
};
}
FMT_OSTREAM(NES::Logical::RenameFunction);
