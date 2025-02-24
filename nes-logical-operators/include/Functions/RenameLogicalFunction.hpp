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
#include <Functions/UnaryLogicalFunction.hpp>

namespace NES
{
/// @brief A RenameLogicalFunction allows us to rename an attribute value via `as` in the query
class RenameLogicalFunction final : public UnaryLogicalFunction
{
public:
    static constexpr std::string_view NAME = "Rename";

    RenameLogicalFunction(const std::shared_ptr<FieldAccessLogicalFunction>& originalField, std::string newFieldName);

    void inferStamp(const Schema& schema) override;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] std::string getNewFieldName() const;
    [[nodiscard]] std::shared_ptr<FieldAccessLogicalFunction> getOriginalField() const;

    [[nodiscard]] std::span<const std::shared_ptr<LogicalFunction>> getChildren() const override;
    [[nodiscard]] bool operator==(std::shared_ptr<LogicalFunction> const& rhs) const override;
    [[nodiscard]] std::shared_ptr<LogicalFunction> clone() const override;

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
    explicit RenameLogicalFunction(const RenameLogicalFunction& other);
    [[nodiscard]] std::string toString() const override;

    std::array<std::shared_ptr<LogicalFunction>, 1> child;
    std::string newFieldName;
};
}
FMT_OSTREAM(NES::RenameLogicalFunction);
