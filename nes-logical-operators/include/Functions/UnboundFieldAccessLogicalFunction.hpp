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
#include "FieldAccessLogicalFunction.hpp"


#include <Identifiers/Identifier.hpp>


#include "LogicalFunction.hpp"

#include <Configurations/Descriptor.hpp>

namespace NES
{


class UnboundFieldAccessLogicalFunction : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "UnboundFieldAccess";

    explicit UnboundFieldAccessLogicalFunction(Identifier fieldName);
    UnboundFieldAccessLogicalFunction(Identifier fieldName, DataType dataType);

    [[nodiscard]] Identifier getFieldName() const;
    [[nodiscard]] LogicalFunction withFieldName(Identifier fieldName) const;

    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;
    friend bool operator==(const UnboundFieldAccessLogicalFunction& lhs, const UnboundFieldAccessLogicalFunction& rhs);
    friend bool operator!=(const UnboundFieldAccessLogicalFunction& lhs, const UnboundFieldAccessLogicalFunction& rhs);

    [[nodiscard]] DataType getDataType() const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<Identifier> FIELD_NAME{
            "fieldName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_NAME, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(FIELD_NAME);
    };

private:
    Identifier fieldName;
    DataType dataType;
    friend std::hash<UnboundFieldAccessLogicalFunction>;
};

struct FieldAccessLogicalFunctionVariantWrapper
{
    std::variant<UnboundFieldAccessLogicalFunction, FieldAccessLogicalFunction> underlying;

    friend bool operator==(const FieldAccessLogicalFunctionVariantWrapper& lhs, const FieldAccessLogicalFunctionVariantWrapper& rhs)
        = default;

    FieldAccessLogicalFunction withInferredDataType(const Schema& schema) const;
};
}

template <>
struct std::hash<NES::UnboundFieldAccessLogicalFunction>
{
    std::size_t operator()(const NES::UnboundFieldAccessLogicalFunction& function) const noexcept;
};

template <>
struct std::hash<NES::FieldAccessLogicalFunctionVariantWrapper>
{
    std::size_t operator()(const NES::FieldAccessLogicalFunctionVariantWrapper& function) const noexcept;
};