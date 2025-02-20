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
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include "UnaryLogicalOperator.hpp"

namespace NES
{

/// Map operator, which contains a field assignment function that manipulates a field of the record.
class MapLogicalOperator : public UnaryLogicalOperator
{
public:
    static constexpr std::string_view NAME = "Map";

    MapLogicalOperator(std::shared_ptr<FieldAssignmentLogicalFunction> const& mapFunction);

    [[nodiscard]] std::shared_ptr<FieldAssignmentLogicalFunction> getMapFunction() const;

    /// @brief Infers the schema of the map operator. We support two cases:
    /// 1. the assignment statement manipulates a already existing field. In this case the data type of the field can change.
    /// 2. the assignment statement creates a new field with an inferred data type.
    /// @throws throws exception if inference was not possible.
    /// @param typeInferencePhaseContext needed for stamp inferring
    /// @return true if inference was possible
    [[nodiscard]] bool inferSchema() override;

    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    std::shared_ptr<Operator> clone() const override;

    [[nodiscard]] SerializableOperator serialize() const override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> MAP_FUNCTION_NAME{
            "mapFunctionName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(MAP_FUNCTION_NAME, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(MAP_FUNCTION_NAME);
    };

protected:
    std::string toString() const override;

private:
    const std::shared_ptr<FieldAssignmentLogicalFunction> mapFunction;
};
}
