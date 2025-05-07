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

#include <API/TimeUnit.hpp>
#include <LogicalFunctions/Function.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Configurations/Descriptor.hpp>

using namespace NES;
namespace NES::Logical
{

class EventTimeWatermarkAssignerOperator : public OperatorConcept
{
public:
    EventTimeWatermarkAssignerOperator(Function onField, Windowing::TimeUnit unit);

    /// Operator specific member
    Function onField;
    Windowing::TimeUnit unit;

    /// OperatorConcept member
    [[nodiscard]] bool operator==(const OperatorConcept& rhs) const override;
    [[nodiscard]] NES::SerializableOperator serialize() const override;

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;

    [[nodiscard]] Operator withChildren(std::vector<Operator> children) const override;
    [[nodiscard]] std::vector<Operator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] Operator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] Operator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] Operator withInferredSchema(std::vector<Schema> inputSchemas) const override;

    /// Serialization
    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<uint64_t> TIME_MS{
            "TimeMs", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(TIME_MS, config);
            }};
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<FunctionList> FUNCTION{
            "Function", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(FUNCTION, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(TIME_MS, FUNCTION);
    };

private:
    /// Operator specific member
    static constexpr std::string_view NAME = "EventTimeWatermarkAssigner";

    /// OperatorConcept member
    std::vector<Operator> children;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
