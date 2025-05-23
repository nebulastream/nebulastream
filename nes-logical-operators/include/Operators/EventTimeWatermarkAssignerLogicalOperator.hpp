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
#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

class EventTimeWatermarkAssignerLogicalOperator : public LogicalOperatorConcept
{
public:
    EventTimeWatermarkAssignerLogicalOperator(LogicalFunction onField, Windowing::TimeUnit unit);

    LogicalFunction onField;
    Windowing::TimeUnit unit;

    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] TraitSet getTraitSet() const override;

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

    struct ConfigParameters
    {
        static inline const Configurations::DescriptorConfig::ConfigParameter<uint64_t> TIME_MS{
            "TimeMs",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return Configurations::DescriptorConfig::tryGet(TIME_MS, config); }};
        static inline const Configurations::DescriptorConfig::ConfigParameter<FunctionList> FUNCTION{
            "Function",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return Configurations::DescriptorConfig::tryGet(FUNCTION, config); }};

        static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = Configurations::DescriptorConfig::createConfigParameterContainerMap(TIME_MS, FUNCTION);
    };

private:
    static constexpr std::string_view NAME = "EventTimeWatermarkAssigner";

    std::vector<LogicalOperator> children;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
