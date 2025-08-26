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

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// Reads out a reservoir sample from the memory layout created by the ReservoirSampleLogicalFunction to be understandable by a sink.
class ReservoirProbeLogicalOperator : public LogicalOperatorConcept
{
public:
    explicit ReservoirProbeLogicalOperator(FieldAccessLogicalFunction asField, Schema sampleSchema);

    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] LogicalOperator withTraitSet(TraitSet) const override;
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
        static inline const DescriptorConfig::ConfigParameter<std::string> SAMPLE_AS_FIELD{
            "sampleAsField",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(SAMPLE_AS_FIELD, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> SAMPLE_SCHEMA{
            "sampleSchema",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(SAMPLE_SCHEMA, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(SAMPLE_AS_FIELD, SAMPLE_SCHEMA);
    };

    /// Name of the field the sample is in.
    FieldAccessLogicalFunction asField;
    std::optional<Schema> sampleSchema;

private:
    static constexpr std::string_view NAME = "ReservoirProbe";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
