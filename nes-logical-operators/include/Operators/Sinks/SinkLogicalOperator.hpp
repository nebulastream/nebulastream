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
#include <string>
#include <Operators/LogicalOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

struct SinkLogicalOperator final : LogicalOperatorConcept
{
    /// During deserialization, we don't need to know/use the name of the sink anymore.
    SinkLogicalOperator() = default;
    /// During query parsing, we require the name of the sink and need to assign it an id.
    SinkLogicalOperator(std::string sinkName);

    std::string sinkName;
    std::shared_ptr<Sinks::SinkDescriptor> sinkDescriptor;

    /// currently only use for testing purposes in IntegrationTestUtil
    void setOutputSchema(Schema schema);

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
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> SINK_NAME{
            "SinkName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(SINK_NAME, config); }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(SINK_NAME);
    };

private:
    static constexpr std::string_view NAME = "Sink";

    std::vector<LogicalOperator> children;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;
};
}
