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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

struct SinkLogicalOperator final
{
    /// During deserialization, we don't need to know/use the name of the sink anymore.
    SinkLogicalOperator() = default;
    /// During query parsing, we require the name of the sink and need to assign it an id.
    explicit SinkLogicalOperator(std::string sinkName);
    explicit SinkLogicalOperator(SinkDescriptor sinkDescriptor);

    [[nodiscard]] bool operator==(const SinkLogicalOperator& rhs) const;

    [[nodiscard]] SinkLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SinkLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SinkLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    [[nodiscard]] std::string getSinkName() const noexcept;
    [[nodiscard]] std::optional<SinkDescriptor> getSinkDescriptor() const;

    [[nodiscard]] SinkLogicalOperator withSinkDescriptor(SinkDescriptor sinkDescriptor) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> SINK_NAME{
            "SinkName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SINK_NAME, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(SINK_NAME);
    };

public:
    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "Sink";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;

    std::string sinkName;
    std::optional<SinkDescriptor> sinkDescriptor;

    friend class OperatorSerializationUtil;
};

template <>
struct Reflector<SinkLogicalOperator>
{
    Reflected operator()(const SinkLogicalOperator& op) const;
};

template <>
struct Unreflector<SinkLogicalOperator>
{
    SinkLogicalOperator operator()(const Reflected& reflected) const;
};

static_assert(LogicalOperatorConcept<SinkLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedSinkLogicalOperator
{
    std::optional<SinkDescriptor> sinkDescriptor;
    std::string sinkName;
};
}
