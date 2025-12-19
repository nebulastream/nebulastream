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
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// Combines both selecting the fields to project and renaming/mapping of fields
class ProjectionLogicalOperator
{
public:
    class Asterisk
    {
        bool value;

    public:
        explicit Asterisk(bool value) : value(value) { }

        friend ProjectionLogicalOperator;
    };

    using Projection = std::pair<std::optional<FieldIdentifier>, LogicalFunction>;
    ProjectionLogicalOperator(std::vector<Projection> projections, Asterisk asterisk);

    [[nodiscard]] const std::vector<Projection>& getProjections() const;

    [[nodiscard]] bool operator==(const ProjectionLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] ProjectionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ProjectionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] std::vector<std::string> getAccessedFields() const;

    [[nodiscard]] ProjectionLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> PROJECTION_FUNCTION_NAME{
            "projectionFunctionName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(PROJECTION_FUNCTION_NAME, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> ASTERISK{
            "asterisk",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(ASTERISK, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(PROJECTION_FUNCTION_NAME, ASTERISK);
    };

private:
    static constexpr std::string_view NAME = "Projection";
    std::vector<Projection> projections;

    bool asterisk = false;
    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

static_assert(LogicalOperatorConcept<ProjectionLogicalOperator>);

}
