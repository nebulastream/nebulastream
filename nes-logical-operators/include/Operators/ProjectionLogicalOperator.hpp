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
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

#include "Reprojecter.hpp"

namespace NES
{

class Schema;

/// Combines both selecting the fields to project and renaming/mapping of fields
class ProjectionLogicalOperator : public Reprojecter
{
public:
    class Asterisk
    {
        bool value;

    public:
        explicit Asterisk(const bool value) : value(value) { }

        friend ProjectionLogicalOperator;
    };

    using Projection = std::pair<Field, LogicalFunction>;
    using UnboundProjection = std::pair<Identifier, LogicalFunction>;
    using ProjectionVariant = NES::Util::VariantContainer<std::vector, Projection, UnboundProjection>;

    ProjectionLogicalOperator(WeakLogicalOperator self, std::vector<UnboundProjection> projections, Asterisk asterisk);
    ProjectionLogicalOperator(WeakLogicalOperator self, LogicalOperator children, DescriptorConfig::Config config);

    [[nodiscard]] const std::vector<Projection>& getProjections() const;
    [[nodiscard]] std::unordered_map<Field, std::unordered_set<Field>> getAccessedFieldsForOutput() const override;

    [[nodiscard]] bool operator==(const ProjectionLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] ProjectionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ProjectionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    /// @brief returns the fields of the child operator this projection accesses
    [[nodiscard]] std::vector<Field> getAccessedFields() const;

    [[nodiscard]] ProjectionLogicalOperator withInferredSchema() const;

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

    LogicalOperator child;
    bool asterisk = false;
    ProjectionVariant projections;

    WeakLogicalOperator self;
    /// Set during schema inference
    std::optional<UnboundSchemaBase<1>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<ProjectionLogicalOperator>;
};

static_assert(LogicalOperatorConcept<ProjectionLogicalOperator>);

}
template <>
struct std::hash<NES::ProjectionLogicalOperator>
{
    size_t operator()(const NES::ProjectionLogicalOperator& projectionOperator) const noexcept;
};
