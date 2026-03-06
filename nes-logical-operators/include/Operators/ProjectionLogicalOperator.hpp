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
#include <Serialization/ReflectedOperator.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Variant.hpp>
#include <Util/Reflection.hpp>

#include "DataTypes/SchemaBase.hpp"

#include "Reorderer.hpp"
#include "Reprojecter.hpp"

namespace NES
{

/// Combines both selecting the fields to project and renaming/mapping of fields
class ProjectionLogicalOperator : public Reprojecter, public Reorderer
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

    ProjectionLogicalOperator(WeakLogicalOperator self, std::vector<UnboundProjection> projections, Asterisk asterisk);
    ProjectionLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::vector<UnboundProjection> projections, Asterisk asterisk);

    [[nodiscard]] std::vector<Projection> getProjections() const;
    [[nodiscard]] std::unordered_map<Field, std::unordered_set<Field>> getAccessedFieldsForOutput() const override;
    [[nodiscard]] bool hasAsterisk() const;

    [[nodiscard]] bool operator==(const ProjectionLogicalOperator& rhs) const;

    [[nodiscard]] ProjectionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] ProjectionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    /// @brief returns the fields of the child operator this projection accesses
    [[nodiscard]] std::vector<Field> getAccessedFields() const;

    [[nodiscard]] ProjectionLogicalOperator withInferredSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

    [[nodiscard]] const DynamicBase* getDynamicBase() const;


    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "Projection";

    std::optional<LogicalOperator> child;
    bool asterisk = false;
    std::vector<UnboundProjection> projections;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<ProjectionLogicalOperator>;
    friend Reflector<ProjectionLogicalOperator>;
};

template <>
struct Reflector<TypedLogicalOperator<ProjectionLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<ProjectionLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<ProjectionLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<ProjectionLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ProjectionLogicalOperator>);

}

template <>
struct std::hash<NES::ProjectionLogicalOperator>
{
    size_t operator()(const NES::ProjectionLogicalOperator& projectionOperator) const noexcept;
};

namespace NES::detail
{
struct ReflectedProjectionLogicalOperator
{
    OperatorId operatorId;
    bool asterisk;
    std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
};

}
