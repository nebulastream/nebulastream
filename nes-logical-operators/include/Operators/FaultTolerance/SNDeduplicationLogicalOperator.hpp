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

#include <string>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>

namespace NES
{

class SNDeduplicationLogicalOperator : public ManagedByOperator
{
public:
    explicit SNDeduplicationLogicalOperator(WeakLogicalOperator self, std::string filePath);
    explicit SNDeduplicationLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::string filePath);

    static TypedLogicalOperator<SNDeduplicationLogicalOperator> create(std::string filePath);
    static TypedLogicalOperator<SNDeduplicationLogicalOperator> create(LogicalOperator child, std::string filePath);

    [[nodiscard]] bool operator==(const SNDeduplicationLogicalOperator& rhs) const;

    [[nodiscard]] SNDeduplicationLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SNDeduplicationLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SNDeduplicationLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;


    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const; //TODO
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SNDeduplicationLogicalOperator withInferredSchema() const;

    [[nodiscard]] std::string getFilePath() const;

private:
    static constexpr std::string_view NAME = "SNDeduplication";
    std::optional<LogicalOperator> child;
    std::string filePath;


    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;

    friend struct std::hash<SNDeduplicationLogicalOperator>;
    friend Reflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>;
};

namespace detail
{
struct ReflectedSNDeduplicationLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::string filePath;
};
}

template <>
struct Reflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SNDeduplicationLogicalOperator>&) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<SNDeduplicationLogicalOperator> operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(LogicalOperatorConcept<SNDeduplicationLogicalOperator>);
}

template <>
struct std::hash<NES::SNDeduplicationLogicalOperator>
{
    uint64_t operator()(const NES::SNDeduplicationLogicalOperator& op) const noexcept;
};
