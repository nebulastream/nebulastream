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

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

inline OperatorId getNextLogicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}

/// Concept defining the interface for all logical operators in the query plan.
/// This concept defines the common interface that all logical operators must implement.
/// Logical operators represent operations in the query plan and are used during query
/// planning and optimization.
/// TODO #875: Investigate C++20 Concepts to replace Operator/Function Inheritance
struct LogicalOperatorConcept
{
    virtual ~LogicalOperatorConcept() = default;

    explicit LogicalOperatorConcept();
    explicit LogicalOperatorConcept(OperatorId existingId);

    /// Returns a string representation of the operator
    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;

    /// Returns the children operators of this operator
    [[nodiscard]] virtual std::vector<struct LogicalOperator> getChildren() const = 0;

    /// Creates a new operator with the given children
    [[nodiscard]] virtual LogicalOperator withChildren(std::vector<LogicalOperator>) const = 0;

    /// Creates a new operator with the given traits
    [[nodiscard]] virtual LogicalOperator withTraitSet(TraitSet) const = 0;

    /// Compares this operator with another for equality
    [[nodiscard]] virtual bool operator==(const LogicalOperatorConcept& rhs) const = 0;

    /// Returns the name of the operator, used during planning and optimization
    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;

    /// Serializes the operator to a protobuf message
    [[nodiscard]] virtual SerializableOperator serialize() const = 0;

    /// Returns the trait set of the operator
    [[nodiscard]] virtual TraitSet getTraitSet() const = 0;

    /// Returns the input schemas of the operator
    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;

    /// Returns the output schema of the operator
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;

    /// Returns the input origin IDs of the operator
    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;

    /// Returns the output origin IDs of the operator
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;

    /// Creates a new operator with the given input origin IDs
    [[nodiscard]] virtual LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>>) const = 0;

    /// Creates a new operator with the given output origin IDs
    [[nodiscard]] virtual LogicalOperator withOutputOriginIds(std::vector<OriginId>) const = 0;

    /// Creates a new operator with inferred schema based on input schemas
    [[nodiscard]] virtual LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const = 0;

    /// Unique identifier for this operator
    /// Currently not const to allow serialization.
    OperatorId id = INVALID_OPERATOR_ID;
};

/// A type-erased wrapper for logical operators.
/// C.f.: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
/// This class provides type erasure for logical operators, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual operator implementation.
/// @tparam T The type of the logical operator. Must inherit from LogicalOperatorConcept.
template <typename T>
concept IsLogicalOperator = std::is_base_of_v<LogicalOperatorConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Id is preserved during copy
struct LogicalOperator
{
    /// Constructs a LogicalOperator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsLogicalOperator concept.
    /// @param op The operator to wrap.
    template <IsLogicalOperator T>
    LogicalOperator(const T& op) : self(std::make_shared<Model<T>>(std::move(op), std::move(op.id))) /// NOLINT
    {
    }

    LogicalOperator();

    /// Attempts to get the underlying operator as type T.
    /// @tparam T The type to try to get the operator as.
    /// @return std::optional<T> The operator if it is of type T, nullopt otherwise.
    template <typename T>
    [[nodiscard]] std::optional<T> tryGet() const
    {
        if (auto model = dynamic_cast<const Model<T>*>(self.get()))
        {
            return model->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying operator as type T.
    /// @tparam T The type to get the operator as.
    /// @return const T The operator.
    /// @throw InvalidDynamicCast If the operator is not of type T.
    template <typename T>
    [[nodiscard]] T get() const
    {
        if (auto model = dynamic_cast<const Model<T>*>(self.get()))
        {
            return model->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    /// Static traits defined as member variables will be present in the new operator nonetheless
    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const;

    [[nodiscard]] OperatorId getId() const;

    [[nodiscard]] bool operator==(const LogicalOperator& other) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SerializableOperator serialize() const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const;

    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const;
    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    struct Concept : LogicalOperatorConcept
    {
        explicit Concept(OperatorId existingId) : LogicalOperatorConcept(existingId) { }
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template <IsLogicalOperator OperatorType>
    struct Model : Concept
    {
        OperatorType data;

        explicit Model(OperatorType d) : Concept(getNextLogicalOperatorId()), data(std::move(d)) { }

        Model(OperatorType d, OperatorId existingId) : Concept(existingId), data(std::move(d)) { }

        [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return data.explain(verbosity); }

        [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return data.getChildren(); }

        [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override
        {
            return data.withChildren(children);
        }

        [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const override
        {
            return data.withTraitSet(traitSet);
        }

        [[nodiscard]] std::string_view getName() const noexcept override { return data.getName(); }

        [[nodiscard]] SerializableOperator serialize() const override { return data.serialize(); }

        [[nodiscard]] TraitSet getTraitSet() const override { return data.getTraitSet(); }

        [[nodiscard]] std::vector<Schema> getInputSchemas() const override { return data.getInputSchemas(); }

        [[nodiscard]] Schema getOutputSchema() const override { return data.getOutputSchema(); }

        [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override { return data.getInputOriginIds(); }

        [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override { return data.getOutputOriginIds(); }

        [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override
        {
            return data.withInputOriginIds(ids);
        }

        [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override
        {
            return data.withOutputOriginIds(ids);
        }

        [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override
        {
            return data.withInferredSchema(inputSchemas);
        }

        [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override
        {
            if (const auto* p = dynamic_cast<const Concept*>(&rhs))
            {
                return equals(*p);
            }
            return false;
        }

        [[nodiscard]] bool equals(const Concept& other) const override
        {
            if (auto p = dynamic_cast<const Model*>(&other))
            {
                return data.operator==(p->data);
            }
            return false;
        }
    };

    std::shared_ptr<const Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const LogicalOperator& op)
{
    return os << op.explain(ExplainVerbosity::Short);
}
}

/// Hash is based solely on unique identifier (needed for e.g. unordered_set)
namespace std
{
template <>
struct hash<NES::LogicalOperator>
{
    std::size_t operator()(const NES::LogicalOperator& op) const noexcept { return std::hash<NES::OperatorId>{}(op.getId()); }
};
}
FMT_OSTREAM(NES::LogicalOperator);
