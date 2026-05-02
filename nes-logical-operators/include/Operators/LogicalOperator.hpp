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
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <DataTypes/LogicalSchema.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES
{

inline OperatorId getNextLogicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}

namespace detail
{
struct ErasedLogicalOperator;
}

template <typename Checked = NES::detail::ErasedLogicalOperator>
struct TypedLogicalOperator;
using LogicalOperator = TypedLogicalOperator<>;

/// LogicalSchema-first API a concrete operator may expose. Operators that opt
/// into this API can natively carry compound LogicalTypes through their input
/// and output schemas — the only operators that need to do so are the ones
/// that introduce or propagate compound types (e.g. Projection over a Point
/// constructor function).
template <typename T>
concept HasLogicalSchemaAPI = requires(const T& thisOperator, std::vector<LogicalSchema> inputs) {
    { thisOperator.getInputLogicalSchemas() } -> std::convertible_to<std::vector<LogicalSchema>>;
    { thisOperator.getOutputLogicalSchema() } -> std::convertible_to<LogicalSchema>;
    { thisOperator.withInferredLogicalSchema(inputs) } -> std::convertible_to<T>;
};

/// Legacy primitive-Schema API. The bulk of existing operators are still in
/// this dialect; OperatorModel<T> bridges them by lifting LogicalSchemas in
/// (assuming primitive-only) and lifting Schemas out.
template <typename T>
concept HasLegacySchemaAPI = requires(const T& thisOperator, std::vector<Schema> inputs) {
    { thisOperator.getInputSchemas() } -> std::convertible_to<std::vector<Schema>>;
    { thisOperator.getOutputSchema() } -> std::convertible_to<Schema>;
    { thisOperator.withInferredSchema(inputs) } -> std::convertible_to<T>;
};

/// Concept defining the interface for all logical operators in the query plan.
/// This concept defines the common interface that all logical operators must implement.
/// Logical operators represent operations in the query plan and are used during query
/// planning and optimization.
///
/// An operator may satisfy the schema portion of the concept either via the
/// LogicalSchema-first API or the legacy primitive-Schema API; OperatorModel
/// bridges between them. Compound LogicalTypes are only valid in operators
/// that opt into HasLogicalSchemaAPI.
/// Trait excluding the wrapper TypedLogicalOperator from concept satisfaction,
/// since the wrapper exposes both schema APIs as forwarders and would otherwise
/// cause a recursive constraint check during overload resolution of its own
/// templated constructors.
template <typename T>
struct IsTypedLogicalOperator : std::false_type
{
};
template <typename Checked>
struct IsTypedLogicalOperator<TypedLogicalOperator<Checked>> : std::true_type
{
};

template <typename T>
concept LogicalOperatorConcept
    = (not IsTypedLogicalOperator<std::remove_cvref_t<T>>::value)
    && (HasLogicalSchemaAPI<T> || HasLegacySchemaAPI<T>) && requires(
        const T& thisOperator,
        ExplainVerbosity verbosity,
        OperatorId operatorId,
        std::vector<LogicalOperator> children,
        TraitSet traitSet,
        const T& rhs) {
    /// Returns a string representation of the operator
    { thisOperator.explain(verbosity, operatorId) } -> std::convertible_to<std::string>;

    /// Returns the children operators of this operator
    { thisOperator.getChildren() } -> std::convertible_to<std::vector<LogicalOperator>>;

    /// Creates a new operator with the given children
    { thisOperator.withChildren(children) } -> std::convertible_to<T>;

    /// Creates a new operator with the given traits
    { thisOperator.withTraitSet(traitSet) } -> std::convertible_to<T>;

    /// Compares this operator with another for equality
    { thisOperator == rhs } -> std::convertible_to<bool>;

    /// Returns the name of the operator, used during planning and optimization
    { thisOperator.getName() } noexcept -> std::convertible_to<std::string_view>;

    /// Serialize the operator to a Reflected object
    { NES::reflect(thisOperator) } -> std::same_as<Reflected>;

    /// Returns the trait set of the operator
    { thisOperator.getTraitSet() } -> std::convertible_to<TraitSet>;
};

namespace detail
{
/// @brief A type erased wrapper for logical operators
struct ErasedLogicalOperator
{
    virtual ~ErasedLogicalOperator() = default;

    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;
    [[nodiscard]] virtual std::vector<LogicalOperator> getChildren() const = 0;
    [[nodiscard]] virtual LogicalOperator withChildren(std::vector<LogicalOperator> children) const = 0;
    [[nodiscard]] virtual LogicalOperator withTraitSet(TraitSet traitSet) const = 0;
    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual Reflected reflect() const = 0;
    [[nodiscard]] virtual TraitSet getTraitSet() const = 0;
    [[nodiscard]] virtual std::vector<LogicalSchema> getInputLogicalSchemas() const = 0;
    [[nodiscard]] virtual LogicalSchema getOutputLogicalSchema() const = 0;
    [[nodiscard]] virtual LogicalOperator withInferredLogicalSchema(std::vector<LogicalSchema> inputSchemas) const = 0;
    [[nodiscard]] virtual bool equals(const ErasedLogicalOperator& other) const = 0;
    [[nodiscard]] virtual OperatorId getOperatorId() const = 0;
    [[nodiscard]] virtual LogicalOperator withOperatorId(OperatorId id) const = 0;

    friend bool operator==(const ErasedLogicalOperator& lhs, const ErasedLogicalOperator& rhs) { return lhs.equals(rhs); }


private:
    template <typename T>
    friend struct NES::TypedLogicalOperator;
    ///If the operator inherits from DynamicBase (over Castable), then returns a pointer to the wrapped operator as DynamicBase,
    ///so that we can then safely try to dyncast the DynamicBase* to Castable<T>*
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};


template <LogicalOperatorConcept OperatorType>
struct OperatorModel;
}

/// @brief A type erased wrapper for heap-allocated logical operators.
/// @tparam Checked A logical operator type that this wrapper guarantees it contains.
/// Either a type erased virtual logical operator class or the actual type.
/// You can cast with TypedLogicalOperator::tryGetAs, to try to get a TypedLogicalOperator for a specific type.
template <typename Checked>
struct TypedLogicalOperator
{
    template <LogicalOperatorConcept T>
    requires std::same_as<Checked, NES::detail::ErasedLogicalOperator>
    TypedLogicalOperator(TypedLogicalOperator<T> other) : self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    /// Constructs a LogicalOperator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsLogicalOperator concept.
    /// @param op The operator to wrap.
    template <LogicalOperatorConcept T>
    TypedLogicalOperator(const T& op) : self(std::make_shared<NES::detail::OperatorModel<T>>(op)) /// NOLINT(google-explicit-constructor)
    {
    }

    template <LogicalOperatorConcept T>
    TypedLogicalOperator(const NES::detail::OperatorModel<T>& op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::OperatorModel<T>>(op.impl, op.id))
    {
    }

    explicit TypedLogicalOperator(std::shared_ptr<const NES::detail::ErasedLogicalOperator> op) : self(std::move(op)) { }

    ///@brief Alternative to operator*
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedLogicalOperator, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedLogicalOperator>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::OperatorModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedLogicalOperator, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedLogicalOperator>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::OperatorModel<Checked>>(self)->impl;
        }
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedLogicalOperator, Checked>)
        {
            return std::dynamic_pointer_cast<const NES::detail::ErasedLogicalOperator>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const NES::detail::OperatorModel<Checked>>(self);
            return &casted->impl;
        }
    }

    TypedLogicalOperator() = delete;

    /// Attempts to get the underlying operator as type T.
    /// @tparam T The type to try to get the operator as.
    /// @return std::optional<TypedLogicalOperator<T>> The operator if it is of type T, nullopt otherwise.
    template <LogicalOperatorConcept T>
    std::optional<TypedLogicalOperator<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::OperatorModel<T>>(self))
        {
            return TypedLogicalOperator<T>{std::static_pointer_cast<const NES::detail::ErasedLogicalOperator>(model)};
        }
        return std::nullopt;
    }

    /// Attempts to get the underlying operator as type T.
    /// @tparam T The type to try to get the operator as.
    /// @return std::optional<std::shared_ptr<const Castable<T>>> The operator if it is of type T and Castable<T>, nullopt otherwise.
    template <typename T>
    requires(!LogicalOperatorConcept<T>)
    std::optional<std::shared_ptr<const Castable<T>>> tryGetAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        return std::nullopt;
    }

    /// Gets the underlying operator as type T.
    /// @tparam T The type to get the operator as.
    /// @return TypedLogicalOperator<T> The operator.
    /// @throw InvalidDynamicCast If the operator is not of type T.
    template <LogicalOperatorConcept T>
    TypedLogicalOperator<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::OperatorModel<T>>(self))
        {
            return TypedLogicalOperator<T>{std::static_pointer_cast<const NES::detail::ErasedLogicalOperator>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
        std::unreachable();
    }

    /// Gets the underlying operator as type T.
    /// @tparam T The type to get the operator as.
    /// @return std::shared_ptr<const Castable<T>> The operator.
    /// @throw InvalidDynamicCast If the operator is not of type T or does not inherit from Castable<T>.
    template <typename T>
    requires(!LogicalOperatorConcept<T>)
    std::shared_ptr<const Castable<T>> getAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
        std::unreachable();
    }

    [[nodiscard]] std::string explain(const ExplainVerbosity verbosity) const { return self->explain(verbosity); }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return self->getChildren(); };

    /// Returns the type-erased LogicalOperator. The transformation goes through
    /// the erased layer, so the typed identity (Checked) cannot be preserved —
    /// callers that need the typed view must `tryGetAs` again on the result.
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const
    {
        return self->withChildren(std::move(children));
    }

    /// Static traits defined as member variables will be present in the new operator nonetheless
    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const { return self->withTraitSet(std::move(traitSet)); }

    [[nodiscard]] OperatorId getId() const { return self->getOperatorId(); }

    [[nodiscard]] bool operator==(const LogicalOperator& other) const { return self->equals(*other.self); }

    [[nodiscard]] std::string_view getName() const noexcept { return self->getName(); }

    [[nodiscard]] TraitSet getTraitSet() const { return self->getTraitSet(); }

    [[nodiscard]] std::vector<LogicalSchema> getInputLogicalSchemas() const { return self->getInputLogicalSchemas(); }

    [[nodiscard]] LogicalSchema getOutputLogicalSchema() const { return self->getOutputLogicalSchema(); }

    [[nodiscard]] LogicalOperator withInferredLogicalSchema(std::vector<LogicalSchema> inputSchemas) const
    {
        return self->withInferredLogicalSchema(std::move(inputSchemas));
    }

    /// Bridge convenience: existing optimizer phases and the query-compiler
    /// still reason in primitive Schema. These forwarders project the
    /// LogicalSchema-typed view through `LogicalSchema::toPrimitiveSchema` /
    /// `LogicalSchema::fromSchema`. Compound LogicalTypes will throw inside the
    /// bridge until the dedicated lowering phase has run.
    [[nodiscard]] std::vector<Schema> getInputSchemas() const
    {
        std::vector<Schema> primitive;
        for (const auto& logicalSchema : self->getInputLogicalSchemas())
        {
            primitive.push_back(logicalSchema.toPrimitiveSchema());
        }
        return primitive;
    }

    [[nodiscard]] Schema getOutputSchema() const { return self->getOutputLogicalSchema().toPrimitiveSchema(); }

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const
    {
        std::vector<LogicalSchema> lifted;
        lifted.reserve(inputSchemas.size());
        for (const auto& schema : inputSchemas)
        {
            lifted.push_back(LogicalSchema::fromSchema(schema));
        }
        return self->withInferredLogicalSchema(std::move(lifted));
    }

private:
    friend class QueryPlanSerializationUtil;
    friend class OperatorSerializationUtil;

    template <typename FriendChecked>
    friend struct TypedLogicalOperator;

    [[nodiscard]] LogicalOperator withOperatorId(const OperatorId id) const { return self->withOperatorId(id); };

    std::shared_ptr<const NES::detail::ErasedLogicalOperator> self;
};

namespace detail
{

/// @brief Wrapper type that acts as a bridge between a type satisfying LogicalOperatorConcept and TypedLogicalOperator
template <LogicalOperatorConcept OperatorType>
struct OperatorModel : ErasedLogicalOperator
{
    OperatorType impl;
    OperatorId id;

    explicit OperatorModel(OperatorType impl) : impl(std::move(impl)), id(getNextLogicalOperatorId()) { }

    OperatorModel(OperatorType impl, const OperatorId existingId) : impl(std::move(impl)), id(existingId) { }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return impl.explain(verbosity, id); }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return impl.getChildren(); }

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override { return impl.withChildren(children); }

    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const override { return impl.withTraitSet(traitSet); }

    [[nodiscard]] std::string_view getName() const noexcept override { return impl.getName(); }

    [[nodiscard]] Reflected reflect() const override { return NES::reflect(impl); }

    [[nodiscard]] TraitSet getTraitSet() const override { return impl.getTraitSet(); }

    [[nodiscard]] std::vector<LogicalSchema> getInputLogicalSchemas() const override
    {
        if constexpr (HasLogicalSchemaAPI<OperatorType>)
        {
            return impl.getInputLogicalSchemas();
        }
        else
        {
            std::vector<LogicalSchema> lifted;
            for (const auto& schema : impl.getInputSchemas())
            {
                lifted.push_back(LogicalSchema::fromSchema(schema));
            }
            return lifted;
        }
    }

    [[nodiscard]] LogicalSchema getOutputLogicalSchema() const override
    {
        if constexpr (HasLogicalSchemaAPI<OperatorType>)
        {
            return impl.getOutputLogicalSchema();
        }
        else
        {
            return LogicalSchema::fromSchema(impl.getOutputSchema());
        }
    }

    [[nodiscard]] LogicalOperator withInferredLogicalSchema(std::vector<LogicalSchema> inputSchemas) const override
    {
        if constexpr (HasLogicalSchemaAPI<OperatorType>)
        {
            return impl.withInferredLogicalSchema(std::move(inputSchemas));
        }
        else
        {
            std::vector<Schema> primitive;
            primitive.reserve(inputSchemas.size());
            for (const auto& logicalSchema : inputSchemas)
            {
                primitive.push_back(logicalSchema.toPrimitiveSchema());
            }
            return impl.withInferredSchema(std::move(primitive));
        }
    }

    [[nodiscard]] bool equals(const ErasedLogicalOperator& other) const override
    {
        if (auto ptr = dynamic_cast<const OperatorModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

    [[nodiscard]] OperatorId getOperatorId() const override { return id; }

    [[nodiscard]] LogicalOperator withOperatorId(OperatorId id) const override { return LogicalOperator{OperatorModel{impl, id}}; }

    [[nodiscard]] OperatorType get() const { return impl; }

    [[nodiscard]] const OperatorType& operator*() const { return impl; }

    [[nodiscard]] const OperatorType* operator->() const { return &impl; }

private:
    template <typename T>
    friend struct TypedLogicalOperator;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, OperatorType>)
        {
            return static_cast<const DynamicBase*>(&impl);
        }
        else
        {
            return std::nullopt;
        }
    }
};

}

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
