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
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
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

namespace detail
{
struct ErasedLogicalOperator;
}

template <typename Checked = NES::detail::ErasedLogicalOperator>
struct TypedLogicalOperator;
using LogicalOperator = TypedLogicalOperator<>;

/// Concept defining the interface for all logical operators in the query plan.
/// This concept defines the common interface that all logical operators must implement.
/// Logical operators represent operations in the query plan and are used during query
/// planning and optimization.
template <typename T>
concept LogicalOperatorConcept = requires(
    const T& thisOperator,
    ExplainVerbosity verbosity,
    OperatorId operatorId,
    std::vector<LogicalOperator> children,
    TraitSet traitSet,
    const T& rhs,
    SerializableOperator& serializableOperator,
    std::vector<Schema> inputSchemas) {
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

    /// Serializes the operator to a protobuf message
    thisOperator.serialize(serializableOperator);

    /// Returns the trait set of the operator
    { thisOperator.getTraitSet() } -> std::convertible_to<TraitSet>;

    /// Returns the input schemas of the operator
    { thisOperator.getInputSchemas() } -> std::convertible_to<std::vector<Schema>>;

    /// Returns the output schema of the operator
    { thisOperator.getOutputSchema() } -> std::convertible_to<Schema>;

    /// Creates a new operator with inferred schema based on input schemas
    { thisOperator.withInferredSchema(inputSchemas) } -> std::convertible_to<T>;
};

namespace detail
{
struct DynamicBase
{
    virtual ~DynamicBase() = default;
};
}

/// If your operator inherits from some type T,
/// you will also need to inherit from Castable<T> if you want to be able to cast with <tt>TypedLogicalOperator<Checked>::getAs<T>()</tt>.
/// If you control T, you can also use it as a CRTP and inherit it directly @code class T : Castable<T> @endcode
/// This class is only there to add a commonly accessible VTable so that we can cast the pointer to the operator.
/// @tparam T the type to be able to cast to from LogicalOperator
template <typename T>
struct Castable : NES::detail::DynamicBase
{
    const T& get() const { return *dynamic_cast<const T*>(this); }

    const T* operator->() const { return dynamic_cast<const T*>(this); }

    const T& operator*() const { return *dynamic_cast<const T*>(this); }

private:
    Castable() = default;
    friend T;
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
    virtual void serialize(SerializableOperator& sOp) const = 0;
    [[nodiscard]] virtual TraitSet getTraitSet() const = 0;
    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;
    [[nodiscard]] virtual LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const = 0;
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
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
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
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
        std::unreachable();
    }

    [[nodiscard]] std::string explain(const ExplainVerbosity verbosity) const { return self->explain(verbosity); }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return self->getChildren(); };

    [[nodiscard]] TypedLogicalOperator withChildren(std::vector<LogicalOperator> children) const
    {
        return self->withChildren(std::move(children));
    }

    /// Static traits defined as member variables will be present in the new operator nonetheless
    [[nodiscard]] TypedLogicalOperator withTraitSet(TraitSet traitSet) const { return self->withTraitSet(std::move(traitSet)); }

    [[nodiscard]] OperatorId getId() const { return self->getOperatorId(); }

    [[nodiscard]] bool operator==(const LogicalOperator& other) const { return self->equals(*other.self); }

    [[nodiscard]] std::string_view getName() const noexcept { return self->getName(); }

    void serialize(SerializableOperator& serializableOperator) const { self->serialize(serializableOperator); }

    [[nodiscard]] TraitSet getTraitSet() const { return self->getTraitSet(); }

    [[nodiscard]] std::vector<Schema> getInputSchemas() const { return self->getInputSchemas(); }

    [[nodiscard]] Schema getOutputSchema() const { return self->getOutputSchema(); }

    [[nodiscard]] TypedLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const
    {
        return self->withInferredSchema(std::move(inputSchemas));
    }

private:
    friend class QueryPlanSerializationUtil;
    friend class OperatorSerializationUtil;

    template <typename FriendChecked>
    friend struct TypedLogicalOperator;

    [[nodiscard]] TypedLogicalOperator withOperatorId(const OperatorId id) const { return self->withOperatorId(id); };

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

    void serialize(SerializableOperator& sOp) const override
    {
        impl.serialize(sOp);
        PRECONDITION(sOp.operator_id() == OperatorId::INVALID, "Operator id should not be serialized in operator implementation");
        sOp.set_operator_id(id.getRawValue());
    }

    [[nodiscard]] TraitSet getTraitSet() const override { return impl.getTraitSet(); }

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override { return impl.getInputSchemas(); }

    [[nodiscard]] Schema getOutputSchema() const override { return impl.getOutputSchema(); }

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override
    {
        return impl.withInferredSchema(inputSchemas);
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

/// Adds additional traits to the given operator, returning a new operator
/// If the same trait (with the same data) is already present, the new trait will not be added.
template <IsTrait... TraitType>
LogicalOperator addAdditionalTraits(const LogicalOperator& op, const TraitType&... traits)
{
    auto result = op.getTraitSet();
    (result.tryInsert(traits), ...);
    return op.withTraitSet(std::move(result));
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
