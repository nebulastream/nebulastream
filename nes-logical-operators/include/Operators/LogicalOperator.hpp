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

struct ErasedLogicalOperator;
template <typename Checked = ErasedLogicalOperator>
struct LogicalOperatorBase;

using LogicalOperator = LogicalOperatorBase<>;
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
    std::vector<Schema> inputSchemas,
    std::vector<std::vector<OriginId>> inputOriginIds,
    std::vector<OriginId> outputOriginIds) {
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

    /// Returns the input origin IDs of the operator
    { thisOperator.getInputOriginIds() } -> std::convertible_to<std::vector<std::vector<OriginId>>>;

    /// Returns the output origin IDs of the operator
    { thisOperator.getOutputOriginIds() } -> std::convertible_to<std::vector<OriginId>>;

    /// Creates a new operator with the given input origin IDs
    { thisOperator.withInputOriginIds(inputOriginIds) } -> std::convertible_to<T>;

    /// Creates a new operator with the given output origin IDs
    { thisOperator.withOutputOriginIds(outputOriginIds) } -> std::convertible_to<T>;

    /// Creates a new operator with inferred schema based on input schemas
    { thisOperator.withInferredSchema(inputSchemas) } -> std::convertible_to<T>;
};

using LogicalOperator = LogicalOperatorBase<>;

struct DynamicBase
{
    virtual ~DynamicBase() = default;
};

template <typename T>
struct Castable : DynamicBase
{
    const T& get() const { return *dynamic_cast<const T*>(this); }

    const T* operator->() const { return dynamic_cast<const T*>(this); }

    const T& operator*() const { return *dynamic_cast<const T*>(this); }
};

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
    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;
    [[nodiscard]] virtual LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const = 0;
    [[nodiscard]] virtual LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const = 0;
    [[nodiscard]] virtual LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const = 0;
    [[nodiscard]] virtual bool equals(const ErasedLogicalOperator& other) const = 0;
    [[nodiscard]] virtual OperatorId getOperatorId() const = 0;
    [[nodiscard]] virtual LogicalOperator withOperatorId(OperatorId id) const = 0;

    friend bool operator==(const ErasedLogicalOperator& lhs, const ErasedLogicalOperator& rhs) { return lhs.equals(rhs); }


private:
    template <typename T>
    friend struct LogicalOperatorBase;
    /// Use std::any because we can't cast void pointer up safely
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

/// A type-erased wrapper for logical operators.
/// C.f.: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
/// This class provides type erasure for logical operators, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual operator implementation.
/// @tparam T The type of the logical operator. Must inherit from LogicalOperatorConcept.

template <LogicalOperatorConcept OperatorType>
struct OperatorModel;

/// Id is preserved during copy
template <typename Checked>
struct LogicalOperatorBase
{
    template <LogicalOperatorConcept T>
    requires std::same_as<Checked, ErasedLogicalOperator>
    LogicalOperatorBase(LogicalOperatorBase<T> other) : self(other.self) /// NOLINT
    {
    }

    /// Constructs a LogicalOperator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsLogicalOperator concept.
    /// @param op The operator to wrap.
    template <LogicalOperatorConcept T>
    LogicalOperatorBase(const T& op) : self(std::make_shared<OperatorModel<T>>(std::move(op))) /// NOLINT
    {
    }

    template <LogicalOperatorConcept T>
    LogicalOperatorBase(const OperatorModel<T>& op) : self(std::make_shared<OperatorModel<T>>(std::move(op.impl), op.id)) /// NOLINT
    {
    }

    explicit LogicalOperatorBase(std::shared_ptr<const ErasedLogicalOperator> op) : self(std::move(op)) { }

    [[nodiscard]] const Checked& get() const { return std::dynamic_pointer_cast<const OperatorModel<Checked>>(self)->impl; }

    const Checked& operator*() const
    {
        if constexpr (std::is_same_v<ErasedLogicalOperator, Checked>)
        {
            return *std::dynamic_pointer_cast<const ErasedLogicalOperator>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const OperatorModel<Checked>>(self)->impl;
        }
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<ErasedLogicalOperator, Checked>)
        {
            return std::dynamic_pointer_cast<const ErasedLogicalOperator>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const OperatorModel<Checked>>(self);
            return &casted->impl;
        }
    }

    LogicalOperatorBase() = default;

    /// Attempts to get the underlying operator as type T.
    /// @tparam T The type to try to get the operator as.
    /// @return std::optional<T> The operator if it is of type T, nullopt otherwise.
    template <LogicalOperatorConcept T>
    std::optional<LogicalOperatorBase<T>> tryGet() const
    {
        if (auto model = std::dynamic_pointer_cast<const OperatorModel<T>>(self))
        {
            return LogicalOperatorBase<T>{std::static_pointer_cast<const ErasedLogicalOperator>(model)};
        }
        return std::nullopt;
    }

    template <typename T>
    requires(!LogicalOperatorConcept<T>)
    std::optional<std::shared_ptr<const Castable<T>>> tryGet() const
    {
        auto castable = self->getImpl();
        if (castable.has_value())
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
    /// @return const T The operator.
    /// @throw InvalidDynamicCast If the operator is not of type T.
    template <LogicalOperatorConcept T>
    LogicalOperatorBase<T> get() const
    {
        if (auto model = std::dynamic_pointer_cast<const OperatorModel<T>>(self))
        {
            return LogicalOperatorBase<T>{std::static_pointer_cast<const ErasedLogicalOperator>(model)};
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    template <typename T>
    requires(!LogicalOperatorConcept<T>)
    std::shared_ptr<const T> get() const
    {
        auto castable = self->getImpl();
        if (castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const T>{self, ptr};
            }
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    [[nodiscard]] std::string explain(const ExplainVerbosity verbosity) const { return self->explain(verbosity); }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return self->getChildren(); };

    [[nodiscard]] LogicalOperatorBase withChildren(std::vector<LogicalOperator> children) const
    {
        return self->withChildren(std::move(children));
    }

    /// Static traits defined as member variables will be present in the new operator nonetheless
    [[nodiscard]] LogicalOperatorBase withTraitSet(TraitSet traitSet) const { return self->withTraitSet(std::move(traitSet)); }

    [[nodiscard]] OperatorId getId() const { return self->getOperatorId(); }

    [[nodiscard]] bool operator==(const LogicalOperator& other) const { return self->equals(*other.self); }

    [[nodiscard]] std::string_view getName() const noexcept { return self->getName(); }

    void serialize(SerializableOperator& serializableOperator) const { self->serialize(serializableOperator); }

    [[nodiscard]] TraitSet getTraitSet() const { return self->getTraitSet(); }

    [[nodiscard]] std::vector<Schema> getInputSchemas() const { return self->getInputSchemas(); }

    [[nodiscard]] Schema getOutputSchema() const { return self->getOutputSchema(); }

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const { return self->getInputOriginIds(); }

    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const { return self->getOutputOriginIds(); }

    [[nodiscard]] LogicalOperatorBase withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
    {
        return self->withInputOriginIds(std::move(ids));
    }

    [[nodiscard]] LogicalOperatorBase withOutputOriginIds(std::vector<OriginId> ids) const
    {
        return self->withOutputOriginIds(std::move(ids));
    }

    [[nodiscard]] LogicalOperatorBase withInferredSchema(std::vector<Schema> inputSchemas) const
    {
        return self->withInferredSchema(std::move(inputSchemas));
    }

private:
    friend class QueryPlanSerializationUtil;
    friend class OperatorSerializationUtil;

    template <typename FriendChecked>
    friend struct LogicalOperatorBase;

    [[nodiscard]] LogicalOperatorBase withOperatorId(const OperatorId id) const { return self->withOperatorId(id); };

    std::shared_ptr<const ErasedLogicalOperator> self;
};

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

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override { return impl.getInputOriginIds(); }

    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override { return impl.getOutputOriginIds(); }

    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override
    {
        return impl.withInputOriginIds(ids);
    }

    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override { return impl.withOutputOriginIds(ids); }

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
    friend struct LogicalOperatorBase;

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
