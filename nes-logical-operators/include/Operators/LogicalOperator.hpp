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
#include <Operators/LogicalOperatorFwd.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/Synchronized.h>
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
/// @brief A type erased wrapper for logical operators
struct ErasedLogicalOperator : std::enable_shared_from_this<ErasedLogicalOperator>
{
    virtual ~ErasedLogicalOperator() = default;

    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;
    [[nodiscard]] virtual std::vector<LogicalOperator> getChildren() const = 0;
    [[nodiscard]] virtual LogicalOperator withChildren(std::vector<LogicalOperator> children) const = 0;
    [[nodiscard]] virtual LogicalOperator withTraitSet(TraitSet traitSet) const = 0;
    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual Reflected reflect() const = 0;
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
    template <typename T>
    requires(std::is_same_v<T, detail::ErasedLogicalOperator> || LogicalOperatorConcept<T>)
    friend struct NES::WeakTypedLogicalOperator;
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

    [[nodiscard]] TypedLogicalOperator withChildren(std::vector<LogicalOperator> children) const
    {
        return self->withChildren(std::move(children));
    }

    /// Static traits defined as member variables will be present in the new operator nonetheless
    [[nodiscard]] TypedLogicalOperator withTraitSet(TraitSet traitSet) const { return self->withTraitSet(std::move(traitSet)); }

    [[nodiscard]] OperatorId getId() const { return self->getOperatorId(); }

    [[nodiscard]] bool operator==(const LogicalOperator& other) const
    {
        if (self == other.self)
        {
            return true;
        }
        return self->equals(*other.self);
    }

    [[nodiscard]] std::string_view getName() const noexcept { return self->getName(); }

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

    template <typename WeakChecked>
    requires(std::is_same_v<WeakChecked, detail::ErasedLogicalOperator> || LogicalOperatorConcept<WeakChecked>)
    friend struct WeakTypedLogicalOperator;

    [[nodiscard]] TypedLogicalOperator withOperatorId(const OperatorId id) const { return self->withOperatorId(id); };

    std::shared_ptr<const NES::detail::ErasedLogicalOperator> self;
};

namespace detail
{
template <typename Checked>
struct SelfRef
{
    folly::Synchronized<std::optional<OperatorModel<Checked>*>> self;

    explicit SelfRef(OperatorModel<Checked>* ptr) : self(ptr) { }
};
}

template <typename Checked>
requires(std::is_same_v<Checked, detail::ErasedLogicalOperator> || LogicalOperatorConcept<Checked>)
struct WeakTypedLogicalOperator
{
private:
    using PtrType = std::conditional_t<
        std::is_same_v<Checked, detail::ErasedLogicalOperator>,
        std::weak_ptr<const detail::ErasedLogicalOperator>,
        std::shared_ptr<detail::SelfRef<Checked>>>;

public:
    WeakTypedLogicalOperator() = default;

    explicit WeakTypedLogicalOperator(const LogicalOperator& owningOperator) : self(owningOperator.self) { }

    explicit WeakTypedLogicalOperator(PtrType weakPtr) : self(std::move(weakPtr)) { }

    template <typename OtherChecked>
    requires(std::is_same_v<Checked, detail::ErasedLogicalOperator> && LogicalOperatorConcept<OtherChecked>)
    explicit WeakTypedLogicalOperator(const std::shared_ptr<detail::SelfRef<OtherChecked>>& other)
        : self(std::shared_ptr<detail::ErasedLogicalOperator>{
              other, static_cast<detail::ErasedLogicalOperator*>(other->self.rlock()->value())})
    {
    }

    std::optional<TypedLogicalOperator<Checked>> tryLock() const
    {
        if constexpr (std::is_same_v<Checked, detail::ErasedLogicalOperator>)
        {
            if (auto ptr = self.lock())
            {
                return TypedLogicalOperator<Checked>{ptr->shared_from_this()};
            }
            return std::nullopt;
        }
        else if constexpr (LogicalOperatorConcept<Checked>)
        {
            if (const auto ptrOpt = self->self.rlock(); ptrOpt->has_value())
            {
                return TypedLogicalOperator<Checked>{ptrOpt->value()->shared_from_this()};
            }
            return std::nullopt;
        }
        else
        {
            static_assert(false, "Non exhaustive tryLock for WeakOperators");
            std::unreachable();
        }
    }

    TypedLogicalOperator<Checked> lock() const
    {
        if constexpr (std::is_same_v<Checked, detail::ErasedLogicalOperator>)
        {
            auto count = self.use_count();
            auto ptr = self.lock();
            if (ptr)
            {
                return TypedLogicalOperator<Checked>{ptr->shared_from_this()};
            }
            PRECONDITION(false, "Operator has been destroyed");
            std::unreachable();
        }
        else if constexpr (LogicalOperatorConcept<Checked>)
        {
            if (const auto ptrOpt = self->self.rlock(); ptrOpt->has_value())
            {
                return TypedLogicalOperator<Checked>{ptrOpt->value()->shared_from_this()};
            }
            PRECONDITION(false, "Operator has been destroyed");
            std::unreachable();
        }
        else
        {
            static_assert(false, "Non exhaustive lock for WeakOperators");
            std::unreachable();
        }
    }

private:
    PtrType self;
};

namespace detail
{

/// @brief Wrapper type that acts as a bridge between a type satisfying LogicalOperatorConcept and TypedLogicalOperator
template <LogicalOperatorConcept OperatorType>
struct OperatorModel : ErasedLogicalOperator
{
    OperatorId id;
    std::shared_ptr<SelfRef<OperatorType>> selfRef;
    OperatorType impl;

    explicit OperatorModel(OperatorType impl)
        : id(getNextLogicalOperatorId()), selfRef(std::make_shared<SelfRef<OperatorType>>(this)), impl(std::move(impl))
    {
        this->impl.self = WeakLogicalOperator{selfRef};
    }

    OperatorModel(OperatorType impl, const OperatorId existingId)
        : id(existingId), selfRef(std::make_shared<SelfRef<OperatorType>>(this)), impl(std::move(impl))
    {
        this->impl.self = WeakLogicalOperator{selfRef};
    }

    OperatorModel(const OperatorModel& other) : id(other.id), selfRef(std::make_shared<SelfRef<OperatorType>>(this)), impl(other.impl)
    {
        impl.self = WeakLogicalOperator{selfRef};
    }

    OperatorModel(OperatorModel&& other) noexcept = delete;

    OperatorModel& operator=(const OperatorModel& other) = delete;

    ~OperatorModel() override { selfRef->self = std::nullopt; }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return impl.explain(verbosity, id); }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return impl.getChildren(); }

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override { return impl.withChildren(children); }

    [[nodiscard]] LogicalOperator withTraitSet(TraitSet traitSet) const override { return impl.withTraitSet(traitSet); }

    [[nodiscard]] std::string_view getName() const noexcept override { return impl.getName(); }

    [[nodiscard]] Reflected reflect() const override { return NES::reflect(impl); }

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
            if constexpr (requires() { static_cast<const DynamicBase*>(&impl); })
            {
                return static_cast<const DynamicBase*>(&impl);
            }
            else if constexpr (requires() {
                                   { impl.getDynamicBase() } -> std::same_as<const DynamicBase*>;
                               })
            {
                return impl.getDynamicBase();
            }
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
