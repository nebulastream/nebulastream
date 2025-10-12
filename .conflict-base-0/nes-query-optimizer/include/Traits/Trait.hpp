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

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_set>
#include <utility>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES
{

namespace detail
{
struct ErasedTrait;
}

template <typename Checked = NES::detail::ErasedTrait>
struct TypedTrait;
using Trait = TypedTrait<>;

/// Concept defining the interface for all traits in the query optimizer.
/// Traits are used to annotate logical operators with additional properties
/// that can be used during query optimization.
template <typename T>
concept TraitConcept = requires(const T& thisTrait, ExplainVerbosity verbosity, const T& rhs) {
    /// Returns the type information of this trait.
    /// @return const std::type_info& The type information of this trait.
    { thisTrait.getType() } -> std::convertible_to<const std::type_info&>;
    { thisTrait.getName() } -> std::convertible_to<std::string_view>;

    /// Serialize the function to a Reflected object
    { NES::reflect(thisTrait) } -> std::same_as<Reflected>;

    /// Returns a string representation of the function
    { thisTrait.explain(verbosity) } -> std::convertible_to<std::string>;

    /// Computes the hash value for this trait.
    /// @return size_t The hash value.
    { thisTrait.hash() } -> std::convertible_to<size_t>;

    /// Compares this trait with another trait for equality.
    /// @param other The trait to compare with.
    /// @return bool True if the traits are equal, false otherwise.
    { thisTrait == rhs } -> std::convertible_to<bool>;
};

namespace detail
{
/// @brief A type erased wrapper for traits
struct ErasedTrait
{
    virtual ~ErasedTrait() = default;

    [[nodiscard]] virtual const std::type_info& getType() const = 0;
    [[nodiscard]] virtual std::string_view getName() const = 0;
    [[nodiscard]] virtual Reflected reflect() const = 0;
    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;
    [[nodiscard]] virtual size_t hash() const = 0;
    [[nodiscard]] virtual bool equals(const ErasedTrait& other) const = 0;

    friend bool operator==(const ErasedTrait& lhs, const ErasedTrait& rhs) { return lhs.equals(rhs); }

private:
    template <typename T>
    friend struct NES::TypedTrait;
    ///If the trait inherits from DynamicBase (over Castable), then returns a pointer to the wrapped trait as DynamicBase,
    ///so that we can then safely try to dyncast the DynamicBase* to Castable<T>*
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <TraitConcept TraitType>
struct TraitModel;
}

/// Base class providing default implementations for traits without custom data
template <typename Derived>
struct DefaultTrait
{
    bool operator==(const DefaultTrait& other) const { return typeid(other) == typeid(Derived); }

    [[nodiscard]] size_t hash() const { return std::type_index(typeid(Derived)).hash_code(); }

    [[nodiscard]] const std::type_info& getType() const { return typeid(Derived); }

    [[nodiscard]] std::string explain(ExplainVerbosity) const { return "DefaultTrait"; }

    [[nodiscard]] virtual std::string_view getName() const = 0;

    friend Derived;

private:
    DefaultTrait() = default;
};

/// @brief A type erased wrapper for heap-allocated traits.
/// @tparam Checked A trait type that this wrapper guarantees it contains.
/// Either a type erased virtual trait class or the actual type.
/// You can cast with TypedTrait::tryGetAs, to try to get a TypedTrait for a specific type.
template <typename Checked>
struct TypedTrait
{
    template <TraitConcept T>
    requires std::same_as<Checked, NES::detail::ErasedTrait>
    TypedTrait(TypedTrait<T> other) : self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    /// Constructs a Trait from a concrete trait type.
    /// @tparam T The type of the trait. Must satisfy TraitConcept concept.
    /// @param op The trait to wrap.
    template <typename T>
    requires requires(const T& trait) {
        { trait.getName() } -> std::convertible_to<std::string_view>;
    }
    TypedTrait(const T& op) : self(std::make_shared<NES::detail::TraitModel<T>>(op)) /// NOLINT(google-explicit-constructor)
    {
    }

    template <TraitConcept T>
    TypedTrait(const NES::detail::TraitModel<T>& op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::TraitModel<T>>(op.impl))
    {
    }

    explicit TypedTrait(std::shared_ptr<const NES::detail::ErasedTrait> op) : self(std::move(op)) { }

    TypedTrait() = delete;

    ///@brief Alternative to operator*
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedTrait, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedTrait>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::TraitModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedTrait, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedTrait>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::TraitModel<Checked>>(self)->impl;
        }
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedTrait, Checked>)
        {
            return std::dynamic_pointer_cast<const NES::detail::ErasedTrait>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const NES::detail::TraitModel<Checked>>(self);
            return &casted->impl;
        }
    }

    /// Attempts to get the underlying trait as type T.
    /// @tparam T The type to try to get the trait as.
    /// @return std::optional<TypedTrait<T>> The trait if it is of type T, nullopt otherwise.
    template <TraitConcept T>
    std::optional<TypedTrait<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::TraitModel<T>>(self))
        {
            return TypedTrait<T>{std::static_pointer_cast<const NES::detail::ErasedTrait>(model)};
        }
        return std::nullopt;
    }

    /// Attempts to get the underlying trait as type T.
    /// @tparam T The type to try to get the trait as.
    /// @return std::optional<std::shared_ptr<const Castable<T>>> The function if it is of type T and Castable<T>, nullopt otherwise.
    template <typename T>
    requires(!TraitConcept<T>)
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

    /// Gets the underlying trait as type T.
    /// @tparam T The type to get the trait as.
    /// @return TypedTrait<T> The trait.
    /// @throw InvalidDynamicCast If the trait is not of type T.
    template <TraitConcept T>
    TypedTrait<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::TraitModel<T>>(self))
        {
            return TypedTrait<T>{std::static_pointer_cast<const NES::detail::ErasedTrait>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
        std::unreachable();
    }

    /// Gets the underlying trait as type T.
    /// @tparam T The type to get the trait as.
    /// @return std::shared_ptr<const Castable<T>> The trait.
    /// @throw InvalidDynamicCast If the trait is not of type T or does not inherit from Castable<T>.
    template <typename T>
    requires(!TraitConcept<T>)
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

    [[nodiscard]] const std::type_info& getTypeInfo() const { return self->getType(); };

    [[nodiscard]] std::string_view getName() const { return self->getName(); };

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const { return self->explain(verbosity); };

    [[nodiscard]] size_t hash() const { return self->hash(); }

    bool operator==(const Trait& other) const { return self->equals(*other.self); }

private:
    template <typename FriendChecked>
    friend struct TypedTrait;

    std::shared_ptr<const NES::detail::ErasedTrait> self;
};

namespace detail
{
/// @brief Wrapper type that acts as a bridge between a type satisfying TraitConcept and TypedTrait
template <TraitConcept TraitType>
struct TraitModel : ErasedTrait
{
    TraitType impl;

    explicit TraitModel(TraitType impl) : impl(std::move(impl)) { }

    bool operator==(const Trait& other) const
    {
        if (auto ptr = dynamic_cast<const TraitModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

    [[nodiscard]] bool equals(const ErasedTrait& other) const override
    {
        if (auto ptr = dynamic_cast<const TraitModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return impl.explain(verbosity); }

    [[nodiscard]] size_t hash() const override { return impl.hash(); }

    [[nodiscard]] const std::type_info& getType() const override { return impl.getType(); }

    [[nodiscard]] std::string_view getName() const override { return impl.getName(); }

    [[nodiscard]] Reflected reflect() const override { return NES::reflect(impl); }

private:
    template <typename T>
    friend struct TypedTrait;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, TraitType>)
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
}

template <>
struct std::hash<NES::Trait>
{
    size_t operator()(const NES::Trait& trait) const noexcept { return trait.hash(); }
};
