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
#include <concepts>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/DynamicBase.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <nameof.hpp>

namespace NES
{

namespace detail
{
struct ErasedPhysicalFunction;
}

template <typename Checked = NES::detail::ErasedPhysicalFunction>
struct TypedPhysicalFunction;
using PhysicalFunction = TypedPhysicalFunction<>;

/// Concept defining the interface for all physical functions in the query plan.
/// This concept defines the common interface that all physical functions must implement.
/// Physical functions represent functions in the query plan and are used during query
/// planning and optimization.
/// Concept defining the interface for all physical functions.
template <typename T>
concept PhysicalFunctionConcept = requires(const T& thisFunction, const Record& record, ArenaRef& arena) {
    /// Executes the function on the given record.
    /// @param record The record to evaluate the function on.
    /// @param arena The arena to allocate memory from.
    /// @return The result of the function evaluation.
    { thisFunction.execute(record, arena) } -> std::convertible_to<VarVal>;
};

namespace detail
{
/// @brief A type erased wrapper for physical functions
struct ErasedPhysicalFunction
{
    virtual ~ErasedPhysicalFunction() = default;

    [[nodiscard]] virtual VarVal execute(const Record& record, ArenaRef& arena) const = 0;

private:
    template <typename T>
    friend struct NES::TypedPhysicalFunction;
    ///If the function inherits from DynamicBase (over Castable), then returns a pointer to the wrapped operator as DynamicBase,
    ///so that we can then safely try to dyncast the DynamicBase* to Castable<T>*
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <PhysicalFunctionConcept PhysicalFunctionType>
struct PhysicalFunctionModel;
}

/// @brief A type erased wrapper for heap-allocated physical functions.
/// @tparam Checked A physical function type that this wrapper guarantees it contains.
/// Either a type erased virtual physical function class or the actual type.
/// You can cast with TypedPhysicalFunction::tryGetAs, to try to get a TypedPhysicalFunction for a specific type.
template <typename Checked>
struct TypedPhysicalFunction
{
    template <PhysicalFunctionConcept T>
    requires std::same_as<Checked, NES::detail::ErasedPhysicalFunction>
    TypedPhysicalFunction(TypedPhysicalFunction<T> other) : self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    /// Constructs a PhysicalFunction from a concrete function type.
    /// @tparam T The type of the function. Must satisfy PhysicalFunctionConcept concept.
    /// @param fn The function to wrap.
    template <PhysicalFunctionConcept T>
    TypedPhysicalFunction(const T& fn) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::PhysicalFunctionModel<T>>(fn))
    {
    }

    template <PhysicalFunctionConcept T>
    TypedPhysicalFunction(const NES::detail::PhysicalFunctionModel<T>& fn) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::PhysicalFunctionModel<T>>(fn.impl))
    {
    }

    explicit TypedPhysicalFunction(std::shared_ptr<const NES::detail::ErasedPhysicalFunction> fn) : self(std::move(fn)) { }

    TypedPhysicalFunction() = delete;

    ///@brief Alternative to operator*
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedPhysicalFunction, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedPhysicalFunction>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::PhysicalFunctionModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedPhysicalFunction, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedPhysicalFunction>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::PhysicalFunctionModel<Checked>>(self)->impl;
        }
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedPhysicalFunction, Checked>)
        {
            return std::dynamic_pointer_cast<const NES::detail::ErasedPhysicalFunction>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const NES::detail::PhysicalFunctionModel<Checked>>(self);
            return &casted->impl;
        }
    }

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<TypedPhysicalFunction<T>> The function if it is of type T, nullopt otherwise.
    template <PhysicalFunctionConcept T>
    std::optional<TypedPhysicalFunction<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::PhysicalFunctionModel<T>>(self))
        {
            return TypedPhysicalFunction<T>{std::static_pointer_cast<const NES::detail::ErasedPhysicalFunction>(model)};
        }
        return std::nullopt;
    }

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<std::shared_ptr<const Castable<T>>> The function if it is of type T and Castable<T>, nullopt otherwise.
    template <typename T>
    requires(!PhysicalFunctionConcept<T>)
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

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return TypedPhysicalFunction<T> The function.
    /// @throw InvalidDynamicCast If the function is not of type T.
    template <PhysicalFunctionConcept T>
    TypedPhysicalFunction<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::PhysicalFunctionModel<T>>(self))
        {
            return TypedPhysicalFunction<T>{std::static_pointer_cast<const NES::detail::ErasedPhysicalFunction>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
        std::unreachable();
    }

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return std::shared_ptr<const Castable<T>> The function.
    /// @throw InvalidDynamicCast If the function is not of type T or does not inherit from Castable<T>.
    template <typename T>
    requires(!PhysicalFunctionConcept<T>)
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

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const { return self->execute(record, arena); }

private:
    template <typename FriendChecked>
    friend struct TypedPhysicalFunction;

    std::shared_ptr<const NES::detail::ErasedPhysicalFunction> self;
};

namespace detail
{

/// @brief Wrapper type that acts as a bridge between a type satisfying PhysicalFunctionConcept and TypedPhysicalFunction
template <PhysicalFunctionConcept PhysicalFunctionType>
struct PhysicalFunctionModel : ErasedPhysicalFunction
{
    PhysicalFunctionType impl;

    explicit PhysicalFunctionModel(PhysicalFunctionType impl) : impl(std::move(impl)) { }

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override { return impl.execute(record, arena); }

    [[nodiscard]] PhysicalFunctionType get() const { return impl; }

private:
    template <typename T>
    friend struct TypedLogicalFunction;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, PhysicalFunctionType>)
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
