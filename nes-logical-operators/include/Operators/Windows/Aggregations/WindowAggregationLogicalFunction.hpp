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
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <nameof.hpp>

namespace NES
{
namespace detail
{
struct ErasedWindowAggregationFunction;
}

template <typename Checked = NES::detail::ErasedWindowAggregationFunction>
struct TypedWindowAggregationLogicalFunction;
using WindowAggregationLogicalFunction = TypedWindowAggregationLogicalFunction<>;

using AggregationFieldAccess
    = std::variant<TypedLogicalFunction<FieldAccessLogicalFunction>, TypedLogicalFunction<UnboundFieldAccessLogicalFunction>>;
/// Concept defining the interface for all logical operators in the query plan.
/// This concept defines the common interface that all logical operators must implement.
/// Logical operators represent operations in the query plan and are used during query
/// planning and optimization.
template <typename T>
concept WindowAggregationFunctionConcept = requires(
    const T& thisFunction,
    const Schema<Field, Unordered>& schema,
    DataType dataType,
    AggregationFieldAccess fieldAccessLogicalFunction,
    ExplainVerbosity verbosity,
    const T& rhs) {
    { thisFunction.getAggregateType() } -> std::convertible_to<DataType>;

    { thisFunction.getInputFunction() } -> std::convertible_to<AggregationFieldAccess>;

    { thisFunction.explain(verbosity) } -> std::convertible_to<std::string>;

    { thisFunction.withInferredType(schema) } -> std::convertible_to<T>;

    { thisFunction.shallIncludeNullValues() } noexcept -> std::convertible_to<bool>;

    { thisFunction.reflect() } -> std::convertible_to<Reflected>;

    { thisFunction.getName() } noexcept -> std::convertible_to<std::string_view>;

    { thisFunction == rhs } -> std::convertible_to<bool>;

    { std::hash<T>{}(thisFunction) } -> std::convertible_to<std::size_t>;
};

namespace detail
{
/// @brief A type erased wrapper for logical functions
struct ErasedWindowAggregationFunction
{
    virtual ~ErasedWindowAggregationFunction() = default;

    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;
    [[nodiscard]] virtual Reflected reflect() const = 0;
    [[nodiscard]] virtual size_t hash() const = 0;
    [[nodiscard]] virtual bool equals(const ErasedWindowAggregationFunction& other) const = 0;
    [[nodiscard]] virtual DataType getAggregateType() const = 0;
    [[nodiscard]] virtual AggregationFieldAccess getInputFunction() const = 0;

    [[nodiscard]] virtual bool shallIncludeNullValues() const noexcept = 0;
    [[nodiscard]] virtual WindowAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const = 0;

    friend bool operator==(const ErasedWindowAggregationFunction& lhs, const ErasedWindowAggregationFunction& rhs)
    {
        return lhs.equals(rhs);
    }

private:
    template <typename T>
    friend struct NES::TypedWindowAggregationLogicalFunction;
    ///If the function inherits from DynamicBase (over Castable), then returns a pointer to the wrapped operator as DynamicBase,
    ///so that we can then safely try to dyncast the DynamicBase* to Castable<T>*
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <WindowAggregationFunctionConcept T>
struct WindowAggregationFunctionModel;
}

/// @brief A type erased wrapper for heap-allocated logical functions.
/// @tparam Checked A logical function type that this wrapper guarantees it contains.
/// Either a type erased virtual logical function class or the actual type.
/// You can cast with TypedLogicalFunction::tryGetAs, to try to get a TypedLogicalFunction for a specific type.
template <typename Checked>
struct TypedWindowAggregationLogicalFunction
{
    template <WindowAggregationFunctionConcept T>
    requires std::same_as<Checked, NES::detail::ErasedWindowAggregationFunction>
    TypedWindowAggregationLogicalFunction(TypedWindowAggregationLogicalFunction<T> other) /// NOLINT(google-explicit-constructor)
        : self(other.self)
    {
    }

    /// Constructs a LogicalFunction from a concrete function type.
    /// @tparam T The type of the function. Must satisfy LogicalFunctionConcept concept.
    /// @param op The function to wrap.
    template <typename T>
    requires requires(const T& function) {
        { function.getAggregateType() } -> std::convertible_to<DataType>;
    }
    TypedWindowAggregationLogicalFunction(const T& op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::WindowAggregationFunctionModel<T>>(op))
    {
    }

    template <WindowAggregationFunctionConcept T>
    TypedWindowAggregationLogicalFunction(const NES::detail::WindowAggregationFunctionModel<T>& op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::WindowAggregationFunctionModel<T>>(op.impl))
    {
    }

    explicit TypedWindowAggregationLogicalFunction(std::shared_ptr<const NES::detail::ErasedWindowAggregationFunction> op)
        : self(std::move(op))
    {
    }

    TypedWindowAggregationLogicalFunction() = delete;

    ///@brief Alternative to operator*
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedWindowAggregationFunction, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedWindowAggregationFunction>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::WindowAggregationFunctionModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedWindowAggregationFunction, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedWindowAggregationFunction>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::WindowAggregationFunctionModel<Checked>>(self)->impl;
        }
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedWindowAggregationFunction, Checked>)
        {
            return std::dynamic_pointer_cast<const NES::detail::ErasedWindowAggregationFunction>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const NES::detail::WindowAggregationFunctionModel<Checked>>(self);
            return &casted->impl;
        }
    }

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<TypedLogicalFunction<T>> The function if it is of type T, nullopt otherwise.
    template <WindowAggregationFunctionConcept T>
    std::optional<TypedWindowAggregationLogicalFunction<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::WindowAggregationFunctionModel<T>>(self))
        {
            return TypedWindowAggregationLogicalFunction<T>{
                std::static_pointer_cast<const NES::detail::ErasedWindowAggregationFunction>(model)};
        }
        return std::nullopt;
    }

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<std::shared_ptr<const Castable<T>>> The function if it is of type T and Castable<T>, nullopt otherwise.
    template <typename T>
    requires(!WindowAggregationFunctionConcept<T>)
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
    /// @return TypedLogicalFunction<T> The function.
    /// @throw InvalidDynamicCast If the function is not of type T.
    template <WindowAggregationFunctionConcept T>
    TypedWindowAggregationLogicalFunction<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::WindowAggregationFunctionModel<T>>(self))
        {
            return TypedWindowAggregationLogicalFunction<T>{
                std::static_pointer_cast<const NES::detail::ErasedWindowAggregationFunction>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
        std::unreachable();
    }

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return std::shared_ptr<const Castable<T>> The function.
    /// @throw InvalidDynamicCast If the function is not of type T or does not inherit from Castable<T>.
    template <typename T>
    requires(!WindowAggregationFunctionConcept<T>)
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

    [[nodiscard]] std::string_view getName() const noexcept { return self->getName(); }

    [[nodiscard]] std::string explain(const ExplainVerbosity verbosity) const { return self->explain(verbosity); }

    [[nodiscard]] Reflected reflect() const { return self->reflect(); }

    [[nodiscard]] bool shallIncludeNullValues() const noexcept { return self->shallIncludeNullValues(); }

    [[nodiscard]] TypedWindowAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const
    {
        return self->withInferredType(schema);
    }

    [[nodiscard]] DataType getAggregateType() const { return self->getAggregateType(); }

    [[nodiscard]] AggregationFieldAccess getInputFunction() const { return self->getInputFunction(); }

    [[nodiscard]] bool operator==(const TypedWindowAggregationLogicalFunction& other) const { return self->equals(*other.self); }

private:
    template <typename FriendChecked>
    friend struct TypedWindowAggregationLogicalFunction;

    std::shared_ptr<const NES::detail::ErasedWindowAggregationFunction> self;
};

namespace detail
{

/// @brief Wrapper type that acts as a bridge between a type satisfying LogicalFunctionConcept and TypedLogicalFunction
template <WindowAggregationFunctionConcept FunctionType>
struct WindowAggregationFunctionModel : ErasedWindowAggregationFunction
{
    FunctionType impl;

    explicit WindowAggregationFunctionModel(FunctionType impl) : impl(std::move(impl)) { }

    [[nodiscard]] std::string_view getName() const noexcept override { return impl.getName(); }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return impl.explain(verbosity); }

    [[nodiscard]] Reflected reflect() const override { return impl.reflect(); }

    [[nodiscard]] bool shallIncludeNullValues() const noexcept override { return impl.shallIncludeNullValues(); }

    [[nodiscard]] size_t hash() const override { return std::hash<FunctionType>{}(impl); }

    [[nodiscard]] WindowAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const override
    {
        return impl.withInferredType(schema);
    }

    [[nodiscard]] DataType getAggregateType() const override { return impl.getAggregateType(); }

    [[nodiscard]] AggregationFieldAccess getInputFunction() const override { return impl.getInputFunction(); }

    [[nodiscard]] bool equals(const ErasedWindowAggregationFunction& other) const override
    {
        if (auto ptr = dynamic_cast<const WindowAggregationFunctionModel*>(&other))
        {
            return impl == ptr->impl;
        }
        return false;
    }

private:
    template <typename T>
    friend struct NES::TypedWindowAggregationLogicalFunction;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, FunctionType>)
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

TypedLogicalFunction<FieldAccessLogicalFunction> inferFieldAccess(AggregationFieldAccess field, const Schema<Field, Unordered>& schema);
}

template <NES::WindowAggregationFunctionConcept FunctionType>
struct std::hash<NES::TypedWindowAggregationLogicalFunction<FunctionType>> /// NOLINT(cert-dcl58-cpp)
{
    size_t operator()(const NES::TypedWindowAggregationLogicalFunction<FunctionType>& aggregationFunction) const noexcept
    {
        return std::hash<FunctionType>{}(aggregationFunction.get());
    }
};

template <>
struct std::hash<NES::WindowAggregationLogicalFunction>
{
    size_t operator()(const NES::WindowAggregationLogicalFunction& aggregationFunction) const noexcept
    {
        return aggregationFunction->hash();
    }
};
