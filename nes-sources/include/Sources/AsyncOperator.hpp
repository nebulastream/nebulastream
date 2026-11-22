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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/Reflection.hpp>
#include <boost/asio/awaitable.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
using AsyncArgumentsWithConstants = std::vector<std::pair<DataType, std::optional<RuntimeValue>>>;

namespace detail
{
struct ErasedAsyncOperator;
template <typename T>
struct AsyncOperatorModel;

template <typename T>
struct IsTypedAsyncOperator : std::false_type
{
};
}

template <typename T>
concept AsyncOperatorConcept = requires(
    const T& op,
    const AsyncArgumentsWithConstants& binding,
    AsyncExecutionContext& context,
    SharedRuntimeValues values,
    const ReflectionContext& reflection) {
    { op.getType() } -> std::convertible_to<std::string_view>;
    { op.getOutputSchema() } -> std::same_as<Schema<UnqualifiedUnboundField, Ordered>>;
    { op.withInferredArguments(binding) } -> std::same_as<T>;
    { op.prepare(context, values) } -> std::same_as<AsyncResult<std::unique_ptr<PreparedAsyncCall>>>;
    { reflection.reflect(op) } -> std::same_as<Reflected>;
};
template <typename Checked = detail::ErasedAsyncOperator>
struct TypedAsyncOperator;
using AsyncOperator = TypedAsyncOperator<>;
/// The owning operator handle and values are retained by the coroutine; explicit context must outlive it.
[[nodiscard]] boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>
invokeOnce(AsyncOperator, AsyncExecutionContext&, SharedRuntimeValues);
/// Owns a fresh context for the full invocation lifetime.
[[nodiscard]] boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> invokeOnce(AsyncOperator, SharedRuntimeValues);

namespace detail
{
struct ErasedAsyncOperator
{
    virtual ~ErasedAsyncOperator() = default;
    [[nodiscard]] virtual std::string getType() const = 0;
    [[nodiscard]] virtual Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const = 0;
    [[nodiscard]] virtual AsyncOperator withInferredArguments(const AsyncArgumentsWithConstants&) const = 0;
    [[nodiscard]] virtual AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext&, SharedRuntimeValues) const = 0;
    [[nodiscard]] virtual std::optional<size_t> maxConcurrency() const = 0;
    [[nodiscard]] virtual Reflected reflect(const ReflectionContext&) const = 0;
};

template <typename T>
struct IsTypedAsyncOperator<TypedAsyncOperator<T>> : std::true_type
{
};

}

/// Immutable owning configuration; copies share the const model, never execution state.
/// getAs<T>() requires a matching concrete type; tryGetAs<T>() probes it.
/// Moved-from wrappers may only be destroyed or reassigned.
template <typename Checked>
struct TypedAsyncOperator
{
    template <typename T>
    requires(
        !detail::IsTypedAsyncOperator<T>::value
        && AsyncOperatorConcept<T> && (std::same_as<Checked, detail::ErasedAsyncOperator> || std::same_as<Checked, T>))
    TypedAsyncOperator(T op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<const detail::AsyncOperatorModel<T>>(std::move(op)))
    {
    }

    template <typename T>
    requires(AsyncOperatorConcept<T> && std::same_as<Checked, detail::ErasedAsyncOperator>)
    TypedAsyncOperator(TypedAsyncOperator<T> op) /// NOLINT(google-explicit-constructor)
        : self(std::move(op.self))
    {
        PRECONDITION(self != nullptr, "Cannot access a moved-from AsyncOperator");
    }

    [[nodiscard]] const Checked& get() const
    {
        const auto& erased = getErased();
        if constexpr (std::same_as<Checked, detail::ErasedAsyncOperator>)
        {
            return erased;
        }
        else
        {
            return static_cast<const detail::AsyncOperatorModel<Checked>&>(erased).impl;
        }
    }

    [[nodiscard]] const Checked& operator*() const { return get(); }

    [[nodiscard]] const Checked* operator->() const { return &get(); }

    template <AsyncOperatorConcept T>
    [[nodiscard]] std::optional<TypedAsyncOperator<T>> tryGetAs() const
    {
        PRECONDITION(self != nullptr, "Cannot access a moved-from AsyncOperator");
        if (std::dynamic_pointer_cast<const detail::AsyncOperatorModel<T>>(self))
        {
            return TypedAsyncOperator<T>{self};
        }
        return std::nullopt;
    }

    template <AsyncOperatorConcept T>
    [[nodiscard]] TypedAsyncOperator<T> getAs() const
    {
        auto result = tryGetAs<T>();
        PRECONDITION(result.has_value(), "Requested async type does not match {}", getType());
        return *result;
    }

    [[nodiscard]] std::string getType() const { return getErased().getType(); }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const { return getErased().getOutputSchema(); }

    [[nodiscard]] TypedAsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const
    {
        auto result = getErased().withInferredArguments(binding);
        if constexpr (std::same_as<Checked, detail::ErasedAsyncOperator>)
        {
            return result;
        }
        else
        {
            return result.template getAs<Checked>();
        }
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, SharedRuntimeValues values) const
    {
        return getErased().prepare(context, std::move(values));
    }

    [[nodiscard]] std::optional<size_t> maxConcurrency() const { return getErased().maxConcurrency(); }

    [[nodiscard]] Reflected reflect(const ReflectionContext& context) const { return getErased().reflect(context); }

private:
    template <typename>
    friend struct TypedAsyncOperator;

    [[nodiscard]] const detail::ErasedAsyncOperator& getErased() const
    {
        PRECONDITION(self != nullptr, "Cannot access a moved-from AsyncOperator");
        return *self;
    }

    explicit TypedAsyncOperator(std::shared_ptr<const detail::ErasedAsyncOperator> value) : self(std::move(value)) { }

    std::shared_ptr<const detail::ErasedAsyncOperator> self;
};

namespace detail
{
template <typename T>
struct AsyncOperatorModel final : ErasedAsyncOperator
{
    T impl;

    explicit AsyncOperatorModel(T value) : impl(std::move(value)) { }

    [[nodiscard]] std::string getType() const override { return std::string{impl.getType()}; }

    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const override { return impl.getOutputSchema(); }

    [[nodiscard]] AsyncOperator withInferredArguments(const AsyncArgumentsWithConstants& binding) const override
    {
        return impl.withInferredArguments(binding);
    }

    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>>
    prepare(AsyncExecutionContext& context, SharedRuntimeValues values) const override
    {
        return impl.prepare(context, std::move(values));
    }

    [[nodiscard]] std::optional<size_t> maxConcurrency() const override
    {
        if constexpr (requires { impl.maxConcurrency(); })
        {
            static_assert(std::same_as<decltype(impl.maxConcurrency()), std::optional<size_t>>);
            return impl.maxConcurrency();
        }
        else
        {
            return std::nullopt;
        }
    }

    [[nodiscard]] Reflected reflect(const ReflectionContext& context) const override { return context.reflect(impl); }
};
}
}
