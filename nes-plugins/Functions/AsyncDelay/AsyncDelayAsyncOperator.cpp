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
#include <AsyncDelayAsyncOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <DataTypes/CheckedInteger.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/CheckedDeadline.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; warning is a false unused-header attribution.
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
void validateArity(const size_t argumentCount)
{
    if (argumentCount < 2 || argumentCount > 3)
    {
        throw InvalidQuerySyntax("ASYNC_DELAY takes (value, millis[, max_concurrency])");
    }
}

struct ReflectedDelayConfiguration
{
    DataType valueType;
    std::optional<size_t> concurrencyLimit;
    AsyncInputAccessors inputs;
};

class PreparedDelayCall final : public PreparedAsyncCall
{
public:
    PreparedDelayCall(RuntimeValue value, uint64_t millis) : value(std::move(value)), millis(millis) { }

    boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
    {
        const auto deadline = checkedSteadyDeadline(millis, "ASYNC_DELAY");
        if (!deadline)
        {
            co_return std::unexpected(AsyncError{.exception = deadline.error(), .kind = AsyncError::Type::Permanent});
        }
        const bool previousThrowPolicy = co_await boost::asio::this_coro::throw_if_cancelled();
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, *deadline};
        boost::system::error_code error; /// NOLINT(misc-include-cleaner) Declared by the public boost/system/error_code.hpp provider.
        co_await timer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
        co_await boost::asio::this_coro::throw_if_cancelled(previousThrowPolicy);
        if (error == boost::asio::error::operation_aborted)
        {
            co_return std::unexpected(AsyncError::cancelled());
        }
        if (error)
        {
            co_return std::unexpected(
                AsyncError{.exception = IOError("ASYNC_DELAY timer failed: {}", error.message()), .kind = AsyncError::Type::Permanent});
        }
        co_return SharedRuntimeValues{value};
    }

private:
    RuntimeValue value;
    uint64_t millis;
};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view AsyncDelayAsyncOperator::getType() const
{
    return "ASYNC_DELAY";
}

Schema<UnqualifiedUnboundField, Ordered> AsyncDelayAsyncOperator::getOutputSchema() const
{
    return {{Identifier::parse("value"), valueType}};
}

/// NOLINTNEXTLINE(misc-include-cleaner) Registry arguments are declared by the directly included public AsyncOperator.hpp.
AsyncOperator AsyncDelayAsyncOperator::create(AsyncOperatorRegistryArguments arguments)
{
    return create(std::move(arguments), std::nullopt);
}

AsyncOperator AsyncDelayAsyncOperator::create(AsyncOperatorRegistryArguments arguments, std::optional<size_t> concurrencyLimit)
{
    validateArity(arguments.inputs.size());
    const auto valueType = arguments.inputs[0].getDataType();
    return AsyncDelayAsyncOperator{.valueType = valueType, .concurrencyLimit = concurrencyLimit}.withInputAccessors(
        std::move(arguments.inputs));
}

Reflected Reflector<AsyncDelayAsyncOperator>::operator()(const AsyncDelayAsyncOperator& op, const ReflectionContext& context) const
{
    return context.reflect(ReflectedDelayConfiguration{op.valueType, op.concurrencyLimit, op.getInputAccessors()});
}

AsyncDelayAsyncOperator Unreflector<AsyncDelayAsyncOperator>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    auto configuration = context.unreflect<ReflectedDelayConfiguration>(data);
    return AsyncDelayAsyncOperator{.valueType = configuration.valueType, .concurrencyLimit = configuration.concurrencyLimit}
        .withInputAccessors(std::move(configuration.inputs));
}

AsyncResult<std::unique_ptr<PreparedAsyncCall>> AsyncDelayAsyncOperator::prepare(AsyncExecutionContext&, const AsyncRowHandle& row) const
{
    if (row.size() != 2 && row.size() != 3)
    {
        return std::unexpected(
            AsyncError{
                .exception = InvalidQuerySyntax("ASYNC_DELAY takes (value, millis[, max_concurrency])"),
                .kind = AsyncError::Type::Permanent});
    }
    PRECONDITION(valueInput && millisInput, "ASYNC_DELAY input accessors are not configured");
    auto value = (*valueInput)(row);
    if (value.getDataType() != valueType)
    {
        return std::unexpected(
            AsyncError{
                .exception = DifferentFieldTypeExpected(
                    "ASYNC_DELAY value type {} does not match its declared output {}", value.getDataType(), valueType),
                .kind = AsyncError::Type::Permanent});
    }
    const auto millisValue = (*millisInput)(row);
    if (!millisValue.getDataType().isInteger() || millisValue.getDataType().nullable)
    {
        return std::unexpected(
            AsyncError{
                .exception = DifferentFieldTypeExpected("ASYNC_DELAY expects a non-nullable integer delay in milliseconds"),
                .kind = AsyncError::Type::Permanent});
    }
    if (concurrencyInput)
    {
        const auto concurrencyValue = (*concurrencyInput)(row);
        if (!concurrencyValue.getDataType().isInteger() || concurrencyValue.getDataType().nullable)
        {
            return std::unexpected(
                AsyncError{
                    .exception = DifferentFieldTypeExpected("ASYNC_DELAY expects a non-nullable integer concurrency limit"),
                    .kind = AsyncError::Type::Permanent});
        }
    }
    const auto millis = checkedUnsignedIntegerOption(millisValue.view(), "ASYNC_DELAY millis");
    if (!millis)
    {
        return std::unexpected(AsyncError{.exception = millis.error(), .kind = AsyncError::Type::Permanent});
    }
    const auto deadline = checkedSteadyDeadline(*millis, "ASYNC_DELAY");
    if (!deadline)
    {
        return std::unexpected(AsyncError{.exception = deadline.error(), .kind = AsyncError::Type::Permanent});
    }
    return std::unique_ptr<PreparedAsyncCall>{std::make_unique<PreparedDelayCall>(std::move(value), *millis)};
}
}
