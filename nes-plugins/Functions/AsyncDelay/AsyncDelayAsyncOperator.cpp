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

void validateIntegerOptionType(const DataType& type, std::string_view option)
{
    if (!type.isInteger() || type.nullable)
    {
        throw DifferentFieldTypeExpected("ASYNC_DELAY expects a non-nullable integer {}", option);
    }
}

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
    validateArity(arguments.argumentTypes.size());
    return AsyncDelayAsyncOperator{.valueType = arguments.argumentTypes.front(), .concurrencyLimit = std::nullopt};
}

AsyncDelayAsyncOperator AsyncDelayAsyncOperator::withInferredArguments(const AsyncArgumentsWithConstants& binding) const
{
    validateArity(binding.size());
    auto inferred = *this;
    inferred.valueType = binding.front().first;
    if (inferred.valueType.isType(DataType::Type::UNDEFINED))
    {
        throw CannotInferSchema("ASYNC_DELAY value type is unresolved");
    }

    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) arity validated above
    const auto& [millisType, millisConstant] = binding[1];
    validateIntegerOptionType(millisType, "delay in milliseconds");
    if (millisConstant)
    {
        const auto millis = checkedUnsignedIntegerOption(millisConstant->view(), "ASYNC_DELAY millis");
        if (!millis || !checkedSteadyDeadline(*millis, "ASYNC_DELAY"))
        {
            throw CannotInferSchema("ASYNC_DELAY constant delay must be a representable non-negative integer");
        }
    }

    inferred.concurrencyLimit.reset();
    if (binding.size() == 3)
    {
        const auto& [concurrencyType, concurrencyConstant] = binding[2];
        validateIntegerOptionType(concurrencyType, "concurrency limit");
        if (!concurrencyConstant)
        {
            throw CannotInferSchema("ASYNC_DELAY requires a constant concurrency limit");
        }
        const auto limit = checkedUnsignedIntegerOption(concurrencyConstant->view(), "ASYNC_DELAY concurrency");
        if (!limit || *limit == 0 || !std::in_range<size_t>(*limit))
        {
            throw CannotInferSchema("ASYNC_DELAY constant concurrency must be positive and representable");
        }
        inferred.concurrencyLimit = static_cast<size_t>(*limit);
    }
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return inferred;
}

AsyncResult<std::unique_ptr<PreparedAsyncCall>> AsyncDelayAsyncOperator::prepare(AsyncExecutionContext&, SharedRuntimeValues values) const
{
    if (values.size() != 2 && values.size() != 3)
    {
        return std::unexpected(
            AsyncError{
                .exception = InvalidQuerySyntax("ASYNC_DELAY takes (value, millis[, max_concurrency])"),
                .kind = AsyncError::Type::Permanent});
    }
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) values has two or three entries as checked above
    if (values[0].getDataType() != valueType)
    {
        return std::unexpected(
            AsyncError{
                .exception = DifferentFieldTypeExpected(
                    "ASYNC_DELAY value type {} does not match its declared output {}", values[0].getDataType(), valueType),
                .kind = AsyncError::Type::Permanent});
    }
    const auto& millisValue = values[1];
    if (!millisValue.getDataType().isInteger() || millisValue.getDataType().nullable)
    {
        return std::unexpected(
            AsyncError{
                .exception = DifferentFieldTypeExpected("ASYNC_DELAY expects a non-nullable integer delay in milliseconds"),
                .kind = AsyncError::Type::Permanent});
    }
    if (values.size() == 3 && (!values[2].getDataType().isInteger() || values[2].getDataType().nullable))
    {
        return std::unexpected(
            AsyncError{
                .exception = DifferentFieldTypeExpected("ASYNC_DELAY expects a non-nullable integer concurrency limit"),
                .kind = AsyncError::Type::Permanent});
    }
    const auto millis = checkedUnsignedIntegerOption(millisValue, "ASYNC_DELAY millis");
    if (!millis)
    {
        return std::unexpected(AsyncError{.exception = millis.error(), .kind = AsyncError::Type::Permanent});
    }
    const auto deadline = checkedSteadyDeadline(*millis, "ASYNC_DELAY");
    if (!deadline)
    {
        return std::unexpected(AsyncError{.exception = deadline.error(), .kind = AsyncError::Type::Permanent});
    }
    return std::unique_ptr<PreparedAsyncCall>{std::make_unique<PreparedDelayCall>(values[0].toOwned(), *millis)};
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
}
}
