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

#include <HttpCallAsyncOperator.hpp>
#include <HttpCallDefaults.hpp>

#include <cerrno> /// NOLINT(misc-include-cleaner) EHOSTDOWN is provided by the platform errno declarations.
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/CheckedInteger.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Util/CheckedDeadline.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp> /// NOLINT(misc-include-cleaner) Public Asio TCP provider; cleaner misses its use through the alias.
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core/error.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/error.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/message.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for request/response types.
#include <boost/beast/http/read.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for async_read.
#include <boost/beast/http/string_body.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for string_body.
#include <boost/beast/http/verb.hpp>
#include <boost/beast/http/write.hpp> /// NOLINT(misc-include-cleaner) Public Beast provider for async_write.
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; checker misattributes its error_code/category APIs.
#include <boost/url/parse.hpp>
#include <ErrorHandling.hpp>
#include <HttpCallResolver.hpp>

namespace NES
{
namespace
{
namespace http = boost::beast::http;
constexpr std::chrono::milliseconds DEFAULT_TIMEOUT{HTTP_CALL_DEFAULT_TIMEOUT_MILLIS};

AsyncError permanent(const Exception& error)
{
    return {.exception = error, .kind = AsyncError::Type::Permanent};
}
}

HttpCallAsyncOperator::HttpCallAsyncOperator(AsyncInputAccessor body, AsyncInputAccessor url)
    : HttpCallAsyncOperator(std::move(body), std::move(url), std::nullopt, std::nullopt)
{
}

HttpCallAsyncOperator::HttpCallAsyncOperator(AsyncInputAccessor body,
                                             AsyncInputAccessor url,
                                             std::optional<AsyncInputAccessor> timeoutMillis,
                                             std::optional<AsyncInputAccessor> retries)
    : bodyInput(std::move(body)), urlInput(std::move(url)), timeoutMillisInput(std::move(timeoutMillis)), retriesInput(std::move(retries))
{
}

/// NOLINTBEGIN(readability-magic-numbers) HTTP status codes are protocol-defined values.
AsyncResult<void> detail::checkHttpStatus(unsigned status, std::string_view endpoint, std::string_view body)
{
    if (status >= 200 && status < 300)
    {
        return {};
    }
    const bool retryable = status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    const auto error = IOError("HTTP_CALL to {} got status {}: {}{}",
                               endpoint,
                               status,
                               body,
                               retryable ? ". Retrying this POST may duplicate server-side effects" : "");
    return std::unexpected(AsyncError{.exception = error, .kind = retryable ? AsyncError::Type::Retryable : AsyncError::Type::Permanent});
}

/// NOLINTEND(readability-magic-numbers)

/// NOLINTNEXTLINE(misc-include-cleaner) error_code is declared by public boost/system/error_code.hpp.
AsyncError detail::classifyHttpTransportError(const boost::system::error_code& error, std::string_view url)
{
    namespace asioError = boost::asio::error;
    if (error == asioError::operation_aborted)
    {
        return AsyncError::cancelled();
    }
    if (error == asioError::timed_out || error == boost::beast::error::timeout)
    {
        return {.exception
                = IOError("HTTP_CALL to {} timed out: {}. Retrying this POST may duplicate server-side effects", url, error.message()),
                .kind = AsyncError::Type::Retryable};
    }
    /// NOLINTBEGIN(misc-include-cleaner) generic_category is declared by public boost/system/error_code.hpp.
    const bool retryable = error == asioError::connection_refused || error == asioError::connection_reset
        || error == asioError::connection_aborted || error == asioError::network_unreachable || error == asioError::network_down
        || error == asioError::host_unreachable
#ifdef EHOSTDOWN
        || error == boost::system::error_condition{EHOSTDOWN, boost::system::generic_category()}
#endif
        || error == asioError::try_again || error == asioError::eof || error == asioError::host_not_found_try_again
        || error == http::error::end_of_stream;
    /// NOLINTEND(misc-include-cleaner)
    const auto* const warning = retryable ? ". Retrying this POST may duplicate server-side effects" : "";
    return {.exception
            = IOError("HTTP_CALL to {} failed {}: {}{}", url, retryable ? "transiently" : "permanently", error.message(), warning),
            .kind = retryable ? AsyncError::Type::Retryable : AsyncError::Type::Permanent};
}

AsyncResult<detail::ParsedHttpTarget> detail::parseHttpTarget(std::string_view url)
{
    const auto parsed = boost::urls::parse_uri(url);
    if (!parsed || parsed->scheme() != "http" || parsed->encoded_host().empty())
    {
        return std::unexpected(permanent(FormattingError("HTTP_CALL needs an http:// url with a host, got {}", url)));
    }
    std::string resolveHost;
    if (parsed->host_type() == boost::urls::host_type::ipv6)
    {
        std::string_view rawIpv6{parsed->encoded_host()};
        rawIpv6 = rawIpv6.substr(1, rawIpv6.size() - 2);
        const auto zonePos = rawIpv6.find("%25");
        resolveHost.assign(rawIpv6.substr(0, zonePos));
        if (zonePos != std::string_view::npos)
        {
            resolveHost.push_back('%');
            resolveHost.append(parsed->zone_id());
        }
    }
    else
    {
        resolveHost = parsed->host_address();
    }
    const std::string uriHost{parsed->encoded_host()};
    const auto explicitPort = parsed->port();
    std::string port = !explicitPort.empty() ? std::string{explicitPort} : "80";
    std::string hostHeader = (!explicitPort.empty() && explicitPort != "80") ? (uriHost + ":" + port) : uriHost;
    std::string endpointKey = uriHost + ":" + port;
    std::string target = parsed->encoded_target().empty() ? "/" : std::string{parsed->encoded_target()};
    return detail::ParsedHttpTarget{
        .resolveHost = std::move(resolveHost),
        .port = std::move(port),
        .hostHeader = std::move(hostHeader),
        .endpointKey = std::move(endpointKey),
        .target = std::move(target),
    };
}

struct HttpCallAsyncOperator::State
{
    std::optional<boost::beast::tcp_stream> stream;
    boost::beast::flat_buffer buffer;
    std::string connectedTo;
    class Prepared;
    boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>
    execute(std::string body, std::string url, detail::ParsedHttpTarget parsedTarget, uint64_t millis);
    boost::asio::awaitable<AsyncResult<std::string>> exchange(detail::ParsedHttpTarget parsedTarget,
                                                              std::string body,
                                                              std::string url,
                                                              std::chrono::steady_clock::time_point deadline);
};

class HttpCallAsyncOperator::State::Prepared final : public PreparedAsyncCall
{
public:
    Prepared(std::shared_ptr<State> owner,
             std::string body,
             std::string url,
             detail::ParsedHttpTarget parsedTarget,
             uint64_t millis,
             size_t retries)
        : PreparedAsyncCall(retries)
        , owner(std::move(owner))
        , body(std::move(body))
        , url(std::move(url))
        , parsedTarget(std::move(parsedTarget))
        , millis(millis)
    {
    }

    boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() override
    {
        co_return co_await owner->execute(body, url, parsedTarget, millis);
    }

private:
    std::shared_ptr<State> owner;
    std::string body, url;
    detail::ParsedHttpTarget parsedTarget;
    uint64_t millis;
};

AsyncResult<std::unique_ptr<PreparedAsyncCall>> /// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
HttpCallAsyncOperator::prepare(AsyncExecutionContext& context, const AsyncRowHandle& row) const
{
    const auto bodyValue = bodyInput(row);
    const auto urlValue = urlInput(row);
    const auto bodyType = bodyValue.getDataType();
    const auto urlType = urlValue.getDataType();
    if (!bodyType.isType(DataType::Type::VARSIZED) || bodyType.nullable || !urlType.isType(DataType::Type::VARSIZED) || urlType.nullable)
    {
        return std::unexpected(permanent(DifferentFieldTypeExpected("HTTP_CALL expects non-nullable VARSIZED body and URL")));
    }
    std::string body{bodyValue.view().get<std::string_view>()};
    std::string url{urlValue.view().get<std::string_view>()};
    auto millis = static_cast<uint64_t>(DEFAULT_TIMEOUT.count());
    if (timeoutMillisInput)
    {
        const auto timeoutValue = (*timeoutMillisInput)(row);
        if (!timeoutValue.getDataType().isInteger() || timeoutValue.getDataType().nullable)
        {
            return std::unexpected(permanent(DifferentFieldTypeExpected("HTTP_CALL timeout must be a non-nullable integer")));
        }
        auto checked = checkedUnsignedIntegerOption(timeoutValue.view(), "HTTP_CALL timeout");
        if (!checked)
        {
            return std::unexpected(permanent(checked.error()));
        }
        millis = *checked;
    }
    if (millis == 0)
    {
        return std::unexpected(permanent(ArithmeticalError("HTTP_CALL expects a positive timeout in milliseconds, got {}", millis)));
    }
    auto deadline = checkedSteadyDeadline(millis, "HTTP_CALL");
    if (!deadline)
    {
        return std::unexpected(permanent(deadline.error()));
    }
    auto parsedTarget = detail::parseHttpTarget(url);
    if (!parsedTarget)
    {
        return std::unexpected(parsedTarget.error());
    }
    size_t retries = 0;
    if (retriesInput)
    {
        const auto retriesValue = (*retriesInput)(row);
        if (!retriesValue.getDataType().isInteger() || retriesValue.getDataType().nullable)
        {
            return std::unexpected(permanent(DifferentFieldTypeExpected("HTTP_CALL retries must be a non-nullable integer")));
        }
        auto checked = checkedUnsignedIntegerOption(retriesValue.view(), "HTTP_CALL retries");
        if (!checked)
        {
            return std::unexpected(permanent(checked.error()));
        }
        if (!std::in_range<size_t>(*checked) || *checked == std::numeric_limits<size_t>::max())
        {
            return std::unexpected(permanent(ArithmeticalError("HTTP_CALL expects a representable retry count, got {}", *checked)));
        }
        retries = static_cast<size_t>(*checked);
    }
    auto state = context.getSharedSlotState<State>();
    return std::unique_ptr<PreparedAsyncCall>{
        std::make_unique<State::Prepared>(std::move(state), std::move(body), std::move(url), std::move(*parsedTarget), millis, retries)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view HttpCallAsyncOperator::getType() const
{
    return "HTTP_CALL";
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema<UnqualifiedUnboundField, Ordered> HttpCallAsyncOperator::getWrittenFields() const
{
    return {{Identifier::parse("response"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}}};
}

namespace detail
{
struct ReflectedHttpCallConfiguration
{
    AsyncInputAccessor bodyInput;
    AsyncInputAccessor urlInput;
    std::optional<AsyncInputAccessor> timeoutMillisInput;
    std::optional<AsyncInputAccessor> retriesInput;
};
}

Reflected Reflector<HttpCallAsyncOperator>::operator()(const HttpCallAsyncOperator& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedHttpCallConfiguration{op.bodyInput, op.urlInput, op.timeoutMillisInput, op.retriesInput});
}

HttpCallAsyncOperator Unreflector<HttpCallAsyncOperator>::operator()(const Reflected& data, const ReflectionContext& context) const
{
    const auto configuration = context.unreflect<detail::ReflectedHttpCallConfiguration>(data);
    return HttpCallAsyncOperator{
        configuration.bodyInput, configuration.urlInput, configuration.timeoutMillisInput, configuration.retriesInput};
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> HttpCallAsyncOperator::State::execute(
    std::string body, std::string url, detail::ParsedHttpTarget parsedTarget, uint64_t millis)
{
    auto deadline = checkedSteadyDeadline(millis, "HTTP_CALL");
    if (!deadline)
    {
        co_return std::unexpected(permanent(deadline.error()));
    }
    const bool previousThrowPolicy = co_await boost::asio::this_coro::throw_if_cancelled();
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    auto response = co_await exchange(std::move(parsedTarget), std::move(body), url, *deadline);
    co_await boost::asio::this_coro::throw_if_cancelled(previousThrowPolicy);
    if (!response)
    {
        stream.reset();
        co_return std::unexpected(response.error());
    }
    co_return SharedRuntimeValues::make(std::move(*response));
}

boost::asio::awaitable<AsyncResult<std::string>> HttpCallAsyncOperator::State::exchange(detail::ParsedHttpTarget parsedTarget,
                                                                                        std::string body,
                                                                                        std::string url,
                                                                                        std::chrono::steady_clock::time_point deadline)
{
    namespace asio = boost::asio;
    auto fail = [&url](const boost::system::error_code& error) -> AsyncResult<std::string>
    { return std::unexpected(detail::classifyHttpTransportError(error, url)); };
    const auto cancellation = co_await asio::this_coro::cancellation_state;
    const auto checkDeadlineAndCancellation = [&]() -> std::optional<AsyncError>
    {
        if (cancellation.cancelled() != asio::cancellation_type::none)
        {
            return AsyncError::cancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return detail::classifyHttpTransportError(asio::error::timed_out, url);
        }
        return std::nullopt;
    };
    if (const auto failure = checkDeadlineAndCancellation())
    {
        co_return std::unexpected(*failure);
    }
    const auto executor = co_await asio::this_coro::executor;
    if (!stream || connectedTo != parsedTarget.endpointKey)
    {
        auto endpoints = co_await detail::resolveHttpHost(parsedTarget.resolveHost, parsedTarget.port, deadline);
        if (!endpoints)
        {
            if (endpoints.error().kind == AsyncError::Type::TimedOut)
            {
                co_return std::unexpected(detail::classifyHttpTransportError(asio::error::timed_out, url));
            }
            co_return std::unexpected(endpoints.error());
        }
        if (const auto failure = checkDeadlineAndCancellation())
        {
            co_return std::unexpected(*failure);
        }
        stream.emplace(executor);
        buffer.clear();
        stream->expires_at(deadline);
        boost::system::error_code error;
        co_await stream->async_connect(*endpoints, asio::redirect_error(asio::use_awaitable, error));
        if (error)
        {
            co_return fail(error);
        }
        if (const auto failure = checkDeadlineAndCancellation())
        {
            co_return std::unexpected(*failure);
        }
        connectedTo = parsedTarget.endpointKey;
    }
    if (const auto failure = checkDeadlineAndCancellation())
    {
        co_return std::unexpected(*failure);
    }
    /// NOLINTBEGIN(misc-include-cleaner, readability-magic-numbers) request/string_body come from public Beast headers; 11 denotes HTTP/1.1.
    http::request<http::string_body> request{http::verb::post, parsedTarget.target, 11, std::move(body)};
    /// NOLINTEND(misc-include-cleaner, readability-magic-numbers)
    request.set(http::field::host, parsedTarget.hostHeader);
    request.set(http::field::content_type, "text/plain");
    request.keep_alive(true);
    request.prepare_payload();
    stream->expires_at(deadline);
    boost::system::error_code error;
    /// NOLINTBEGIN(misc-include-cleaner) async_write is declared by public boost/beast/http/write.hpp.
    co_await http::async_write(*stream, request, asio::redirect_error(asio::use_awaitable, error));
    /// NOLINTEND(misc-include-cleaner)
    if (error)
    {
        co_return fail(error);
    }
    if (const auto failure = checkDeadlineAndCancellation())
    {
        co_return std::unexpected(*failure);
    }
    /// NOLINTBEGIN(misc-include-cleaner) response is declared by public Beast message.hpp.
    http::response<http::string_body> response;
    /// NOLINTEND(misc-include-cleaner)
    stream->expires_at(deadline);
    /// NOLINTBEGIN(misc-include-cleaner) async_read is declared by public boost/beast/http/read.hpp.
    co_await http::async_read(*stream, buffer, response, asio::redirect_error(asio::use_awaitable, error));
    /// NOLINTEND(misc-include-cleaner)
    if (error)
    {
        co_return fail(error);
    }
    if (const auto failure = checkDeadlineAndCancellation())
    {
        co_return std::unexpected(*failure);
    }
    if (!response.keep_alive())
    {
        stream.reset();
    }
    auto status = detail::checkHttpStatus(response.result_int(), url, response.body());
    if (!status)
    {
        co_return std::unexpected(status.error());
    }
    co_return std::move(response.body());
}
}
