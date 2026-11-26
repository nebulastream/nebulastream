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
#include <optional>
#include <string>
#include <string_view>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <boost/system/error_code.hpp>

namespace NES
{
namespace detail
{
struct ParsedHttpTarget
{
    std::string resolveHost;
    std::string port;
    std::string hostHeader;
    std::string endpointKey;
    std::string target;
};

AsyncResult<ParsedHttpTarget> parseHttpTarget(std::string_view url);
AsyncResult<void> checkHttpStatus(unsigned status, std::string_view endpoint, std::string_view body);
AsyncError classifyHttpTransportError(const boost::system::error_code& error, std::string_view url);
}

struct HttpCallAsyncOperator
{
    HttpCallAsyncOperator(AsyncInputAccessor body, AsyncInputAccessor url);
    HttpCallAsyncOperator(AsyncInputAccessor body,
                          AsyncInputAccessor url,
                          std::optional<AsyncInputAccessor> timeoutMillis,
                          std::optional<AsyncInputAccessor> retries);
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getWrittenFields() const;
    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, const AsyncRowHandle& row) const;

private:
    friend struct Reflector<HttpCallAsyncOperator>;
    friend struct Unreflector<HttpCallAsyncOperator>;
    AsyncInputAccessor bodyInput;
    AsyncInputAccessor urlInput;
    std::optional<AsyncInputAccessor> timeoutMillisInput;
    std::optional<AsyncInputAccessor> retriesInput;
    struct State;
};

template <>
struct Reflector<HttpCallAsyncOperator>
{
    Reflected operator()(const HttpCallAsyncOperator&, const ReflectionContext&) const;
};

template <>
struct Unreflector<HttpCallAsyncOperator>
{
    HttpCallAsyncOperator operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(AsyncOperatorConcept<HttpCallAsyncOperator>);
}
