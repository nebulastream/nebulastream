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
#include <string_view>
#include <utility>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Sources/AsyncOperator.hpp>
#include <boost/system/error_code.hpp>
#include <AsyncOperatorRegistry.hpp>

namespace NES
{
namespace detail
{
/// HTTP status and transport classification never throws; only explicitly transient outcomes are retryable.
AsyncResult<void> checkHttpStatus(unsigned status, std::string_view endpoint, std::string_view body);
AsyncError classifyHttpTransportError(const boost::system::error_code& error, std::string_view url);
}

/// Native serializable HTTP_CALL configuration; connection state belongs to an execution context, not this value.
struct HttpCallAsyncOperator
{
    AsyncInputAccessors inputs{};
    std::optional<AsyncInputAccessor> bodyInput{};
    std::optional<AsyncInputAccessor> urlInput{};
    std::optional<AsyncInputAccessor> timeoutMillisInput{};
    std::optional<AsyncInputAccessor> retriesInput{};

    [[nodiscard]] const AsyncInputAccessors& getInputAccessors() const { return inputs; }

    [[nodiscard]] HttpCallAsyncOperator withInputAccessors(AsyncInputAccessors accessors) const
    {
        auto copy = *this;
        copy.inputs = std::move(accessors);
        copy.bodyInput = copy.inputs.size() >= 1 ? std::optional{copy.inputs[0]} : std::nullopt;
        copy.urlInput = copy.inputs.size() >= 2 ? std::optional{copy.inputs[1]} : std::nullopt;
        copy.timeoutMillisInput = copy.inputs.size() >= 3 ? std::optional{copy.inputs[2]} : std::nullopt;
        copy.retriesInput = copy.inputs.size() >= 4 ? std::optional{copy.inputs[3]} : std::nullopt;
        return copy;
    }

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] Schema<UnqualifiedUnboundField, Ordered> getOutputSchema() const;
    [[nodiscard]] AsyncResult<std::unique_ptr<PreparedAsyncCall>> prepare(AsyncExecutionContext& context, const AsyncRowHandle& row) const;

    static AsyncOperator create(AsyncOperatorRegistryArguments arguments);

private:
    struct State;
};

template <>
struct Reflector<HttpCallAsyncOperator>
{
    Reflected operator()(const HttpCallAsyncOperator& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<HttpCallAsyncOperator>
{
    HttpCallAsyncOperator operator()(const Reflected& data, const ReflectionContext& context) const;
};

static_assert(AsyncOperatorConcept<HttpCallAsyncOperator>);
}
