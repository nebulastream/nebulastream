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

#include <Async/HttpOperatorExecutor.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <string_view>

#include <fmt/format.h>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncOperatorOptions.hpp>
#include <Http/HttpBackend.hpp>
#include <ErrorHandling.hpp>

namespace NES::detail
{

std::exception_ptr requestFailure(const std::string_view url, const HttpError& error, const std::string_view executorName)
{
    return std::make_exception_ptr(HttpRequestFailed("{} could not reach {}: {}", executorName, url, error.message));
}

HttpRequest httpRequestFromConfig(const AsyncOperatorContext& context, const std::string_view executorName)
{
    HttpRequest request{};
    request.url = requiredOption(context, "url", executorName);

    const HttpRequest defaults{};
    request.timeout = std::chrono::seconds{static_cast<std::chrono::seconds::rep>(
        numericOption(context, "timeout_seconds", static_cast<uint64_t>(defaults.timeout.count()), executorName))};
    request.connectTimeout = std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(
        numericOption(context, "connect_timeout_ms", static_cast<uint64_t>(defaults.connectTimeout.count()), executorName))};
    request.maxRetries = numericOption(context, "max_retries", defaults.maxRetries, executorName);

    /// The credential lives on the worker; what travelled here is the variable's name. Resolved
    /// once, when the consumer source opens, not per request.
    if (const auto variable = findOption(context, "api_key_env"))
    {
        const char* const value = std::getenv(variable->c_str());
        if (value == nullptr)
        {
            throw InvalidConfigParameter(
                "{} reads its API key from environment variable {}, which is not set on this worker", executorName, *variable);
        }
        request.headers.emplace_back("Authorization", fmt::format("Bearer {}", value));
    }
    return request;
}

}
