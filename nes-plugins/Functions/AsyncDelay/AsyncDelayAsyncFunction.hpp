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

#include <cstddef>
#include <optional>
#include <string>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <AsyncFunctionRegistry.hpp>

namespace NES
{

/// Executes `ASYNC_DELAY(value, millis[, max_concurrency])` on a timer, so a waiting call occupies no thread. Each row
/// gets its value and the delay it waited.
class AsyncDelayAsyncFunction final : public AsyncFunction
{
public:
    explicit AsyncDelayAsyncFunction(std::optional<size_t> concurrencyLimit) : concurrencyLimit(concurrencyLimit) { }

    /// The configuration is the concurrency limit, empty for none.
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments arguments);

    boost::asio::awaitable<AsyncCallResult> callValues(std::vector<std::string> arguments) override;

    [[nodiscard]] std::optional<size_t> maxConcurrency() const override { return concurrencyLimit; }

private:
    std::optional<size_t> concurrencyLimit;
};

}
