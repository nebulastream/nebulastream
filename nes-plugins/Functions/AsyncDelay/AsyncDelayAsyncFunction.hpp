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
#include <string>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <AsyncFunctionRegistry.hpp>

namespace NES
{

/// Executes `ASYNC_DELAY(value, millis[, batch_size])` on a timer, so a waiting call occupies no thread. One wait covers
/// a whole batch; each row gets its value and the number of rows the wait covered.
class AsyncDelayAsyncFunction final : public AsyncFunction
{
public:
    explicit AsyncDelayAsyncFunction(size_t batchSize) : batchSize(batchSize) { }

    /// The configuration is the batch size, empty for one row per call.
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments arguments);

    boost::asio::awaitable<std::vector<AsyncCallResult>> callBatch(std::vector<std::vector<std::string>> rows) override;
    [[nodiscard]] size_t maxBatchSize() const override { return batchSize; }

private:
    size_t batchSize;
};

}
