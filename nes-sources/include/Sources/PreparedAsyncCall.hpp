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
#include <Sources/AsyncError.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <boost/asio/awaitable.hpp>

namespace NES
{
/// One owning producer invocation, reusable for sequential retry attempts.
class PreparedAsyncCall
{
public:
    PreparedAsyncCall() = default;

    explicit PreparedAsyncCall(size_t maxRetries) : maxRetries(maxRetries) { }

    PreparedAsyncCall(const PreparedAsyncCall&) = delete;
    PreparedAsyncCall& operator=(const PreparedAsyncCall&) = delete;
    PreparedAsyncCall(PreparedAsyncCall&&) = delete;
    PreparedAsyncCall& operator=(PreparedAsyncCall&&) = delete;
    virtual ~PreparedAsyncCall() = default;

    [[nodiscard]] virtual boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() = 0;

    /// Fixed at preparation; zero disables retries.
    [[nodiscard]] size_t getMaxRetries() const { return maxRetries; }

private:
    const size_t maxRetries = 0;
};
}
