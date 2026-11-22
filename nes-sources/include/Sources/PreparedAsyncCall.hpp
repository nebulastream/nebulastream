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
#include <Sources/AsyncError.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <boost/asio/awaitable.hpp>

namespace NES
{
/// One owning producer invocation, reusable for sequential retry attempts.
class PreparedAsyncCall
{
public:
    PreparedAsyncCall() : PreparedAsyncCall(std::nullopt) { }

    explicit PreparedAsyncCall(std::optional<size_t> retryOverride) : retryOverride(retryOverride) { }

    PreparedAsyncCall(const PreparedAsyncCall&) = delete;
    PreparedAsyncCall& operator=(const PreparedAsyncCall&) = delete;
    PreparedAsyncCall(PreparedAsyncCall&&) = delete;
    PreparedAsyncCall& operator=(PreparedAsyncCall&&) = delete;
    virtual ~PreparedAsyncCall() = default;

    [[nodiscard]] virtual boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> executeAttempt() = 0;

    /// Fixed at preparation; nullopt selects the executor's default retry policy.
    [[nodiscard]] const std::optional<size_t>& getRetryOverride() const { return retryOverride; }

private:
    const std::optional<size_t> retryOverride;
};
}
