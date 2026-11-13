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
#include <initializer_list>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <boost/asio/awaitable.hpp>

namespace NES
{

/// Immutable values plus ownership of their backing storage.
/// Views are borrowed from this handle, not from a temporary coroutine frame.
/// Initially backed by owned scalar values, in the future this could be backed by tuple buffers owned by the operator handle.
class SharedRuntimeValues
{
public:
    SharedRuntimeValues() = delete;
    explicit SharedRuntimeValues(std::vector<RuntimeValue> values);
    SharedRuntimeValues(std::initializer_list<RuntimeValue> values);

    template <typename... T>
    [[nodiscard]] static SharedRuntimeValues make(T&&... values)
    {
        std::vector<RuntimeValue> owned;
        owned.reserve(sizeof...(T));
        (owned.emplace_back(std::forward<T>(values)), ...);
        return SharedRuntimeValues{std::move(owned)};
    }

    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] RuntimeValueView operator[](size_t index) const&;
    RuntimeValueView operator[](size_t index) const&& = delete;

private:
    std::shared_ptr<const std::vector<RuntimeValue>> storage;
};

/// Given to each instance once. Constant positions are preserved; a missing value means a per-row argument.
/// A present RuntimeValue may itself be a typed SQL null. Constant arguments remain visible to call().
struct AsyncFunctionBinding
{
    std::vector<DataType> argumentTypes;
    std::vector<std::optional<RuntimeValue>> constantArguments;
    std::vector<DataType> resultTypes;
};

/// One validated, owning producer invocation, reusable for sequential retry attempts.
/// Preparation rejects input errors before retries; call() performs only execution and operational checks.
/// A prepared call may borrow its function instance, which must outlive it. Attempts must not run concurrently.
class PreparedAsyncCall
{
public:
    virtual ~PreparedAsyncCall() = default;
    virtual boost::asio::awaitable<SharedRuntimeValues> call() = 0;

    [[nodiscard]] virtual std::optional<size_t> maxRetries() const { return std::nullopt; }
};

/// Runtime implementation of an AsyncLogicalFunction. An instance serves one row at a time and may retain connections.
/// Arguments and returned values retain their storage across suspension. Results must match binding.resultTypes exactly,
/// including nullability; the executor does not convert them. Plan descriptions live separately in AsyncCallGraph.hpp.
class AsyncFunction
{
public:
    virtual ~AsyncFunction() = default;
    virtual boost::asio::awaitable<SharedRuntimeValues> call(SharedRuntimeValues arguments) = 0;
    /// Runs once before the source's retry loop. Override to validate/normalize per-row input before any attempt.
    /// The default adapter owns arguments, evaluates maxRetries once, and delegates attempts to call(arguments).
    virtual std::unique_ptr<PreparedAsyncCall> prepare(SharedRuntimeValues arguments);

    /// Additional attempts for this call. An explicit value overrides the source default, including zero.
    /// Evaluated once before invocation; invalid retry options are not retried.
    [[nodiscard]] virtual std::optional<size_t> maxRetries(const SharedRuntimeValues&) const { return std::nullopt; }

    /// Rows whose calls of this function may be in flight at once, if the function itself sets a bound.
    [[nodiscard]] virtual std::optional<size_t> maxConcurrency() const { return std::nullopt; }
};

}
