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

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/system/error_code.hpp>

namespace NES
{

/// A bound on the calls in flight to one resource, e.g. one model on one endpoint, shared by every async call in the
/// process that names the resource: all stages of all queries on the worker. Each call waits for a permit before it
/// goes out and returns it once its answer is in, so a free permit goes to whichever stage has a call ready, in the
/// order they asked.
///
/// A stage's own `max_concurrency` still caps that stage; the shared limit caps them all together.
class AsyncResourceLimiter : public std::enable_shared_from_this<AsyncResourceLimiter>
{
public:
    /// Held while one call is in flight; returns its permit on destruction.
    class Permit
    {
    public:
        Permit() = default;
        explicit Permit(std::shared_ptr<AsyncResourceLimiter> limiter) : limiter(std::move(limiter)) { }
        Permit(const Permit&) = delete;
        Permit& operator=(const Permit&) = delete;
        Permit(Permit&& other) noexcept = default;
        Permit& operator=(Permit&& other) noexcept;
        ~Permit();

    private:
        std::shared_ptr<AsyncResourceLimiter> limiter;
    };

    /// The limiter of `resource`, shared with everything that asked for it and still holds it. The first to ask sets the
    /// limit; one that asks for another limit later gets the existing limiter, and a warning is logged.
    static std::shared_ptr<AsyncResourceLimiter> of(const std::string& resource, size_t limit);

    AsyncResourceLimiter(std::string resource, size_t limit);
    ~AsyncResourceLimiter();
    AsyncResourceLimiter(const AsyncResourceLimiter&) = delete;
    AsyncResourceLimiter& operator=(const AsyncResourceLimiter&) = delete;
    AsyncResourceLimiter(AsyncResourceLimiter&&) = delete;
    AsyncResourceLimiter& operator=(AsyncResourceLimiter&&) = delete;

    /// Waits for a permit. Supports per-operation cancellation; a cancelled wait throws and holds no permit.
    boost::asio::awaitable<Permit> acquire();

    /// A permit if one is free right now, for work that may as well be skipped, e.g. a further parallel request.
    std::optional<Permit> tryAcquire();

    [[nodiscard]] const std::string& resource() const { return name; }
    [[nodiscard]] size_t limit() const { return bound; }
    /// The most permits held at once so far.
    [[nodiscard]] size_t mostInUse() const { return mostHeld.load(); }

private:
    void granted();
    void release();

    std::string name;
    size_t bound;
    /// Holds one element per permit in use: a send waits while all `bound` places are taken, a receive frees one.
    boost::asio::experimental::concurrent_channel<void(boost::system::error_code)> places;
    std::atomic<size_t> held{0};
    std::atomic<size_t> mostHeld{0};
    std::atomic<size_t> waits{0};
};

}
