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

#include <Sources/AsyncResourceLimiter.hpp>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

namespace NES
{

namespace
{
struct Registry
{
    std::mutex mutex;
    std::unordered_map<std::string, std::weak_ptr<AsyncResourceLimiter>> limiters;
};

Registry& registry()
{
    static Registry instance;
    return instance;
}
}

std::shared_ptr<AsyncResourceLimiter> AsyncResourceLimiter::of(const std::string& resource, const size_t limit)
{
    auto& [mutex, limiters] = registry();
    const std::scoped_lock lock(mutex);
    std::erase_if(limiters, [](const auto& entry) { return entry.second.expired(); });
    if (const auto existing = limiters.find(resource); existing != limiters.end())
    {
        auto limiter = existing->second.lock();
        if (limiter->limit() != limit)
        {
            NES_WARNING(
                "Shared limit of {} is already {} calls in flight; a call that asks for {} shares that limit",
                resource,
                limiter->limit(),
                limit);
        }
        return limiter;
    }
    auto limiter = std::make_shared<AsyncResourceLimiter>(resource, limit);
    limiters.emplace(resource, limiter);
    return limiter;
}

AsyncResourceLimiter::AsyncResourceLimiter(std::string resource, const size_t limit)
    : name(std::move(resource)), bound(std::max<size_t>(1, limit)), places(boost::asio::system_executor{}, bound)
{
}

AsyncResourceLimiter::~AsyncResourceLimiter()
{
    NES_DEBUG(
        "Shared limit of {}: at most {} of {} calls in flight; {} calls waited for a permit", name, mostHeld.load(), bound, waits.load());
}

boost::asio::awaitable<AsyncResourceLimiter::Permit> AsyncResourceLimiter::acquire()
{
    if (!places.try_send(boost::system::error_code{}))
    {
        ++waits;
        co_await places.async_send(boost::system::error_code{}, boost::asio::use_awaitable);
    }
    granted();
    co_return Permit{shared_from_this()};
}

std::optional<AsyncResourceLimiter::Permit> AsyncResourceLimiter::tryAcquire()
{
    if (!places.try_send(boost::system::error_code{}))
    {
        return std::nullopt;
    }
    granted();
    return Permit{shared_from_this()};
}

void AsyncResourceLimiter::granted()
{
    const auto now = ++held;
    auto most = mostHeld.load();
    while (now > most && !mostHeld.compare_exchange_weak(most, now))
    {
    }
}

void AsyncResourceLimiter::release()
{
    --held;
    /// Frees one place; the oldest waiting send, if any, takes it.
    [[maybe_unused]] const bool freed = places.try_receive([](boost::system::error_code) { });
}

AsyncResourceLimiter::Permit& AsyncResourceLimiter::Permit::operator=(Permit&& other) noexcept
{
    if (this != &other)
    {
        if (limiter)
        {
            limiter->release();
        }
        limiter = std::move(other.limiter);
    }
    return *this;
}

AsyncResourceLimiter::Permit::~Permit()
{
    if (limiter)
    {
        limiter->release();
    }
}

}
