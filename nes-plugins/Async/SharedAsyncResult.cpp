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
#include <SharedAsyncResult.hpp>

#include <chrono>
#include <expected>
#include <optional>
#include <exception>
#include <memory>
#include <utility>
#include <Sources/SharedRuntimeValues.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; diagnostic misses direct use below
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio co_spawn provider; diagnostic misses direct use below
#include <boost/asio/dispatch.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <folly/ScopeGuard.h>
#include <ErrorHandling.hpp>
#include <Sources/AsyncError.hpp>

namespace NES
{
namespace
{
boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> runProducer(SharedAsyncResult::Factory factory)
{
    const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
        co_return std::unexpected(AsyncError::cancelled());
    }
    auto outcome = co_await factory();
    const bool wasCancelled = (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none;
    co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
    if (!outcome)
    {
        co_return outcome;
    }
    if (wasCancelled)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    co_return outcome;
}
}

SharedAsyncResult::SharedAsyncResult(Strand executor, Factory factory) : executor(std::move(executor)), factory(std::move(factory))
{
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> SharedAsyncResult::get()
{
    return wait(shared_from_this(), std::nullopt);
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> SharedAsyncResult::getUntil(const std::chrono::steady_clock::time_point deadline)
{
    return wait(shared_from_this(), deadline);
}

boost::asio::awaitable<void> SharedAsyncResult::join()
{
    return joinImpl(shared_from_this());
}

bool SharedAsyncResult::finished() const
{
    PRECONDITION(executor.running_in_this_thread(), "Inspect shared results on their strand");
    return done;
}

void SharedAsyncResult::start()
{
    PRECONDITION(executor.running_in_this_thread(), "Shared result must run on its strand");
    if (started || done)
    {
        return;
    }
    AsyncError launchFailure{.exception = IOError("Async producer launch failed"), .kind = AsyncError::Type::Permanent};
    const auto self = shared_from_this();
    auto publishOnFailedLaunch = folly::makeGuard(
        [this, failure = std::move(launchFailure)]() mutable
        {
            if (!done)
            {
                complete(std::unexpected(std::move(failure)));
            }
        });
    started = true;
    /// NOLINTNEXTLINE(misc-include-cleaner) Public <boost/asio/co_spawn.hpp> declares this; provider attribution is incorrect.
    boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) Declared by the public boost/asio/co_spawn.hpp provider.
        executor,
        runProducer(factory),
        boost::asio::bind_cancellation_slot(
            producerCancellation.slot(),
            [self](std::exception_ptr error, AsyncResult<SharedRuntimeValues> value)
            {
                if (error)
                {
                    /// Unexpected task failures are terminal, never retryable.
                    self->complete(
                        std::unexpected(
                            AsyncError{
                                .exception = IOError("Unexpected async producer task failure"), .kind = AsyncError::Type::Permanent}));
                }
                else
                {
                    self->complete(std::move(value));
                }
            }));
    publishOnFailedLaunch.dismiss();
}

void SharedAsyncResult::complete(AsyncResult<SharedRuntimeValues> outcome)
{
    PRECONDITION(executor.running_in_this_thread(), "Shared result must complete on its strand");
    if (done)
    {
        return;
    }
    this->outcome = std::move(outcome);
    completedAt = std::chrono::steady_clock::now();
    done = true;
    factory = {};
    for (const auto& waiter : waiters)
    {
        waiter->cancel();
    }
    waiters.clear();
}

AsyncResult<SharedRuntimeValues> SharedAsyncResult::read() const
{
    PRECONDITION(done, "Cannot read a pending shared result");
    INVARIANT(outcome.has_value(), "A completed shared result must have an outcome");
    return *outcome;
}

void SharedAsyncResult::cancel()
{
    boost::asio::dispatch(executor, [self = shared_from_this()] { self->cancelImpl(); });
}

void SharedAsyncResult::cancelImpl()
{
    if (done)
    {
        return;
    }
    if (!started)
    {
        complete(std::unexpected(AsyncError::cancelled()));
        return;
    }
    producerCancellation.emit(boost::asio::cancellation_type::terminal);
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> SharedAsyncResult::wait(
    std::shared_ptr<SharedAsyncResult> self, const std::optional<std::chrono::steady_clock::time_point> deadline)
{
    return wait(std::move(self), deadline, false);
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> SharedAsyncResult::wait(
    std::shared_ptr<SharedAsyncResult> self, const std::optional<std::chrono::steady_clock::time_point> deadline, const bool cleanup)
{
    PRECONDITION(self->executor.running_in_this_thread(), "Shared result waits must run on its strand");
    if (!cleanup && (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    if (self->done)
    {
        co_return self->read();
    }
    if (deadline && *deadline <= std::chrono::steady_clock::now())
    {
        co_return std::unexpected(AsyncError::timedOut());
    }
    auto waiter = std::make_shared<boost::asio::steady_timer>(self->executor);
    waiter->expires_at(deadline.value_or(std::chrono::steady_clock::time_point::max()));
    self->waiters.push_back(waiter);
    const auto unregister = folly::makeGuard([&] { std::erase(self->waiters, waiter); });
    self->start();
    if (self->done)
    {
        if (deadline && self->completedAt > *deadline)
        {
            co_return std::unexpected(AsyncError::timedOut());
        }
        co_return self->read();
    }
    const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    /// NOLINTNEXTLINE(misc-include-cleaner) Public <boost/system/error_code.hpp> declares this; provider attribution is incorrect.
    boost::system::error_code error; /// NOLINT(misc-include-cleaner) Declared by the public boost/system/error_code.hpp provider.
    if (cleanup)
    {
        co_await waiter->async_wait(
            boost::asio::bind_cancellation_slot(
                boost::asio::cancellation_slot{}, boost::asio::redirect_error(boost::asio::use_awaitable, error)));
    }
    else
    {
        co_await waiter->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
    }
    const bool wasCancelled
        = !cleanup && (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none;
    co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
    if (wasCancelled)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    if (self->done)
    {
        if (deadline && self->completedAt > *deadline)
        {
            co_return std::unexpected(AsyncError::timedOut());
        }
        co_return self->read();
    }
    co_return std::unexpected(AsyncError::timedOut());
}

boost::asio::awaitable<void> SharedAsyncResult::joinImpl(std::shared_ptr<SharedAsyncResult> self)
{
    PRECONDITION(self->executor.running_in_this_thread(), "Join shared results on their strand");
    if (self->started)
    {
        const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        static_cast<void>(co_await wait(self, std::nullopt, true));
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
    }
}
}
