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

#include <algorithm>
#include <exception>
#include <memory>
#include <utility>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>
#include <folly/ScopeGuard.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// Only the co_spawn transport needs a default-constructible result, not the function API.
boost::asio::awaitable<std::optional<AsyncValues>> optionalResult(boost::asio::awaitable<AsyncValues> operation)
{
    co_return co_await std::move(operation);
}
}

SharedAsyncResult::SharedAsyncResult(Executor executor, Factory factory) : executor(std::move(executor)), factory(std::move(factory))
{
}

boost::asio::awaitable<AsyncValues> SharedAsyncResult::get()
{
    return wait(shared_from_this(), std::nullopt);
}

boost::asio::awaitable<AsyncValues> SharedAsyncResult::getUntil(const Deadline deadline)
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
    started = true;
    const auto self = shared_from_this();
    try
    {
        boost::asio::co_spawn(
            executor,
            optionalResult(factory()),
            boost::asio::bind_cancellation_slot(
                producerCancellation.slot(),
                [self](std::exception_ptr error, std::optional<AsyncValues> values)
                { self->complete(std::move(error), std::move(values)); }));
    }
    catch (...) /// NOLINT(no-raw-catch-all) publish synchronous initiation failures to every subscriber
    {
        complete(std::current_exception(), {});
    }
}

void SharedAsyncResult::complete(std::exception_ptr error, std::optional<AsyncValues> values)
{
    PRECONDITION(executor.running_in_this_thread(), "Shared result must complete on its strand");
    if (done)
    {
        return;
    }
    failure = std::move(error);
    if (!failure)
    {
        PRECONDITION(values.has_value(), "A successful async invocation must produce a result");
        result = std::move(values);
    }
    completedAt = std::chrono::steady_clock::now();
    done = true;
    factory = {};
    for (const auto& waiter : waiters)
    {
        waiter->cancel();
    }
    waiters.clear();
}

AsyncValues SharedAsyncResult::read() const
{
    PRECONDITION(done, "Cannot read a pending shared result");
    if (failure)
    {
        std::rethrow_exception(failure);
    }
    return result.value();
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
        complete(std::make_exception_ptr(boost::system::system_error{boost::asio::error::operation_aborted}), {});
        return;
    }
    producerCancellation.emit(boost::asio::cancellation_type::terminal);
}

boost::asio::awaitable<AsyncValues> SharedAsyncResult::wait(std::shared_ptr<SharedAsyncResult> self, const std::optional<Deadline> deadline)
{
    PRECONDITION(self->executor.running_in_this_thread(), "Shared result waits must run on its strand");
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    if (self->done)
    {
        co_return self->read();
    }
    if (deadline && *deadline <= std::chrono::steady_clock::now())
    {
        throw boost::system::system_error{boost::asio::error::timed_out};
    }
    auto waiter = std::make_shared<boost::asio::steady_timer>(self->executor);
    waiter->expires_at(deadline.value_or(Deadline::max()));
    self->waiters.push_back(waiter);
    const auto readForWait = [&]
    {
        if (deadline && self->completedAt > *deadline)
        {
            throw boost::system::system_error{boost::asio::error::timed_out};
        }
        return self->read();
    };
    const auto unregister = folly::makeGuard([&] { std::erase(self->waiters, waiter); });
    self->start();
    /// Initiation may complete inline. Do not miss a notification that precedes registration of async_wait.
    if (self->done)
    {
        co_return readForWait();
    }
    boost::system::error_code error;
    co_await waiter->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, error));
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    if (self->done)
    {
        co_return readForWait();
    }
    if (!error)
    {
        error = boost::asio::error::timed_out;
    }
    throw boost::system::system_error{error};
}

boost::asio::awaitable<void> SharedAsyncResult::joinImpl(std::shared_ptr<SharedAsyncResult> self)
{
    PRECONDITION(self->executor.running_in_this_thread(), "Join shared results on their strand");
    if (!self->started)
    {
        co_return;
    }
    try
    {
        static_cast<void>(co_await self->get());
    }
    catch (...) /// NOLINT(no-raw-catch-all) joining observes completion, not the invocation's result
    {
    }
}

}
