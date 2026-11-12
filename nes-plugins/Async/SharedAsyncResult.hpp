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

#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

namespace NES
{

/// One lazy invocation, many asynchronous subscribers, one immutable result/error. This is not a shared awaitable frame.
/// Waits and state inspection run on the supplied strand. Each wait has its own timer/cancellation slot; ending a wait never cancels the producer.
/// The owner explicitly cancels the producer and joins it before releasing resources. cancel() may be called from any thread.
class SharedAsyncResult final : public std::enable_shared_from_this<SharedAsyncResult>
{
public:
    using Executor = boost::asio::strand<boost::asio::io_context::executor_type>;
    using Factory = std::function<boost::asio::awaitable<AsyncValues>()>;
    using Deadline = std::chrono::steady_clock::time_point;

    SharedAsyncResult(Executor executor, Factory factory);
    [[nodiscard]] boost::asio::awaitable<AsyncValues> get();
    [[nodiscard]] boost::asio::awaitable<AsyncValues> getUntil(Deadline deadline);
    /// Join started work without starting a lazy producer, ignoring its outcome. Use a non-cancelled cleanup scope.
    [[nodiscard]] boost::asio::awaitable<void> join();
    void cancel();
    [[nodiscard]] bool finished() const;

private:
    static boost::asio::awaitable<AsyncValues> wait(std::shared_ptr<SharedAsyncResult> self, std::optional<Deadline> deadline);
    static boost::asio::awaitable<void> joinImpl(std::shared_ptr<SharedAsyncResult> self);
    void start();
    void cancelImpl();
    void complete(std::exception_ptr error, std::optional<AsyncValues> result);
    [[nodiscard]] AsyncValues read() const;

    Executor executor;
    Factory factory;
    boost::asio::cancellation_signal producerCancellation;
    bool started = false;
    bool done = false;
    Deadline completedAt{};
    std::optional<AsyncValues> result;
    std::exception_ptr failure;
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters;
};

}
