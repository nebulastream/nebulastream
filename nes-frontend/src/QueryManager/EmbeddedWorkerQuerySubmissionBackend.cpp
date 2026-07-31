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

#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>
#include <variant>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Overloaded.hpp>
#include <folly/concurrency/UnboundedQueue.h>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES::detail
{
namespace
{
struct Stop
{
};

enum class ReplyStateStatus : uint8_t
{
    Waiting,
    Abandoned,
    Completed
};

template <typename Result>
struct ReplyState
{
    std::promise<Result> promise;
    std::atomic<ReplyStateStatus> status = ReplyStateStatus::Waiting;
};

using StartQueryResult = std::expected<QueryId, Exception>;
using StopQueryResult = std::expected<void, Exception>;
using QueryStatusResult = std::expected<LocalQueryStatusSnapshot, Exception>;
using WorkerStatusResult = std::expected<WorkerStatus, Exception>;

struct StartQuery
{
    LogicalPlan plan;
    std::shared_ptr<ReplyState<StartQueryResult>> reply;
};

struct StopQuery
{
    QueryId id;
    std::shared_ptr<ReplyState<StopQueryResult>> reply;
};

struct QueryStatusQuery
{
    QueryId id;
    std::shared_ptr<ReplyState<QueryStatusResult>> reply;
};

struct WorkerStatusQuery
{
    std::chrono::system_clock::time_point after;
    std::shared_ptr<ReplyState<WorkerStatusResult>> reply;
};

using Request = std::variant<Stop, StartQuery, StopQuery, QueryStatusQuery, WorkerStatusQuery>;
}

/// All shared state between callers and the single worker thread lives here:
/// the request/reply queues, the mutex that serializes callers, the config,
/// and the worker thread itself. The header forward-declares this so the
/// backend is a thin pimpl wrapper.
class Channel
{
    using RequestQueue = folly::UMPSCQueue<Request, /*MayBlock*/ true>;

public:
    Channel(WorkerConfig config, const SingleNodeWorkerConfiguration& workerConfiguration)
        : requests(std::make_shared<RequestQueue>())
        , completion(std::make_shared<std::promise<void>>())
        , completed(completion->get_future().share())
        , thread(
              "main",
              config.host,
              [requests = requests, completion = completion, config = std::move(config), workerConfiguration](
                  const std::stop_token& stopToken)
              {
                  runWorker(stopToken, *requests, config, workerConfiguration);
                  completion->set_value();
              })
    {
    }

    ~Channel() noexcept
    {
        try
        {
            static_cast<void>(shutdown(std::chrono::steady_clock::now()));
        }
        catch (...)
        {
        }
    }

    bool shutdown(const std::chrono::steady_clock::time_point deadline)
    {
        if (!thread.joinable())
        {
            return !shutdownTimedOut;
        }
        if (completed.wait_for(std::chrono::milliseconds{0}) == std::future_status::ready)
        {
            thread.join();
            return true;
        }
        thread.requestStop();
        if (deadline != std::chrono::steady_clock::time_point::max() && std::chrono::steady_clock::now() >= deadline)
        {
            shutdownTimedOut = true;
            thread.detach();
            return false;
        }
        const auto finished = deadline == std::chrono::steady_clock::time_point::max()
            ? (completed.wait(), true)
            : completed.wait_until(deadline) == std::future_status::ready;
        if (finished)
        {
            thread.join();
            return true;
        }
        shutdownTimedOut = true;
        thread.detach();
        return false;
    }

    StartQueryResult start(LogicalPlan plan, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
    {
        auto reply = std::make_shared<ReplyState<StartQueryResult>>();
        return submit(
            StartQuery{.plan = std::move(plan), .reply = reply},
            reply,
            deadline,
            stopToken,
            [] { return QueryStartFailed("Embedded query start was cancelled or reached its deadline"); });
    }

    StopQueryResult stop(QueryId id, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
    {
        auto reply = std::make_shared<ReplyState<StopQueryResult>>();
        return submit(
            StopQuery{.id = std::move(id), .reply = reply},
            reply,
            deadline,
            stopToken,
            [] { return QueryStopFailed("Embedded query stop was cancelled or reached its deadline"); });
    }

    QueryStatusResult status(QueryId id, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken) const
    {
        auto reply = std::make_shared<ReplyState<QueryStatusResult>>();
        return submit(
            QueryStatusQuery{.id = std::move(id), .reply = reply},
            reply,
            deadline,
            stopToken,
            [] { return QueryStatusFailed("Embedded query status was cancelled or reached its deadline"); });
    }

    WorkerStatusResult workerStatus(std::chrono::system_clock::time_point after) const
    {
        auto reply = std::make_shared<ReplyState<WorkerStatusResult>>();
        return submit(
            WorkerStatusQuery{.after = after, .reply = reply},
            reply,
            std::chrono::steady_clock::time_point::max(),
            {},
            [] { return UnknownException("Embedded worker status request was cancelled"); });
    }

private:
    template <typename Result, typename Cleanup>
    static void complete(const std::shared_ptr<ReplyState<Result>>& reply, Result result, Cleanup&& cleanup)
    {
        const auto previous = reply->status.exchange(ReplyStateStatus::Completed);
        if (previous == ReplyStateStatus::Abandoned)
        {
            std::forward<Cleanup>(cleanup)(result);
        }
        reply->promise.set_value(std::move(result));
    }

    static void runWorker(
        const std::stop_token& stopToken,
        RequestQueue& requests,
        const WorkerConfig& config,
        const SingleNodeWorkerConfiguration& workerConfiguration)
    {
        SingleNodeWorkerConfiguration mergedConfig = config.config;
        mergedConfig.applyExplicitlySetFrom(workerConfiguration);
        mergedConfig.grpcAddressUri.setValue(config.host.getRawValue());
        mergedConfig.dataAddress.setValue(config.dataAddress);
        SingleNodeWorker worker(mergedConfig, config.host);
        const std::stop_callback poison(stopToken, [&]() { requests.enqueue(Request{Stop{}}); });

        while (!stopToken.stop_requested())
        {
            Request request;
            requests.dequeue(request);
            if (std::holds_alternative<Stop>(request))
            {
                break;
            }
            std::visit(
                Overloaded{
                    [](Stop) { std::unreachable(); },
                    [&](StartQuery& request)
                    {
                        auto result = worker.startQuery(std::move(request.plan));
                        complete(
                            request.reply,
                            std::move(result),
                            [&](const StartQueryResult& abandoned)
                            {
                                if (abandoned)
                                {
                                    static_cast<void>(worker.stopQuery(*abandoned));
                                }
                            });
                    },
                    [&](const StopQuery& request)
                    { complete(request.reply, worker.stopQuery(request.id), [](const StopQueryResult&) { }); },
                    [&](const QueryStatusQuery& request)
                    { complete(request.reply, worker.getQueryStatus(request.id), [](const QueryStatusResult&) { }); },
                    [&](const WorkerStatusQuery& request)
                    {
                        complete(
                            request.reply, WorkerStatusResult{worker.getWorkerStatus(request.after)}, [](const WorkerStatusResult&) { });
                    },
                },
                request);
        }
    }

    template <typename RequestT, typename Result, typename ErrorFactory>
    Result submit(
        RequestT request,
        const std::shared_ptr<ReplyState<Result>>& reply,
        const std::chrono::steady_clock::time_point deadline,
        const std::stop_token stopToken,
        ErrorFactory&& errorFactory) const
    {
        const std::lock_guard lock(submitMutex);
        if (stopToken.stop_requested() || std::chrono::steady_clock::now() >= deadline)
        {
            return std::unexpected(std::forward<ErrorFactory>(errorFactory)());
        }

        auto future = reply->promise.get_future();
        requests->enqueue(Request{std::move(request)});
        while (true)
        {
            auto waitDuration = std::chrono::milliseconds(25);
            if (deadline != std::chrono::steady_clock::time_point::max())
            {
                waitDuration = std::min(
                    waitDuration, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()));
            }
            if (future.wait_for(waitDuration) == std::future_status::ready)
            {
                return future.get();
            }
            if (!stopToken.stop_requested() && std::chrono::steady_clock::now() < deadline)
            {
                continue;
            }

            auto expected = ReplyStateStatus::Waiting;
            if (reply->status.compare_exchange_strong(expected, ReplyStateStatus::Abandoned))
            {
                return std::unexpected(std::forward<ErrorFactory>(errorFactory)());
            }
            return future.get();
        }
    }

    std::shared_ptr<RequestQueue> requests;
    std::shared_ptr<std::promise<void>> completion;
    std::shared_future<void> completed;
    mutable std::mutex submitMutex;
    Thread thread;
    bool shutdownTimedOut = false;
};
}

namespace NES
{

EmbeddedWorkerQuerySubmissionBackend::EmbeddedWorkerQuerySubmissionBackend(
    WorkerConfig config, SingleNodeWorkerConfiguration workerConfiguration)
    : channel(std::make_unique<detail::Channel>(std::move(config), std::move(workerConfiguration)))
{
}

EmbeddedWorkerQuerySubmissionBackend::~EmbeddedWorkerQuerySubmissionBackend() = default;

std::expected<QueryId, Exception> EmbeddedWorkerQuerySubmissionBackend::start(LogicalPlan plan)
{
    return start(std::move(plan), std::chrono::steady_clock::time_point::max(), {});
}

std::expected<QueryId, Exception> EmbeddedWorkerQuerySubmissionBackend::start(
    LogicalPlan plan, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    return channel->start(std::move(plan), deadline, stopToken);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::stop(QueryId queryId)
{
    return stop(std::move(queryId), std::chrono::steady_clock::time_point::max(), {});
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::stop(
    QueryId queryId, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    return channel->stop(std::move(queryId), deadline, stopToken);
}

std::expected<LocalQueryStatusSnapshot, Exception> EmbeddedWorkerQuerySubmissionBackend::status(QueryId queryId) const
{
    return status(std::move(queryId), std::chrono::steady_clock::time_point::max(), {});
}

std::expected<LocalQueryStatusSnapshot, Exception> EmbeddedWorkerQuerySubmissionBackend::status(
    QueryId queryId, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken) const
{
    return channel->status(std::move(queryId), deadline, stopToken);
}

std::expected<WorkerStatus, Exception> EmbeddedWorkerQuerySubmissionBackend::workerStatus(std::chrono::system_clock::time_point after) const
{
    return channel->workerStatus(after);
}

void EmbeddedWorkerQuerySubmissionBackend::shutdown(const std::chrono::steady_clock::time_point deadline)
{
    if (!channel->shutdown(deadline))
    {
        throw QueryStopFailed("Embedded worker shutdown reached its deadline");
    }
}

BackendProvider createEmbeddedBackend(const SingleNodeWorkerConfiguration& workerConfiguration)
{
    return [workerConfiguration](const WorkerConfig& config) /// NOLINT(bugprone-exception-escape)
    { return std::make_unique<EmbeddedWorkerQuerySubmissionBackend>(config, workerConfiguration); };
}

}
