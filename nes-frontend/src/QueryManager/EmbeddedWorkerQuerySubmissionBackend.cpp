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

#include <chrono>
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
#include <QueryStatus.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <WorkerStatus.hpp>

namespace NES::detail
{
namespace
{
struct Stop
{
};

struct RegisterQuery
{
    LogicalPlan plan;
};

struct StartQuery
{
    QueryId id;
};

struct StopQuery
{
    QueryId id;
};

struct QueryStatusQuery
{
    QueryId id;
};

struct WorkerStatusQuery
{
    std::chrono::system_clock::time_point after;
};

struct RegisterQueryReply
{
    std::expected<QueryId, Exception> reply;
};

struct StartQueryReply
{
    std::expected<void, Exception> reply;
};

struct StopQueryReply
{
    std::expected<void, Exception> reply;
};

struct QueryStatusReply
{
    std::expected<LocalQueryStatusSnapshot, Exception> reply;
};

struct WorkerStatusReply
{
    std::expected<WorkerStatus, Exception> reply;
};

using Request = std::variant<Stop, RegisterQuery, StartQuery, StopQuery, QueryStatusQuery, WorkerStatusQuery>;
/// `std::monostate` first so the variant is default-constructible — needed because
/// folly's `dequeue(T&)` writes into an existing slot. It is never actually pushed.
using Reply = std::variant<std::monostate, RegisterQueryReply, StartQueryReply, StopQueryReply, QueryStatusReply, WorkerStatusReply>;
}

/// All shared state between callers and the single worker thread lives here:
/// the request/reply queues, the mutex that serializes callers, the config,
/// and the worker thread itself. The header forward-declares this so the
/// backend is a thin pimpl wrapper.
class Channel
{
public:
    Channel(WorkerConfig cfg, SingleNodeWorkerConfiguration workerConfiguration)
        : config(std::move(cfg))
        , thread(
              "main",
              this->config.host,
              [&requests = this->requests, &replies = this->replies, config = this->config, workerConfiguration](std::stop_token stopToken)
              {
                  /// Start with the per-worker topology config, then overlay only
                  /// explicitly-set CLI values so that CLI args take highest priority
                  /// but topology values aren't clobbered by CLI defaults.
                  SingleNodeWorkerConfiguration mergedConfig = config.config;
                  mergedConfig.applyExplicitlySetFrom(workerConfiguration);

                  /// Set grpc/data from topology (these always come from cluster config)
                  mergedConfig.grpcAddressUri.setValue(config.host.getRawValue());
                  mergedConfig.dataAddress.setValue(config.dataAddress);
                  SingleNodeWorker worker(mergedConfig, config.host);

                  /// On stop, push a poison `Stop` request so the blocking dequeue below wakes up.
                  std::stop_callback poison(stopToken, [&]() { requests.enqueue(Request{Stop{}}); });

                  while (!stopToken.stop_requested())
                  {
                      Request request;
                      requests.dequeue(request);
                      if (std::holds_alternative<Stop>(request))
                      {
                          break;
                      }
                      Reply reply = std::visit(
                          Overloaded{
                              [](Stop) -> Reply { std::unreachable(); },
                              [&](const RegisterQuery& r) -> Reply { return RegisterQueryReply{.reply = worker.registerQuery(r.plan)}; },
                              [&](const StartQuery& r) -> Reply { return StartQueryReply{.reply = worker.startQuery(r.id)}; },
                              [&](const StopQuery& r) -> Reply { return StopQueryReply{.reply = worker.stopQuery(r.id)}; },
                              [&](const QueryStatusQuery& r) -> Reply { return QueryStatusReply{.reply = worker.getQueryStatus(r.id)}; },
                              [&](const WorkerStatusQuery& r) -> Reply
                              { return WorkerStatusReply{.reply = worker.getWorkerStatus(r.after)}; },
                          },
                          request);
                      replies.enqueue(std::move(reply));
                  }
                  /// `worker` is destroyed here, on the same thread it was constructed on.
              })
    {
    }

    std::expected<QueryId, Exception> registerQuery(LogicalPlan plan)
    {
        return submit<RegisterQueryReply>(RegisterQuery{.plan = std::move(plan)});
    }

    std::expected<void, Exception> start(QueryId id) { return submit<StartQueryReply>(StartQuery{.id = id}); }

    std::expected<void, Exception> stop(QueryId id) { return submit<StopQueryReply>(StopQuery{.id = id}); }

    std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId id) const
    {
        return submit<QueryStatusReply>(QueryStatusQuery{.id = id});
    }

    std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const
    {
        return submit<WorkerStatusReply>(WorkerStatusQuery{.after = after});
    }

private:
    /// Send a request to the worker thread, block until its reply lands, and
    /// extract the alternative the caller expects. The mutex guarantees only
    /// one (request, reply) pair is in flight at a time, so the reply we pop
    /// here is the one for the request we just enqueued.
    template <typename ReplyT, typename RequestT>
    decltype(ReplyT::reply) submit(RequestT request) const
    {
        std::lock_guard lock(submitMutex);
        requests.enqueue(Request{std::move(request)});
        Reply reply;
        replies.dequeue(reply);
        return std::move(std::get<ReplyT>(reply).reply);
    }

    /// `mutable` so const observers (`status`, `workerStatus`) can post requests.
    mutable folly::UMPSCQueue<Request, /*MayBlock*/ true> requests;
    mutable folly::USPSCQueue<Reply, /*MayBlock*/ true> replies;
    mutable std::mutex submitMutex;
    WorkerConfig config;
    /// Must be declared last: its body references the queues above, and on
    /// destruction the jthread requests stop and joins before earlier members go.
    Thread thread;
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

std::expected<QueryId, Exception> EmbeddedWorkerQuerySubmissionBackend::registerQuery(LogicalPlan plan)
{
    return channel->registerQuery(std::move(plan));
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::start(QueryId queryId)
{
    return channel->start(queryId);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::stop(QueryId queryId)
{
    return channel->stop(queryId);
}

std::expected<LocalQueryStatusSnapshot, Exception> EmbeddedWorkerQuerySubmissionBackend::status(QueryId queryId) const
{
    return channel->status(queryId);
}

std::expected<WorkerStatus, Exception> EmbeddedWorkerQuerySubmissionBackend::workerStatus(std::chrono::system_clock::time_point after) const
{
    return channel->workerStatus(after);
}

BackendProvider createEmbeddedBackend(const SingleNodeWorkerConfiguration& workerConfiguration)
{
    return [workerConfiguration](const WorkerConfig& config) /// NOLINT(bugprone-exception-escape)
    { return std::make_unique<EmbeddedWorkerQuerySubmissionBackend>(config, workerConfiguration); };
}

}
