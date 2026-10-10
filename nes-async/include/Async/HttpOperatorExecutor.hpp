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
#include <exception>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Http/HttpBackend.hpp>
#include <Http/HttpEventLoop.hpp>

namespace NES
{

namespace detail
{

/// Why the request produced no answer, as something the framework can rethrow on the source thread.
/// A transport that gave up after its retries will not do better for the next record, so the query
/// fails rather than running on with a hole in it.
[[nodiscard]] std::exception_ptr requestFailure(std::string_view url, const HttpError& error, std::string_view executorName);

/// The request skeleton the configuration describes: everything that does not depend on the
/// records. Reads `url` (required), `timeout_seconds`, `connect_timeout_ms` and `max_retries`, and
/// turns `api_key_env` into an `Authorization: Bearer` header by reading that environment variable
/// on this worker — the configuration travels from the coordinator, so it names the variable and
/// never the secret.
[[nodiscard]] HttpRequest httpRequestFromConfig(const AsyncOperatorContext& context, std::string_view executorName);

}

/// Base for an asynchronous operator whose work is one HTTP request per batch.
///
/// It owns an event loop, so a call in flight costs no thread: the framework submits up to
/// `maxConcurrency` batches and this class keeps them all on one loop. What is left for the
/// operator is the protocol — how records become a request, and how a response becomes fields.
///
/// `Scratch` is whatever the operator needs to carry from building the request to decoding the
/// response, for instance the row payloads a prompt was built from. It is kept for the duration of
/// the call and handed back to `decode`, so an operator needs no state of its own per call. Use the
/// default when there is nothing to carry.
///
/// `decode` runs on a loop thread, which is also the thread that drives the other requests'
/// completions. Decoding that costs real time therefore delays them; that is what a second loop
/// thread is for.
template <typename Scratch = std::monostate>
class HttpOperatorExecutor : public EventLoopAsyncExecutor
{
public:
    void submit(std::span<const AsyncRecordView> batch, AsyncBatchCompletion done) final;

protected:
    /// `executorName` appears in configuration and failure messages. `loopThreads` is how many
    /// threads run the loop; one keeps any number of requests in flight, and a second only helps
    /// when `decode` is expensive.
    explicit HttpOperatorExecutor(const std::string_view executorName, const size_t loopThreads = 1)
        : executorName(executorName), eventLoop(loopThreads)
    {
    }

    /// Called on whichever thread the framework submits from. Must return promptly.
    virtual std::pair<HttpRequest, Scratch> buildRequest(std::span<const AsyncRecordView> batch) = 0;

    /// One result per input record, in input order. A response the operator cannot read is its own
    /// business: return empty results and the framework leaves the output fields at their default,
    /// or throw to fail the query.
    virtual std::vector<AsyncRecordResult>
    decode(const HttpResponse& response, const Scratch& scratch, std::span<const AsyncRecordView> batch) = 0;

    /// For an operator that needs the loop itself, for instance to wait without a thread.
    [[nodiscard]] HttpEventLoop& loop() { return eventLoop; }

private:
    /// Owned rather than viewed: a subclass may well pass a temporary.
    std::string executorName;
    /// Declared after everything a completion touches, so it is stopped first on destruction and no
    /// completion can run into a half-destroyed subclass.
    HttpEventLoop eventLoop;
};

template <typename Scratch>
void HttpOperatorExecutor<Scratch>::submit(const std::span<const AsyncRecordView> batch, AsyncBatchCompletion done)
{
    if (batch.empty())
    {
        done(std::vector<AsyncRecordResult>{});
        return;
    }

    try
    {
        auto [request, scratch] = buildRequest(batch);
        const auto url = request.url;
        eventLoop.send(
            std::move(request),
            [this, batch, url, scratch = std::move(scratch), done = std::move(done)](std::expected<HttpResponse, HttpError> outcome) mutable
            {
                if (!outcome.has_value())
                {
                    done(std::unexpected{detail::requestFailure(url, outcome.error(), executorName)});
                    return;
                }
                try
                {
                    done(decode(outcome.value(), scratch, batch));
                }
                catch (...)
                {
                    done(std::unexpected{std::current_exception()});
                }
            });
    }
    catch (...)
    {
        done(std::unexpected{std::current_exception()});
    }
}

}
