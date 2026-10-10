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

#include <Http/HttpEventLoop.hpp>

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <curl/curl.h>
#include <curl/multi.h>
#include <fmt/format.h>

#include <Http/HttpBackend.hpp>
#include <Http/HttpLatencyStats.hpp>
#include <CurlGlobalInit.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

size_t appendToString(char* data, const size_t size, const size_t nmemb, void* userData)
{
    static_cast<std::string*>(userData)->append(data, size * nmemb);
    return size * nmemb;
}

/// The policy the blocking client applies, so the two remain comparable: a transport failure, 429
/// and 5xx are worth another attempt, any other 4xx is the request's own fault.
bool isRetryable(const CURLcode result, const long status)
{
    return result != CURLE_OK || status == 429 || (status >= 500 && status < 600);
}

std::chrono::milliseconds backoffFor(const size_t attempt)
{
    return std::chrono::milliseconds(100) * (1U << attempt);
}

/// One request, from the first attempt to the final completion. Held by shared_ptr because Asio
/// handlers have to be copyable, and because its address has to stay put: libcurl keeps pointers
/// into `body` and into the request's own strings for the duration of a transfer.
struct Transfer
{
    CURL* easy = nullptr;
    curl_slist* headers = nullptr;
    HttpRequest request;
    HttpCompletion done;
    std::string body;
    size_t attempt = 0;
    /// Taken before the first attempt, so the recorded latency spans the retries — the wait the
    /// caller actually experiences.
    std::chrono::steady_clock::time_point startedAt;

    Transfer() = default;
    Transfer(const Transfer&) = delete;
    Transfer& operator=(const Transfer&) = delete;
    Transfer(Transfer&&) = delete;
    Transfer& operator=(Transfer&&) = delete;

    ~Transfer()
    {
        curl_slist_free_all(headers);
        if (easy != nullptr)
        {
            curl_easy_cleanup(easy);
        }
    }
};

using TransferHandle = std::shared_ptr<Transfer>;

}

struct HttpEventLoop::State
{
    boost::asio::io_context ioContext;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work;
    /// Everything that touches the multi handle runs here. curl_multi is not thread-safe, and with
    /// more than one thread this is what keeps that true while the completions still run freely.
    boost::asio::strand<boost::asio::io_context::executor_type> strand;
    boost::asio::steady_timer curlTimer;
    std::vector<std::thread> threads;
    CURLM* multi = nullptr;
    /// Sockets curl asked us to watch. The descriptor is released and never closed: the socket
    /// belongs to curl, which keeps it for the next request to the same host.
    std::unordered_map<curl_socket_t, std::unique_ptr<boost::asio::posix::stream_descriptor>> sockets;
    std::unordered_map<CURL*, TransferHandle> transfers;
    std::atomic<size_t> inFlightCount{0};
    std::atomic<bool> stopping{false};

    explicit State(const size_t threadCount)
        : work(boost::asio::make_work_guard(ioContext)), strand(boost::asio::make_strand(ioContext)), curlTimer(ioContext)
    {
        ensureCurlGlobalInit();
        multi = curl_multi_init();
        if (multi == nullptr)
        {
            throw CannotCreateHttpClient("curl_multi_init failed");
        }
        curl_multi_setopt(multi, CURLMOPT_SOCKETFUNCTION, &State::onSocket);
        curl_multi_setopt(multi, CURLMOPT_SOCKETDATA, this);
        curl_multi_setopt(multi, CURLMOPT_TIMERFUNCTION, &State::onTimer);
        curl_multi_setopt(multi, CURLMOPT_TIMERDATA, this);

        threads.reserve(threadCount);
        for (size_t thread = 0; thread < threadCount; ++thread)
        {
            threads.emplace_back(
                [this, thread]
                {
                    /// Named so that a thread dump shows what this process is actually spending
                    /// threads on — which is the whole question this loop exists to answer.
                    pthread_setname_np(pthread_self(), fmt::format("HttpLoop-{}", thread).c_str());
                    ioContext.run();
                });
        }
    }

    /// curl tells us which socket it wants watched for what. Only ever called from the strand,
    /// because that is the only place the multi handle is driven from.
    static int onSocket(CURL* /*easy*/, const curl_socket_t fd, const int what, void* userp, void* /*socketp*/)
    {
        auto* const self = static_cast<State*>(userp);
        if (what == CURL_POLL_REMOVE)
        {
            self->forget(fd);
            return 0;
        }
        self->watch(fd, what);
        return 0;
    }

    /// curl tells us when it next wants to be driven, in milliseconds, or -1 for "not at all".
    static int onTimer(CURLM* /*multi*/, const long timeoutMs, void* userp)
    {
        auto* const self = static_cast<State*>(userp);
        self->curlTimer.cancel();
        if (timeoutMs < 0)
        {
            return 0;
        }
        self->curlTimer.expires_after(std::chrono::milliseconds(timeoutMs));
        self->curlTimer.async_wait(boost::asio::bind_executor(
            self->strand,
            [self](const boost::system::error_code& error)
            {
                if (!error)
                {
                    self->drive(CURL_SOCKET_TIMEOUT, 0);
                }
            }));
        return 0;
    }

    void watch(const curl_socket_t fd, const int what)
    {
        auto& descriptor = sockets[fd];
        if (descriptor == nullptr)
        {
            descriptor = std::make_unique<boost::asio::posix::stream_descriptor>(ioContext, fd);
        }
        /// One wait per direction, re-armed in its own handler. Cancelling first drops a wait for a
        /// direction curl no longer cares about; its handler then returns on operation_aborted.
        boost::system::error_code ignored;
        descriptor->cancel(ignored);
        if ((what & CURL_POLL_IN) != 0)
        {
            arm(fd, boost::asio::posix::stream_descriptor::wait_read, CURL_CSELECT_IN);
        }
        if ((what & CURL_POLL_OUT) != 0)
        {
            arm(fd, boost::asio::posix::stream_descriptor::wait_write, CURL_CSELECT_OUT);
        }
    }

    void arm(const curl_socket_t fd, const boost::asio::posix::stream_descriptor::wait_type type, const int event)
    {
        const auto it = sockets.find(fd);
        if (it == sockets.end())
        {
            return;
        }
        it->second->async_wait(
            type,
            boost::asio::bind_executor(
                strand,
                [this, fd, type, event](const boost::system::error_code& error)
                {
                    if (error || stopping.load())
                    {
                        /// Cancelled, or the socket is gone. curl asks again if it still cares.
                        return;
                    }
                    drive(fd, event);
                    /// `drive` may have removed the socket, in which case curl is done with it.
                    if (sockets.contains(fd))
                    {
                        arm(fd, type, event);
                    }
                }));
    }

    void forget(const curl_socket_t fd)
    {
        const auto it = sockets.find(fd);
        if (it == sockets.end())
        {
            return;
        }
        boost::system::error_code ignored;
        it->second->cancel(ignored);
        /// Released, not closed: the socket is curl's, and closing it here would break keep-alive
        /// and leave curl with a dangling descriptor.
        it->second->release();
        sockets.erase(it);
    }

    /// Hands one event to curl, then collects whatever finished because of it.
    void drive(const curl_socket_t fd, const int event)
    {
        int running = 0;
        curl_multi_socket_action(multi, fd, event, &running);
        collectFinished();
    }

    void collectFinished()
    {
        int remaining = 0;
        while (CURLMsg* const message = curl_multi_info_read(multi, &remaining))
        {
            if (message->msg != CURLMSG_DONE)
            {
                continue;
            }
            CURL* const easy = message->easy_handle;
            const CURLcode result = message->data.result;

            long status = 0;
            if (result == CURLE_OK)
            {
                curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
            }
            curl_multi_remove_handle(multi, easy);

            const auto it = transfers.find(easy);
            if (it == transfers.end())
            {
                continue;
            }
            auto transfer = it->second;
            transfers.erase(it);

            if (isRetryable(result, status) && transfer->attempt < transfer->request.maxRetries)
            {
                retryLater(std::move(transfer));
                continue;
            }
            finish(std::move(transfer), result, status);
        }
    }

    void retryLater(TransferHandle transfer)
    {
        const auto delay = backoffFor(transfer->attempt);
        transfer->attempt += 1;
        auto timer = std::make_shared<boost::asio::steady_timer>(ioContext);
        timer->expires_after(delay);
        timer->async_wait(boost::asio::bind_executor(
            strand,
            [this, timer, transfer = std::move(transfer)](const boost::system::error_code& error) mutable
            {
                if (error || stopping.load())
                {
                    finish(std::move(transfer), CURLE_ABORTED_BY_CALLBACK, 0);
                    return;
                }
                transfer->body.clear();
                start(std::move(transfer));
            }));
    }

    void finish(TransferHandle transfer, const CURLcode result, const long status)
    {
        const auto elapsed = std::chrono::steady_clock::now() - transfer->startedAt;
        const bool failed = result != CURLE_OK || status < 200 || status >= 300;
        HttpLatencyStats::instance().record(elapsed, failed);

        std::expected<HttpResponse, HttpError> outcome = HttpResponse{.status = status, .body = std::move(transfer->body)};
        if (result != CURLE_OK)
        {
            outcome = std::unexpected{
                HttpError{.message = fmt::format("request to {} failed: {}", transfer->request.url, curl_easy_strerror(result))}};
        }

        inFlightCount.fetch_sub(1);
        /// Posted off the strand rather than called here: a completion may cost real time — parsing
        /// an answer, say — and the strand has to stay free to keep the other requests moving. It is
        /// also what makes a second thread worth having.
        boost::asio::post(
            ioContext, [transfer = std::move(transfer), outcome = std::move(outcome)]() mutable { transfer->done(std::move(outcome)); });
    }

    /// Puts a prepared transfer onto the multi handle. On the strand.
    void start(TransferHandle transfer)
    {
        CURL* const easy = transfer->easy;
        curl_easy_setopt(easy, CURLOPT_WRITEDATA, &transfer->body);
        transfers.emplace(easy, std::move(transfer));
        curl_multi_add_handle(multi, easy);
    }
};

HttpEventLoop::HttpEventLoop(const size_t threads) : state(std::make_unique<State>(threads == 0 ? 1 : threads))
{
}

HttpEventLoop::~HttpEventLoop()
{
    stop();
}

void HttpEventLoop::send(HttpRequest request, HttpCompletion done)
{
    if (state->stopping.load())
    {
        done(std::unexpected{HttpError{.message = "the HTTP event loop was stopped"}});
        return;
    }

    auto transfer = std::make_shared<Transfer>();
    transfer->request = std::move(request);
    transfer->done = std::move(done);
    transfer->startedAt = std::chrono::steady_clock::now();
    /// A fresh easy handle per request, unlike the blocking client which reuses one. Connections
    /// are not lost by that: the multi handle owns the connection cache and every easy handle added
    /// to it shares that cache, so keep-alive works across requests.
    transfer->easy = curl_easy_init();
    if (transfer->easy == nullptr)
    {
        throw CannotCreateHttpClient("curl_easy_init failed");
    }

    const auto& prepared = transfer->request;
    for (const auto& [name, value] : prepared.headers)
    {
        transfer->headers = curl_slist_append(transfer->headers, fmt::format("{}: {}", name, value).c_str());
    }
    /// Suppressed for the same reason as in the blocking client: a minimal server need not
    /// implement the 100-continue handshake.
    transfer->headers = curl_slist_append(transfer->headers, "Expect:");

    CURL* const easy = transfer->easy;
    curl_easy_setopt(easy, CURLOPT_URL, prepared.url.c_str());
    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, transfer->headers);
    if (prepared.method == HttpMethod::POST)
    {
        curl_easy_setopt(easy, CURLOPT_POST, 1L);
        curl_easy_setopt(easy, CURLOPT_POSTFIELDS, prepared.body.c_str());
        curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(prepared.body.size()));
    }
    else
    {
        curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
    }
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, appendToString);
    /// Mandatory off the main thread: without it libcurl uses SIGALRM for DNS timeouts.
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, static_cast<long>(prepared.timeout.count()));
    curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(prepared.connectTimeout.count()));

    state->inFlightCount.fetch_add(1);
    boost::asio::post(state->strand, [this, transfer = std::move(transfer)]() mutable { state->start(std::move(transfer)); });
}

void HttpEventLoop::after(const std::chrono::milliseconds delay, std::move_only_function<void()> work)
{
    if (state->stopping.load())
    {
        return;
    }
    auto timer = std::make_shared<boost::asio::steady_timer>(state->ioContext);
    timer->expires_after(delay);
    /// The work is move-only and an Asio handler has to be copyable, so it travels in a shared_ptr.
    auto held = std::make_shared<std::move_only_function<void()>>(std::move(work));
    timer->async_wait(
        [timer, held](const boost::system::error_code& error)
        {
            if (!error)
            {
                (*held)();
            }
        });
}

void HttpEventLoop::stop()
{
    if (state->stopping.exchange(true))
    {
        return;
    }

    boost::asio::post(
        state->strand,
        [this]
        {
            boost::system::error_code ignored;
            state->curlTimer.cancel();
            for (auto& [fd, descriptor] : state->sockets)
            {
                descriptor->cancel(ignored);
                descriptor->release();
            }
            state->sockets.clear();

            /// Everything in flight is abandoned, and every caller hears about it: a completion
            /// that never runs is a caller that waits forever.
            auto abandoned = std::move(state->transfers);
            state->transfers.clear();
            for (auto& [easy, transfer] : abandoned)
            {
                curl_multi_remove_handle(state->multi, easy);
                state->inFlightCount.fetch_sub(1);
                transfer->done(std::unexpected{HttpError{.message = "the HTTP event loop was stopped"}});
            }
        });

    /// The posted handler above is work, so it runs before the threads leave.
    state->work.reset();
    for (auto& thread : state->threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    state->threads.clear();
    if (state->multi != nullptr)
    {
        curl_multi_cleanup(state->multi);
        state->multi = nullptr;
    }
}

size_t HttpEventLoop::inFlight() const
{
    return state->inFlightCount.load();
}

}
