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

#include <Http/DummyHttpServer.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <fmt/format.h>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

using boost::asio::ip::tcp;

struct ParsedRequest
{
    std::string path;
    std::string query;
    std::string body;
};

/// Reads one HTTP/1.1 request: the request line, the headers, and as many body bytes as
/// Content-Length announces.
ParsedRequest readRequest(tcp::socket& socket)
{
    boost::asio::streambuf buffer;
    const auto headerBytes = boost::asio::read_until(socket, buffer, "\r\n\r\n");
    std::string header{
        boost::asio::buffers_begin(buffer.data()), boost::asio::buffers_begin(buffer.data()) + static_cast<std::ptrdiff_t>(headerBytes)};
    buffer.consume(headerBytes);

    ParsedRequest request;
    const auto targetStart = header.find(' ');
    const auto targetEnd = header.find(' ', targetStart + 1);
    if (targetStart != std::string::npos && targetEnd != std::string::npos)
    {
        auto target = header.substr(targetStart + 1, targetEnd - targetStart - 1);
        if (const auto questionMark = target.find('?'); questionMark != std::string::npos)
        {
            request.query = target.substr(questionMark + 1);
            target.resize(questionMark);
        }
        request.path = std::move(target);
    }

    size_t contentLength = 0;
    constexpr std::string_view ContentLength = "Content-Length:";
    if (const auto position = header.find(ContentLength); position != std::string::npos)
    {
        const auto value = std::string_view{header}.substr(position + ContentLength.size());
        const auto digits = value.find_first_not_of(" \t");
        if (digits != std::string_view::npos)
        {
            std::from_chars(value.data() + digits, value.data() + value.size(), contentLength);
        }
    }
    if (buffer.size() < contentLength)
    {
        boost::asio::read(socket, buffer, boost::asio::transfer_exactly(contentLength - buffer.size()));
    }
    request.body.assign(
        boost::asio::buffers_begin(buffer.data()), boost::asio::buffers_begin(buffer.data()) + static_cast<std::ptrdiff_t>(contentLength));
    return request;
}

/// Value of `key` in a `a=1&b=2` query string, or nullopt.
std::optional<uint64_t> numericQueryParameter(const std::string_view query, const std::string_view key)
{
    for (size_t start = 0; start <= query.size();)
    {
        const auto end = std::min(query.find('&', start), query.size());
        const auto pair = query.substr(start, end - start);
        if (const auto equals = pair.find('='); equals != std::string_view::npos && pair.substr(0, equals) == key)
        {
            uint64_t value = 0;
            const auto digits = pair.substr(equals + 1);
            if (std::from_chars(digits.data(), digits.data() + digits.size(), value).ec == std::errc{})
            {
                return value;
            }
            return std::nullopt;
        }
        start = end + 1;
    }
    return std::nullopt;
}

void writeResponse(tcp::socket& socket, const int status, const std::string_view body)
{
    boost::asio::write(
        socket,
        boost::asio::buffer(fmt::format(
            "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            status == 200 ? "OK" : "Error",
            body.size(),
            body)));
}

}

struct DummyHttpServer::State
{
    boost::asio::io_context ioContext;
    std::optional<tcp::acceptor> acceptor;
    /// Runs the io_context, which is where connections are accepted.
    std::thread ioThread;
    std::mutex connectionsMutex;
    std::vector<std::thread> connections;
    std::atomic<size_t> requests{0};
    /// How many /flaky requests have already been answered with a 503.
    std::atomic<uint64_t> flakyFailures{0};
    std::atomic<bool> stopping{false};

    void serve(tcp::socket socket);
    void acceptNext();
};

void DummyHttpServer::State::serve(tcp::socket socket)
{
    try
    {
        const auto request = readRequest(socket);
        requests.fetch_add(1);

        if (request.path == "/echo")
        {
            writeResponse(socket, 200, request.body);
        }
        else if (request.path == "/upper")
        {
            auto body = request.body;
            std::ranges::transform(
                body, body.begin(), [](const unsigned char character) { return static_cast<char>(std::toupper(character)); });
            writeResponse(socket, 200, body);
        }
        else if (request.path == "/slow")
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(numericQueryParameter(request.query, "ms").value_or(0)));
            writeResponse(socket, 200, request.body);
        }
        else if (request.path == "/fail")
        {
            writeResponse(socket, 500, "always fails");
        }
        else if (request.path == "/status")
        {
            writeResponse(socket, static_cast<int>(numericQueryParameter(request.query, "code").value_or(200)), request.body);
        }
        else if (request.path == "/flaky")
        {
            const auto budget = numericQueryParameter(request.query, "n").value_or(1);
            if (flakyFailures.fetch_add(1) < budget)
            {
                writeResponse(socket, 503, "not yet");
            }
            else
            {
                writeResponse(socket, 200, request.body);
            }
        }
        else if (request.path == "/stall")
        {
            /// Reads the request and never answers, so the client has to hit its own timeout.
            /// Holding the connection is the point; the wait ends so the thread can be joined.
            std::this_thread::sleep_for(std::chrono::milliseconds(numericQueryParameter(request.query, "ms").value_or(5000)));
        }
        else
        {
            writeResponse(socket, 404, fmt::format("no route {}", request.path));
        }
    }
    catch (...)
    {
        /// A client that hung up mid-request is not this server's problem, and an exception
        /// escaping a thread would take the test process with it.
    }
}

/// Asynchronous rather than a blocking accept loop: a blocking `accept` does not return when
/// another thread closes the acceptor, so `stop` could never join its thread. Here `stop` posts the
/// close into the io_context, the pending accept completes with an error, no new one is armed, and
/// `run` returns because no work is left.
void DummyHttpServer::State::acceptNext()
{
    acceptor->async_accept(
        [this](const boost::system::error_code& error, tcp::socket socket)
        {
            if (error || stopping.load())
            {
                return;
            }
            /// One thread per connection: a slow route must not hold up the others, or a
            /// concurrency measurement against this server would only measure this server.
            auto connection = std::thread([this, socket = std::move(socket)]() mutable { serve(std::move(socket)); });
            {
                const std::lock_guard lock{connectionsMutex};
                connections.push_back(std::move(connection));
            }
            acceptNext();
        });
}

DummyHttpServer::DummyHttpServer() : state(std::make_unique<State>())
{
}

DummyHttpServer::~DummyHttpServer()
{
    stop();
}

void DummyHttpServer::start()
{
    state->acceptor.emplace(state->ioContext, tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0));
    state->acceptNext();
    state->ioThread = std::thread([this] { state->ioContext.run(); });
}

void DummyHttpServer::stop()
{
    if (state->stopping.exchange(true))
    {
        return;
    }
    if (state->acceptor.has_value())
    {
        boost::asio::post(
            state->ioContext,
            [this]
            {
                boost::system::error_code ignored;
                state->acceptor->close(ignored);
            });
    }
    if (state->ioThread.joinable())
    {
        state->ioThread.join();
    }
    const std::lock_guard lock{state->connectionsMutex};
    for (auto& connection : state->connections)
    {
        if (connection.joinable())
        {
            connection.join();
        }
    }
    state->connections.clear();
}

uint16_t DummyHttpServer::port() const
{
    PRECONDITION(state->acceptor.has_value(), "The dummy server has no port before start() was called");
    return state->acceptor->local_endpoint().port();
}

std::string DummyHttpServer::url(const std::string& path) const
{
    return fmt::format("http://127.0.0.1:{}{}", port(), path);
}

size_t DummyHttpServer::requestCount() const
{
    return state->requests.load();
}

}
