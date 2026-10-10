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

#include <Http/CurlHttpBackend.hpp>

#include <chrono>
#include <cstddef>
#include <istream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <Http/HttpBackend.hpp>
#include <Http/HttpLatencyStats.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <boost/asio.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

namespace
{

/// Reads one HTTP/1.1 request (headers plus Content-Length body) and returns it verbatim.
std::string readHttpRequest(boost::asio::ip::tcp::socket& socket)
{
    boost::asio::streambuf buffer;
    const auto headerBytes = boost::asio::read_until(socket, buffer, "\r\n\r\n");
    std::string request{
        boost::asio::buffers_begin(buffer.data()), boost::asio::buffers_begin(buffer.data()) + static_cast<std::ptrdiff_t>(headerBytes)};
    buffer.consume(headerBytes);

    size_t contentLength = 0;
    constexpr std::string_view ContentLength = "Content-Length:";
    if (const auto position = request.find(ContentLength); position != std::string::npos)
    {
        contentLength = std::stoul(request.substr(position + ContentLength.size()));
    }
    if (buffer.size() < contentLength)
    {
        boost::asio::read(socket, buffer, boost::asio::transfer_exactly(contentLength - buffer.size()));
    }
    request.append(
        boost::asio::buffers_begin(buffer.data()), boost::asio::buffers_begin(buffer.data()) + static_cast<std::ptrdiff_t>(contentLength));
    return request;
}

/// Serves one scripted response per connection, in order, and records every request it read. A
/// response of std::nullopt reads the request and never answers, which exercises the timeout.
class ScriptedHttpServer
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor acceptor;
    std::vector<std::string> requests;
    std::thread serverThread;

public:
    explicit ScriptedHttpServer(std::vector<std::optional<std::pair<int, std::string>>> script)
        : acceptor(ioContext, boost::asio::ip::tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0))
    {
        serverThread = std::thread(
            [this, script = std::move(script)]
            {
                for (const auto& response : script)
                {
                    boost::asio::ip::tcp::socket socket(ioContext);
                    acceptor.accept(socket);
                    requests.push_back(readHttpRequest(socket));
                    if (!response.has_value())
                    {
                        /// Hold the connection open past the client's timeout, then drop it.
                        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
                        continue;
                    }
                    const auto& [status, body] = *response;
                    boost::asio::write(
                        socket,
                        boost::asio::buffer(fmt::format(
                            "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            status,
                            status == 200 ? "OK" : "Error",
                            body.size(),
                            body)));
                }
            });
    }

    ScriptedHttpServer(const ScriptedHttpServer&) = delete;
    ScriptedHttpServer& operator=(const ScriptedHttpServer&) = delete;
    ScriptedHttpServer(ScriptedHttpServer&&) = delete;
    ScriptedHttpServer& operator=(ScriptedHttpServer&&) = delete;

    ~ScriptedHttpServer()
    {
        if (serverThread.joinable())
        {
            serverThread.join();
        }
    }

    [[nodiscard]] std::string url(const std::string_view path) const
    {
        return fmt::format("http://127.0.0.1:{}{}", acceptor.local_endpoint().port(), path);
    }

    /// Valid once the destructor or all scripted exchanges have run.
    [[nodiscard]] const std::vector<std::string>& receivedRequests()
    {
        if (serverThread.joinable())
        {
            serverThread.join();
        }
        return requests;
    }
};

HttpRequest post(std::string url, std::string body, const size_t maxRetries = 0)
{
    return HttpRequest{
        .url = std::move(url),
        .method = HttpMethod::POST,
        .headers = {},
        .body = std::move(body),
        .timeout = std::chrono::seconds(1),
        .connectTimeout = std::chrono::milliseconds(500),
        .maxRetries = maxRetries};
}

std::string_view bodyOf(const std::string& request)
{
    const auto separator = request.find("\r\n\r\n");
    return separator == std::string::npos ? std::string_view{} : std::string_view{request}.substr(separator + 4);
}

}

class CurlHttpBackendTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("CurlHttpBackendTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(CurlHttpBackendTest, PostsTheBodyAndReturnsTheResponse)
{
    ScriptedHttpServer server({std::make_pair(200, std::string{"PONG"})});
    CurlHttpBackend backend;

    const auto response = backend.send(post(server.url("/echo"), "ping"));
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(response->status, 200);
    EXPECT_EQ(response->body, "PONG");

    const auto& requests = server.receivedRequests();
    ASSERT_EQ(requests.size(), 1);
    EXPECT_TRUE(requests.front().starts_with("POST /echo ")) << requests.front();
    EXPECT_EQ(bodyOf(requests.front()), "ping");
}

TEST_F(CurlHttpBackendTest, SendsTheHeadersItIsGivenAndNothingElse)
{
    ScriptedHttpServer server({std::make_pair(200, std::string{"x"})});
    CurlHttpBackend backend;

    auto request = post(server.url("/thing"), "{}");
    request.headers = {{"Content-Type", "application/json"}, {"Authorization", "Bearer secret-key"}};
    ASSERT_TRUE(backend.send(request).has_value());

    const auto& received = server.receivedRequests().front();
    EXPECT_NE(received.find("Content-Type: application/json\r\n"), std::string::npos) << received;
    EXPECT_NE(received.find("Authorization: Bearer secret-key\r\n"), std::string::npos) << received;
    /// The transport adds no authentication of its own.
    EXPECT_EQ(received.find("Authorization: Bearer secret-key\r\n"), received.rfind("Authorization: Bearer secret-key\r\n"));
    /// Suppressed, because a minimal server need not implement the handshake.
    EXPECT_EQ(received.find("Expect: 100-continue"), std::string::npos) << received;
}

TEST_F(CurlHttpBackendTest, GetSendsNoBody)
{
    ScriptedHttpServer server({std::make_pair(200, std::string{"value"})});
    CurlHttpBackend backend;

    auto request = post(server.url("/thing"), "ignored");
    request.method = HttpMethod::GET;
    const auto response = backend.send(request);
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(response->body, "value");

    const auto& received = server.receivedRequests().front();
    EXPECT_TRUE(received.starts_with("GET /thing ")) << received;
    EXPECT_TRUE(bodyOf(received).empty()) << received;
}

/// A non-2xx answer is an answer: the transport hands it back and lets the caller judge it, because
/// what counts as a failure depends on the protocol.
TEST_F(CurlHttpBackendTest, ReportsANonSuccessStatusAsAResponse)
{
    ScriptedHttpServer server({std::make_pair(404, std::string{"no such thing"})});
    CurlHttpBackend backend;

    const auto response = backend.send(post(server.url("/missing"), "x"));
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(response->status, 404);
    EXPECT_EQ(response->body, "no such thing");
}

TEST_F(CurlHttpBackendTest, RetriesServerErrorsThenSucceeds)
{
    ScriptedHttpServer server(
        {std::make_pair(503, std::string{}), std::make_pair(429, std::string{}), std::make_pair(200, std::string{"ok"})});
    CurlHttpBackend backend;

    const auto response = backend.send(post(server.url("/flaky"), "x", 2));
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(response->status, 200);
    EXPECT_EQ(response->body, "ok");
    EXPECT_EQ(server.receivedRequests().size(), 3);
}

TEST_F(CurlHttpBackendTest, GivesUpAfterMaxRetriesAndReturnsTheLastStatus)
{
    ScriptedHttpServer server({std::make_pair(500, std::string{}), std::make_pair(500, std::string{})});
    CurlHttpBackend backend;

    const auto response = backend.send(post(server.url("/broken"), "x", 1));
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(response->status, 500);
    EXPECT_EQ(server.receivedRequests().size(), 2);
}

/// Only one response is scripted: a retried 4xx would block on a second connection nobody accepts.
TEST_F(CurlHttpBackendTest, DoesNotRetryClientErrors)
{
    ScriptedHttpServer server({std::make_pair(400, std::string{"bad request"})});
    CurlHttpBackend backend;

    const auto response = backend.send(post(server.url("/bad"), "x", 2));
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(response->status, 400);
    EXPECT_EQ(server.receivedRequests().size(), 1);
}

TEST_F(CurlHttpBackendTest, ReportsAnUnreachableEndpointAsAnError)
{
    /// Nothing listens on port 1 of the loopback interface: the connection is refused immediately.
    CurlHttpBackend backend;
    const auto response = backend.send(post("http://127.0.0.1:1/thing", "x"));
    ASSERT_FALSE(response.has_value());
    EXPECT_NE(response.error().message.find("127.0.0.1:1"), std::string::npos) << response.error().message;
}

/// A stalled endpoint must not park the calling thread beyond the request timeout.
TEST_F(CurlHttpBackendTest, TimesOutAgainstStalledEndpoint)
{
    ScriptedHttpServer server({std::nullopt});
    CurlHttpBackend backend;

    const auto start = std::chrono::steady_clock::now();
    const auto response = backend.send(post(server.url("/stalled"), "x"));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_FALSE(response.has_value());
    EXPECT_LT(elapsed, std::chrono::milliseconds(1400));
}

TEST_F(CurlHttpBackendTest, ReusesOneHandleAcrossRequests)
{
    ScriptedHttpServer server({std::make_pair(200, std::string{"a"}), std::make_pair(200, std::string{"b"})});
    CurlHttpBackend backend;

    const auto first = backend.send(post(server.url("/one"), "1"));
    const auto second = backend.send(post(server.url("/two"), "2"));
    ASSERT_TRUE(first.has_value()) << first.error().message;
    ASSERT_TRUE(second.has_value()) << second.error().message;
    EXPECT_EQ(first->body, "a");
    EXPECT_EQ(second->body, "b");

    /// The second request must not inherit the first one's URL or body: curl_easy_reset runs
    /// before every exchange.
    const auto& requests = server.receivedRequests();
    ASSERT_EQ(requests.size(), 2);
    EXPECT_TRUE(requests[0].starts_with("POST /one ")) << requests[0];
    EXPECT_TRUE(requests[1].starts_with("POST /two ")) << requests[1];
    EXPECT_EQ(bodyOf(requests[1]), "2");
}

TEST_F(CurlHttpBackendTest, RecordsTheRoundTripIncludingRetries)
{
    HttpLatencyStats::instance().reset();
    ScriptedHttpServer server({std::make_pair(503, std::string{}), std::make_pair(200, std::string{"ok"})});
    CurlHttpBackend backend;

    ASSERT_TRUE(backend.send(post(server.url("/flaky"), "x", 1)).has_value());

    const auto summary = HttpLatencyStats::instance().summarize();
    ASSERT_TRUE(summary.has_value());
    /// One recorded call for the whole exchange, retry included -- that is the wait the caller saw.
    EXPECT_EQ(summary->calls, 1);
    EXPECT_EQ(summary->failures, 0);
    /// The backoff after the 503 is 100 ms, so the round trip cannot have been instant.
    EXPECT_GE(summary->maxMs, 100);
    HttpLatencyStats::instance().reset();
}

}
