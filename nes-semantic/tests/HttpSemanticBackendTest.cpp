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

#include <HttpSemanticBackend.hpp>

#include <chrono>
#include <cstddef>
#include <istream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <boost/asio.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <BaseUnitTest.hpp>
#include <SemanticBackend.hpp>

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
                            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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

    [[nodiscard]] std::string endpoint() const { return fmt::format("http://127.0.0.1:{}/v1", acceptor.local_endpoint().port()); }

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

std::string chatCompletion(const std::string& content)
{
    return nlohmann::json{{"choices", nlohmann::json::array({{{"message", {{"role", "assistant"}, {"content", content}}}}})}}.dump();
}

CompletionRequest request(size_t maxRetries = 0)
{
    return CompletionRequest{
        .prompt = "Classify: \"great\"",
        .modelName = "llama3.1:8b",
        .timeout = std::chrono::seconds(1),
        .connectTimeout = std::chrono::milliseconds(500),
        .maxRetries = maxRetries};
}

}

class HttpSemanticBackendTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("HttpSemanticBackendTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(HttpSemanticBackendTest, ReturnsMessageContentAndSendsChatCompletion)
{
    ScriptedHttpServer server({std::make_pair(200, chatCompletion(R"({"row1": {"S": {"answer": "POSITIVE"}}})"))});
    HttpSemanticBackend backend{server.endpoint(), std::string{"secret-key"}};

    const auto response = backend.complete(request());
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(*response, R"({"row1": {"S": {"answer": "POSITIVE"}}})");

    const auto& requests = server.receivedRequests();
    ASSERT_EQ(requests.size(), 1);
    EXPECT_TRUE(requests.front().starts_with("POST /v1/chat/completions ")) << requests.front();
    EXPECT_NE(requests.front().find("Authorization: Bearer secret-key\r\n"), std::string::npos);
    const auto body = nlohmann::json::parse(requests.front().substr(requests.front().find("\r\n\r\n") + 4));
    EXPECT_EQ(body["model"], "llama3.1:8b");
    ASSERT_EQ(body["messages"].size(), 1);
    EXPECT_EQ(body["messages"][0]["role"], "user");
    EXPECT_EQ(body["messages"][0]["content"], "Classify: \"great\"");
}

TEST_F(HttpSemanticBackendTest, SendsNoAuthorizationWithoutKey)
{
    ScriptedHttpServer server({std::make_pair(200, chatCompletion("x"))});
    HttpSemanticBackend backend{server.endpoint(), std::nullopt};
    ASSERT_TRUE(backend.complete(request()).has_value());
    EXPECT_EQ(server.receivedRequests().front().find("Authorization"), std::string::npos);
}

TEST_F(HttpSemanticBackendTest, RetriesServerErrorsThenSucceeds)
{
    ScriptedHttpServer server(
        {std::make_pair(503, std::string{}), std::make_pair(429, std::string{}), std::make_pair(200, chatCompletion("ok"))});
    HttpSemanticBackend backend{server.endpoint(), std::nullopt};

    const auto response = backend.complete(request(2));
    ASSERT_TRUE(response.has_value()) << response.error().message;
    EXPECT_EQ(*response, "ok");
    EXPECT_EQ(server.receivedRequests().size(), 3);
}

TEST_F(HttpSemanticBackendTest, GivesUpAfterMaxRetries)
{
    ScriptedHttpServer server({std::make_pair(500, std::string{}), std::make_pair(500, std::string{})});
    HttpSemanticBackend backend{server.endpoint(), std::nullopt};

    const auto response = backend.complete(request(1));
    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error().kind, BackendError::Kind::HTTP_STATUS);
    EXPECT_NE(response.error().message.find("500"), std::string::npos);
}

/// Only one response is scripted: a retried 4xx would block on a second connection nobody accepts.
TEST_F(HttpSemanticBackendTest, DoesNotRetryClientErrors)
{
    ScriptedHttpServer server({std::make_pair(400, std::string{"bad request"})});
    HttpSemanticBackend backend{server.endpoint(), std::nullopt};

    const auto response = backend.complete(request(2));
    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error().kind, BackendError::Kind::HTTP_STATUS);
    EXPECT_EQ(server.receivedRequests().size(), 1);
}

TEST_F(HttpSemanticBackendTest, ReportsMalformedEnvelopes)
{
    for (const auto* body : {"not json", R"({"choices": []})", R"({"choices": [{"message": {"content": 3}}]})", R"([1, 2])"})
    {
        ScriptedHttpServer server({std::make_pair(200, std::string{body})});
        HttpSemanticBackend backend{server.endpoint(), std::nullopt};
        const auto response = backend.complete(request());
        ASSERT_FALSE(response.has_value()) << body;
        EXPECT_EQ(response.error().kind, BackendError::Kind::MALFORMED_RESPONSE) << body;
    }
}

TEST_F(HttpSemanticBackendTest, ReportsUnreachableEndpoint)
{
    /// Nothing listens on port 1 of the loopback interface: the connection is refused immediately.
    HttpSemanticBackend backend{"http://127.0.0.1:1/v1", std::nullopt};
    const auto response = backend.complete(request());
    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error().kind, BackendError::Kind::UNREACHABLE);
}

/// A stalled endpoint must not park the worker thread beyond the request timeout.
TEST_F(HttpSemanticBackendTest, TimesOutAgainstStalledEndpoint)
{
    ScriptedHttpServer server({std::nullopt});
    HttpSemanticBackend backend{server.endpoint(), std::nullopt};

    const auto start = std::chrono::steady_clock::now();
    const auto response = backend.complete(request());
    const auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error().kind, BackendError::Kind::UNREACHABLE);
    EXPECT_LT(elapsed, std::chrono::milliseconds(1400));
}

}
