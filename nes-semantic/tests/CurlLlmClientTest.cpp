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

#include <CurlLlmClient.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SemanticModelConfig.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

namespace NES
{

namespace
{

/// Reads one HTTP/1.1 request (headers + body, using Content-Length) off `socket` and discards it
/// — every test server here only cares about replying, not about request content.
void readHttpRequest(boost::asio::ip::tcp::socket& socket)
{
    boost::asio::streambuf requestBuffer;
    boost::asio::read_until(socket, requestBuffer, "\r\n\r\n");

    std::istream requestStream(&requestBuffer);
    std::string headerLine;
    size_t contentLength = 0;
    while (std::getline(requestStream, headerLine) && headerLine != "\r")
    {
        constexpr std::string_view contentLengthPrefix = "Content-Length:";
        if (headerLine.starts_with(contentLengthPrefix))
        {
            contentLength = std::stoul(headerLine.substr(contentLengthPrefix.size()));
        }
    }
    /// Some of the body may already be sitting in requestBuffer from the read_until above.
    const size_t alreadyRead = requestBuffer.size();
    if (alreadyRead < contentLength)
    {
        boost::asio::read(socket, requestBuffer, boost::asio::transfer_exactly(contentLength - alreadyRead));
    }
}

/// Accepts exactly one HTTP/1.1 connection, reads the request (headers + body, using
/// Content-Length), and replies with `responseBody` as a 200 with `Content-Type: application/json`.
/// Runs on its own thread so the test's `CurlLlmClient::map()` call can block on it synchronously,
/// exactly like it would block on a real endpoint (plan §D6).
class OneShotHttpServer
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor acceptor;
    std::thread serverThread;

public:
    explicit OneShotHttpServer(std::string responseBody)
        : acceptor(ioContext, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0))
    {
        serverThread = std::thread(
            [this, responseBody = std::move(responseBody)]
            {
                boost::asio::ip::tcp::socket socket(ioContext);
                acceptor.accept(socket);
                readHttpRequest(socket);

                const std::string response = fmt::format(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    responseBody.size(),
                    responseBody);
                boost::asio::write(socket, boost::asio::buffer(response));
            });
    }

    [[nodiscard]] unsigned short port() const { return acceptor.local_endpoint().port(); }

    ~OneShotHttpServer()
    {
        if (serverThread.joinable())
        {
            serverThread.join();
        }
    }
};

/// Accepts one connection per entry in `statusThenBody` in sequence, replying to each with the
/// given HTTP status and body. Used to test the retry loop's backoff-and-retry-on-5xx path: e.g.
/// `{{503, ""}, {200, envelope}}` fails the first attempt and succeeds on the retry.
class MultiShotHttpServer
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor acceptor;
    std::thread serverThread;

public:
    explicit MultiShotHttpServer(std::vector<std::pair<int, std::string>> statusThenBody)
        : acceptor(ioContext, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0))
    {
        serverThread = std::thread(
            [this, statusThenBody = std::move(statusThenBody)]
            {
                for (const auto& [status, body] : statusThenBody)
                {
                    boost::asio::ip::tcp::socket socket(ioContext);
                    acceptor.accept(socket);
                    readHttpRequest(socket);

                    const std::string response = fmt::format(
                        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status,
                        status == 200 ? "OK" : "Error",
                        body.size(),
                        body);
                    boost::asio::write(socket, boost::asio::buffer(response));
                }
            });
    }

    [[nodiscard]] unsigned short port() const { return acceptor.local_endpoint().port(); }

    ~MultiShotHttpServer()
    {
        if (serverThread.joinable())
        {
            serverThread.join();
        }
    }
};

/// Accepts a connection, reads the request, and never replies — exercises CURLOPT_TIMEOUT (a
/// stuck endpoint must not park the calling thread forever).
class StallingHttpServer
{
    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::acceptor acceptor;
    std::thread serverThread;

public:
    StallingHttpServer() : acceptor(ioContext, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0))
    {
        serverThread = std::thread(
            [this]
            {
                boost::asio::ip::tcp::socket socket(ioContext);
                acceptor.accept(socket);
                readHttpRequest(socket);
                /// Deliberately never writes a response; the socket is closed on destruction,
                /// which is enough to unblock curl once its timeout fires.
            });
    }

    [[nodiscard]] unsigned short port() const { return acceptor.local_endpoint().port(); }

    ~StallingHttpServer()
    {
        if (serverThread.joinable())
        {
            serverThread.join();
        }
    }
};

SemanticModelConfig configFor(const std::string& baseUrl)
{
    return SemanticModelConfig{
        .baseUrl = baseUrl,
        .model = "test-model",
        .apiKeyEnv = std::nullopt,
        .steps = {SemanticStep{
            .kind = SemanticStep::Kind::MAP,
            .prompt = "Classify the sentiment as POSITIVE or NEGATIVE",
            .outputValues = {"POSITIVE", "NEGATIVE"},
            .defaultValue = ""}},
        .requestTimeout = std::chrono::seconds(2),
        .connectTimeout = std::chrono::milliseconds(500)};
}

/// A canned OpenAI-compatible chat-completions envelope whose message content is the
/// `_parse_llm_json` row payload the physical operator ultimately needs.
std::string chatCompletionEnvelope(const std::string& messageContent)
{
    const nlohmann::json envelope{{"choices", nlohmann::json::array({{{"message", {{"role", "assistant"}, {"content", messageContent}}}}})}};
    return envelope.dump();
}

}

class CurlLlmClientTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("CurlLlmClientTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(CurlLlmClientTest, ParsesFieldAnswerAndConfidenceFromEndpointResponse)
{
    const std::string rowContent = R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.87}}})";
    OneShotHttpServer server(chatCompletionEnvelope(rowContent));

    CurlLlmClient client(configFor(fmt::format("http://127.0.0.1:{}", server.port())), {"sentiment"});
    const auto result = client.map("this product is great");

    ASSERT_TRUE(result.contains("sentiment"));
    EXPECT_EQ(result.at("sentiment").answer, "POSITIVE");
    EXPECT_DOUBLE_EQ(result.at("sentiment").confidence, 0.87);
}

TEST_F(CurlLlmClientTest, NormalizesAnswerAgainstDeclaredOutputValues)
{
    /// Lowercase + wrapped in a markdown fence: exercises both the normalisation cascade and
    /// the `_parse_llm_json` code-fence fallback in the same round trip.
    const std::string rowContent = "```json\n{\"row1\": {\"sentiment\": {\"answer\": \"positive\", \"confidence\": 0.5}}}\n```";
    OneShotHttpServer server(chatCompletionEnvelope(rowContent));

    CurlLlmClient client(configFor(fmt::format("http://127.0.0.1:{}", server.port())), {"sentiment"});
    const auto result = client.map("meh, it's fine I guess");

    EXPECT_EQ(result.at("sentiment").answer, "POSITIVE");
}

TEST_F(CurlLlmClientTest, DefaultFillsWhenResponseIsUnparseable)
{
    OneShotHttpServer server(chatCompletionEnvelope("the model rambled instead of answering in JSON"));

    CurlLlmClient client(configFor(fmt::format("http://127.0.0.1:{}", server.port())), {"sentiment"});
    const auto result = client.map("ambiguous input");

    EXPECT_EQ(result.at("sentiment").answer, "");
    EXPECT_DOUBLE_EQ(result.at("sentiment").confidence, 0.0);
}

TEST_F(CurlLlmClientTest, ThrowsInferenceRuntimeFailureWhenEndpointIsUnreachable)
{
    /// Port 1 is reserved and nothing binds a client-facing service there: connection is refused
    /// immediately, giving a deterministic transport failure without any real network dependency.
    CurlLlmClient client(configFor("http://127.0.0.1:1"), {"sentiment"});
    ASSERT_EXCEPTION_ERRORCODE(client.map("anything"), NES::ErrorCode::InferenceRuntimeFailure);
}

TEST_F(CurlLlmClientTest, RetriesOn503ThenSucceeds)
{
    const std::string rowContent = R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.9}}})";
    MultiShotHttpServer server({{503, ""}, {200, chatCompletionEnvelope(rowContent)}});

    auto config = configFor(fmt::format("http://127.0.0.1:{}", server.port()));
    config.maxRetries = 2;
    CurlLlmClient client(config, {"sentiment"});
    const auto result = client.map("this product is great");

    EXPECT_EQ(result.at("sentiment").answer, "POSITIVE");
}

TEST_F(CurlLlmClientTest, DoesNotRetryOn4xx)
{
    /// A 400 is exhausted on the first attempt — MultiShotHttpServer only has one response queued,
    /// so a second connection attempt (which would happen if 4xx were retried) hangs until the
    /// test's own timeout, which is what would turn this into a slow/flaky test if the "no retry
    /// on 4xx" behaviour ever regressed.
    MultiShotHttpServer server({{400, "bad request"}});

    auto config = configFor(fmt::format("http://127.0.0.1:{}", server.port()));
    config.maxRetries = 2;
    CurlLlmClient client(config, {"sentiment"});
    ASSERT_EXCEPTION_ERRORCODE(client.map("anything"), NES::ErrorCode::InferenceRuntimeFailure);
}

TEST_F(CurlLlmClientTest, TimesOutAgainstAStalledEndpoint)
{
    StallingHttpServer server;

    auto config = configFor(fmt::format("http://127.0.0.1:{}", server.port()));
    config.requestTimeout = std::chrono::seconds(1);
    config.maxRetries = 0;
    CurlLlmClient client(config, {"sentiment"});
    ASSERT_EXCEPTION_ERRORCODE(client.map("anything"), NES::ErrorCode::InferenceRuntimeFailure);
}

}
