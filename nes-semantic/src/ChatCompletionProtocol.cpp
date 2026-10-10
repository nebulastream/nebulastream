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

#include <ChatCompletionProtocol.hpp>

#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <Http/HttpBackend.hpp>
#include <SemanticBackend.hpp>

namespace NES
{

HttpRequest
chatCompletionRequest(const std::string_view endpoint, const std::optional<std::string>& apiKey, const CompletionRequest& request)
{
    HttpRequest httpRequest{
        .url = fmt::format("{}/chat/completions", endpoint),
        .method = HttpMethod::POST,
        .headers = {{"Content-Type", "application/json"}},
        .body
        = nlohmann::
              json{{"model", request.modelName}, {"messages", nlohmann::json::array({{{"role", "user"}, {"content", request.prompt}}})}}
                  .dump(),
        .timeout = request.timeout,
        .connectTimeout = request.connectTimeout,
        .maxRetries = request.maxRetries};
    if (apiKey.has_value())
    {
        httpRequest.headers.emplace_back("Authorization", fmt::format("Bearer {}", *apiKey));
    }
    return httpRequest;
}

std::optional<std::string> chatCompletionContent(const std::string& body)
{
    const auto envelope = nlohmann::json::parse(body, nullptr, false);
    if (envelope.is_discarded() || !envelope.is_object())
    {
        return std::nullopt;
    }
    const auto choices = envelope.find("choices");
    if (choices == envelope.end() || !choices->is_array() || choices->empty() || !(*choices)[0].is_object())
    {
        return std::nullopt;
    }
    const auto message = (*choices)[0].find("message");
    if (message == (*choices)[0].end() || !message->is_object())
    {
        return std::nullopt;
    }
    const auto content = message->find("content");
    if (content == message->end() || !content->is_string())
    {
        return std::nullopt;
    }
    return content->get<std::string>();
}

std::expected<std::string, BackendError>
chatCompletionOutcome(const std::string_view url, const std::expected<HttpResponse, HttpError>& exchange)
{
    if (!exchange.has_value())
    {
        return std::unexpected{BackendError{.kind = BackendError::Kind::UNREACHABLE, .message = exchange.error().message}};
    }
    if (exchange->status < 200 || exchange->status >= 300)
    {
        return std::unexpected{BackendError{
            .kind = BackendError::Kind::HTTP_STATUS, .message = fmt::format("request to {} returned HTTP {}", url, exchange->status)}};
    }
    if (auto content = chatCompletionContent(exchange->body))
    {
        return std::move(content).value();
    }
    return std::unexpected{BackendError{
        .kind = BackendError::Kind::MALFORMED_RESPONSE, .message = fmt::format("response from {} is not a chat-completion envelope", url)}};
}

}
