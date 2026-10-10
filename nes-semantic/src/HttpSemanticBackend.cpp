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

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <fmt/format.h>

#include <Http/HttpBackend.hpp>
#include <ChatCompletionProtocol.hpp>
#include <ErrorHandling.hpp>
#include <SemanticBackend.hpp>

namespace NES
{

HttpSemanticBackend::HttpSemanticBackend(std::string endpoint, std::optional<std::string> apiKey)
    : HttpSemanticBackend(std::move(endpoint), std::move(apiKey), defaultHttpBackendFactory()())
{
}

HttpSemanticBackend::HttpSemanticBackend(std::string endpoint, std::optional<std::string> apiKey, std::unique_ptr<HttpBackend> transport)
    : endpoint(std::move(endpoint))
    , url(fmt::format("{}/chat/completions", this->endpoint))
    , apiKey(std::move(apiKey))
    , transport(std::move(transport))
{
    PRECONDITION(this->transport != nullptr, "A semantic backend needs a transport");
}

HttpSemanticBackend::~HttpSemanticBackend() = default;

std::expected<std::string, BackendError> HttpSemanticBackend::complete(const CompletionRequest& request)
{
    /// The protocol is in ChatCompletionProtocol, shared with the asynchronous executor, so the two
    /// transports cannot drift apart in what they send or in how they read an answer.
    return chatCompletionOutcome(url, transport->send(chatCompletionRequest(endpoint, apiKey, request)));
}

}
