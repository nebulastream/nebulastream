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

#include <expected>
#include <memory>
#include <optional>
#include <string>

#include <SemanticBackend.hpp>

namespace NES
{

/// Forward declared on purpose: nes-http stays a private dependency of nes-semantic, so this
/// header -- which four other modules reach through nes-semantic's public interface -- does not put
/// the transport on their include path. A test that wants to inject one includes Http/HttpBackend.hpp
/// and links nes-http itself.
class HttpBackend;

/// OpenAI-compatible chat-completions client. POSTs `{endpoint}/chat/completions` with a single
/// user message and returns `choices[0].message.content`.
///
/// What is left here is the protocol and only the protocol: the URL suffix, bearer authentication,
/// the request envelope and the response envelope. Sockets, timeouts, retries and latency belong to
/// the `HttpBackend` underneath, which knows nothing about models or prompts.
class HttpSemanticBackend final : public SemanticBackend
{
public:
    /// `apiKey` is the resolved secret, never an environment variable name. It exists only in this
    /// instance; the catalog entry and the serialized plan carry the variable's name at most.
    HttpSemanticBackend(std::string endpoint, std::optional<std::string> apiKey);

    /// Takes the transport, so a test can assert on the request that was built and answer it
    /// without opening a socket.
    HttpSemanticBackend(std::string endpoint, std::optional<std::string> apiKey, std::unique_ptr<HttpBackend> transport);

    /// Out of line because `transport` points at an incomplete type here.
    ~HttpSemanticBackend() override;

    HttpSemanticBackend(const HttpSemanticBackend&) = delete;
    HttpSemanticBackend& operator=(const HttpSemanticBackend&) = delete;
    HttpSemanticBackend(HttpSemanticBackend&&) = delete;
    HttpSemanticBackend& operator=(HttpSemanticBackend&&) = delete;

    [[nodiscard]] std::expected<std::string, BackendError> complete(const CompletionRequest& request) override;

private:
    std::string endpoint;
    /// `{endpoint}/chat/completions`, kept for the messages.
    std::string url;
    std::optional<std::string> apiKey;
    std::unique_ptr<HttpBackend> transport;
};

}
