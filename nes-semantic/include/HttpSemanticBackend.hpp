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
#include <optional>
#include <string>

#include <SemanticBackend.hpp>

namespace NES
{

/// OpenAI-compatible chat-completions client over libcurl. POSTs `{endpoint}/chat/completions`
/// with a single user message and returns `choices[0].message.content`.
///
/// Timeouts and retries are set explicitly on every request: the Python reference inherits them
/// from the OpenAI SDK, whereas libcurl's defaults are "wait forever" and "never retry".
class HttpSemanticBackend final : public SemanticBackend
{
public:
    /// `apiKey` is the resolved secret, never an environment variable name. It exists only in this
    /// instance; the catalog entry and the serialized plan carry the variable's name at most.
    HttpSemanticBackend(std::string endpoint, std::optional<std::string> apiKey);
    ~HttpSemanticBackend() override;

    HttpSemanticBackend(const HttpSemanticBackend&) = delete;
    HttpSemanticBackend& operator=(const HttpSemanticBackend&) = delete;
    HttpSemanticBackend(HttpSemanticBackend&&) = delete;
    HttpSemanticBackend& operator=(HttpSemanticBackend&&) = delete;

    [[nodiscard]] std::expected<std::string, BackendError> complete(const CompletionRequest& request) override;

private:
    std::string url;
    std::optional<std::string> apiKey;
    /// Opaque `CURL*`, so this header does not pull libcurl into every consumer.
    void* curlHandle;
};

}
