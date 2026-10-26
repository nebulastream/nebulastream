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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>

namespace NES
{

/// One completion request. Operator-agnostic: the prompt is fully assembled by the caller's codec
/// and the transport knows nothing about records, columns or operators.
struct CompletionRequest
{
    std::string prompt;
    /// Model identifier passed through to the endpoint, e.g. llama3.1:8b.
    std::string modelName;
    /// Upper bound for one attempt, connect included (libcurl's CURLOPT_TIMEOUT).
    std::chrono::seconds timeout{600};
    std::chrono::milliseconds connectTimeout{10000};
    /// Additional attempts after the first on a transport failure, HTTP 429 or 5xx.
    size_t maxRetries = 2;
};

/// Why a completion produced no model output.
struct BackendError
{
    enum class Kind : uint8_t
    {
        /// No HTTP exchange happened: refused, DNS, TLS, timeout — after all retries.
        UNREACHABLE,
        /// The endpoint answered with a non-2xx status after all retries.
        HTTP_STATUS,
        /// A 2xx answer whose body is not a chat-completion envelope with a text message.
        MALFORMED_RESPONSE,
    };

    Kind kind;
    std::string message;
};

/// Transport to an LLM: a prompt goes in, the model's response text comes out. Unchanged for every
/// semantic operator; everything operator-specific (prompt layout, response decoding) lives in the
/// operator's codec, e.g. `SemanticMapCodec`.
///
/// Implementations are not thread-safe. The physical operator keeps one instance per worker thread.
class SemanticBackend
{
public:
    virtual ~SemanticBackend() = default;
    [[nodiscard]] virtual std::expected<std::string, BackendError> complete(const CompletionRequest& request) = 0;
};

/// Creates one backend instance; called once per worker thread when a pipeline starts.
using SemanticBackendProvider = std::function<std::unique_ptr<SemanticBackend>()>;

}
