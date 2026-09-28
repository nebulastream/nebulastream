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
#include <expected>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include <curl/curl.h>
#include <curl/easy.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <ErrorHandling.hpp>
#include <SemanticBackend.hpp>

namespace NES
{

namespace
{

/// curl_easy_init initializes libcurl implicitly, but that implicit path is not thread-safe, and
/// worker threads may construct their backends concurrently. One explicit global init, never
/// cleaned up: backends can live until process exit.
void ensureCurlGlobalInit()
{
    static std::once_flag once;
    std::call_once(
        once,
        []
        {
            if (const auto code = curl_global_init(CURL_GLOBAL_DEFAULT); code != CURLE_OK)
            {
                throw CannotLoadModel("curl_global_init failed: {}", curl_easy_strerror(code));
            }
        });
}

size_t appendToString(char* data, const size_t size, const size_t nmemb, void* userData)
{
    static_cast<std::string*>(userData)->append(data, size * nmemb);
    return size * nmemb;
}

/// Frees the header list on every exit path, including exceptions from the retry loop.
struct HeaderList
{
    curl_slist* list = nullptr;

    HeaderList() = default;
    HeaderList(const HeaderList&) = delete;
    HeaderList& operator=(const HeaderList&) = delete;
    HeaderList(HeaderList&&) = delete;
    HeaderList& operator=(HeaderList&&) = delete;

    ~HeaderList() { curl_slist_free_all(list); }

    void append(const std::string& header) { list = curl_slist_append(list, header.c_str()); }
};

/// `choices[0].message.content` of an OpenAI chat-completion envelope. Every level is checked: a 2xx
/// body that is valid JSON of the wrong shape must not escape as a nlohmann type error.
std::optional<std::string> extractMessageContent(const std::string& body)
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

}

HttpSemanticBackend::HttpSemanticBackend(std::string endpoint, std::optional<std::string> apiKey)
    : url(fmt::format("{}/chat/completions", endpoint)), apiKey(std::move(apiKey)), curlHandle(nullptr)
{
    ensureCurlGlobalInit();
    curlHandle = curl_easy_init();
    if (curlHandle == nullptr)
    {
        throw CannotLoadModel("curl_easy_init failed for semantic model endpoint {}", endpoint);
    }
}

HttpSemanticBackend::~HttpSemanticBackend()
{
    curl_easy_cleanup(static_cast<CURL*>(curlHandle));
}

std::expected<std::string, BackendError> HttpSemanticBackend::complete(const CompletionRequest& request)
{
    auto* curl = static_cast<CURL*>(curlHandle);

    const auto body
        = nlohmann::
              json{{"model", request.modelName}, {"messages", nlohmann::json::array({{{"role", "user"}, {"content", request.prompt}}})}}
                  .dump();

    HeaderList headers;
    headers.append("Content-Type: application/json");
    /// Suppresses the "Expect: 100-continue" handshake libcurl adds for larger bodies; prompts cross
    /// that threshold easily and minimal OpenAI-compatible servers need not implement it.
    headers.append("Expect:");
    if (apiKey.has_value())
    {
        headers.append(fmt::format("Authorization: Bearer {}", *apiKey));
    }

    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers.list);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size()));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, appendToString);
    /// Mandatory off the main thread: without it libcurl uses SIGALRM for DNS timeouts.
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, static_cast<long>(request.timeout.count()));
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(request.connectTimeout.count()));

    CURLcode result = CURLE_OK;
    long status = 0;
    std::string responseBody;
    for (size_t attempt = 0;; ++attempt)
    {
        responseBody.clear();
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
        result = curl_easy_perform(curl);
        status = 0;
        if (result == CURLE_OK)
        {
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
        }

        /// Retry only transport failures, 429 and 5xx. Any other 4xx is the request's fault and
        /// would fail identically, only slower.
        const bool retryable = result != CURLE_OK || status == 429 || (status >= 500 && status < 600);
        if (!retryable || attempt >= request.maxRetries)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100) * (1U << attempt));
    }

    if (result != CURLE_OK)
    {
        return std::unexpected{BackendError{
            .kind = BackendError::Kind::UNREACHABLE, .message = fmt::format("request to {} failed: {}", url, curl_easy_strerror(result))}};
    }
    if (status < 200 || status >= 300)
    {
        return std::unexpected{
            BackendError{.kind = BackendError::Kind::HTTP_STATUS, .message = fmt::format("request to {} returned HTTP {}", url, status)}};
    }
    if (auto content = extractMessageContent(responseBody))
    {
        return std::move(content).value();
    }
    return std::unexpected{BackendError{
        .kind = BackendError::Kind::MALFORMED_RESPONSE, .message = fmt::format("response from {} is not a chat-completion envelope", url)}};
}

}
