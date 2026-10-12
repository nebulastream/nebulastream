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
#include <expected>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <curl/curl.h>
#include <curl/easy.h>
#include <fmt/format.h>

#include <Http/HttpBackend.hpp>
#include <Http/HttpLatencyStats.hpp>
#include <CurlGlobalInit.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

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

}

CurlHttpBackend::CurlHttpBackend() : curlHandle(nullptr)
{
    ensureCurlGlobalInit();
    curlHandle = curl_easy_init();
    if (curlHandle == nullptr)
    {
        throw CannotCreateHttpClient("curl_easy_init failed");
    }
}

CurlHttpBackend::~CurlHttpBackend()
{
    curl_easy_cleanup(static_cast<CURL*>(curlHandle));
}

std::expected<HttpResponse, HttpError> CurlHttpBackend::send(const HttpRequest& request)
{
    auto* curl = static_cast<CURL*>(curlHandle);

    HeaderList headers;
    for (const auto& [name, value] : request.headers)
    {
        headers.append(fmt::format("{}: {}", name, value));
    }
    /// Suppresses the "Expect: 100-continue" handshake libcurl adds for larger bodies; minimal
    /// HTTP servers need not implement it, and a body easily crosses the threshold. A caller that
    /// wants the handshake can set the header itself, which then comes after this one and wins.
    headers.append("Expect:");

    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, request.url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers.list);
    if (request.method == HttpMethod::POST)
    {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.body.size()));
    }
    else
    {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    }
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, appendToString);
    /// Mandatory off the main thread: without it libcurl uses SIGALRM for DNS timeouts.
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, static_cast<long>(request.timeout.count()));
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(request.connectTimeout.count()));

    CURLcode result = CURLE_OK;
    long status = 0;
    std::string responseBody;
    /// Timed across the retries, because that is the wait the caller actually experiences, and it
    /// is the figure a throughput number has to be read against.
    const auto startedAt = std::chrono::steady_clock::now();
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

    const auto elapsed = std::chrono::steady_clock::now() - startedAt;
    const bool failed = result != CURLE_OK || status < 200 || status >= 300;
    HttpLatencyStats::instance().record(elapsed, failed);

    if (result != CURLE_OK)
    {
        return std::unexpected{HttpError{.message = fmt::format("request to {} failed: {}", request.url, curl_easy_strerror(result))}};
    }
    return HttpResponse{.status = status, .body = std::move(responseBody)};
}

HttpBackendFactory defaultHttpBackendFactory()
{
    return [] { return std::make_unique<CurlHttpBackend>(); };
}

}
