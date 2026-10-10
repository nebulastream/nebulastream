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

#include <Http/HttpBackend.hpp>

namespace NES
{

/// libcurl client. Timeouts and retries are set explicitly on every request, because libcurl's
/// defaults are "wait forever" and "never retry".
///
/// The easy handle is a member and is reset, not recreated, per request: `curl_easy_reset` clears
/// the options but *not* the connection cache, so keep-alive survives across calls. Creating a
/// fresh handle in `send` would look tidier and would charge every request a new TCP and TLS
/// handshake, which changes what a latency measurement means.
class CurlHttpBackend final : public HttpBackend
{
public:
    CurlHttpBackend();
    ~CurlHttpBackend() override;

    CurlHttpBackend(const CurlHttpBackend&) = delete;
    CurlHttpBackend& operator=(const CurlHttpBackend&) = delete;
    CurlHttpBackend(CurlHttpBackend&&) = delete;
    CurlHttpBackend& operator=(CurlHttpBackend&&) = delete;

    [[nodiscard]] std::expected<HttpResponse, HttpError> send(const HttpRequest& request) override;

private:
    /// Opaque `CURL*`, so this header does not pull libcurl into every consumer.
    void* curlHandle;
};

}
