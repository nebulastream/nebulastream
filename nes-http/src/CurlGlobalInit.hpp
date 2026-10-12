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

#include <mutex>

#include <curl/curl.h>

#include <ErrorHandling.hpp>

namespace NES
{

/// curl_easy_init and curl_multi_init initialize libcurl implicitly, but that implicit path is not
/// thread-safe and both clients may be constructed concurrently. One explicit global init, never
/// cleaned up: a client can live until process exit.
///
/// Inline, so the two clients share one flag rather than each calling the function again. The
/// static lives once across translation units.
inline void ensureCurlGlobalInit()
{
    static std::once_flag once;
    std::call_once(
        once,
        []
        {
            if (const auto code = curl_global_init(CURL_GLOBAL_DEFAULT); code != CURLE_OK)
            {
                throw CannotCreateHttpClient("curl_global_init failed: {}", curl_easy_strerror(code));
            }
        });
}

}
