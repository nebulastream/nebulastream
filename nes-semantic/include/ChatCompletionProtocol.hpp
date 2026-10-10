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

#include <optional>
#include <string>
#include <string_view>

#include <Http/HttpBackend.hpp>
#include <SemanticBackend.hpp>

namespace NES
{

/// The OpenAI chat-completions protocol as two plain functions: a prompt becomes a request, and a
/// response body becomes the model's text.
///
/// Two transports need exactly this and nothing else. The synchronous operator blocks on
/// `HttpSemanticBackend`, which owns a connection and waits; the asynchronous one hands the same
/// request to an event loop and is told when it finished. Keeping the protocol here is what stops
/// the second one from being a second copy of it.
///
/// This header exposes nes-http types, so whoever includes it needs that module on its include
/// path. nes-semantic keeps it as a private dependency: the only outside user is the asynchronous
/// executor plugin, which gets nes-http through nes-async anyway. No other public header of this
/// module may include it, or the four modules that link nes-semantic publicly would need it too.

/// POSTs to `{endpoint}/chat/completions` with the prompt as the single user message, and carries
/// the bearer token when there is one. `apiKey` is the resolved secret, never a variable name.
[[nodiscard]] HttpRequest
chatCompletionRequest(std::string_view endpoint, const std::optional<std::string>& apiKey, const CompletionRequest& request);

/// `choices[0].message.content` of a chat-completion envelope. Every level is checked, so a 2xx
/// body that is valid JSON of the wrong shape comes back as nullopt rather than as a type error.
[[nodiscard]] std::optional<std::string> chatCompletionContent(const std::string& body);

/// Turns the outcome of one exchange into what a semantic backend reports: the model's text, or
/// why there is none. Shared so that both transports classify the same answer the same way.
[[nodiscard]] std::expected<std::string, BackendError>
chatCompletionOutcome(std::string_view url, const std::expected<HttpResponse, HttpError>& exchange);

}
