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

#include <string>
#include <string_view>
#include <vector>

#include <LlmClient.hpp>
#include <SemanticModelConfig.hpp>

namespace NES
{

/// OpenAI-compatible chat-completions client over libcurl. POSTs `{baseUrl}/chat/completions`
/// with a single "user" message assembling the prompt per plan §2.1, then runs the response
/// through `parseLlmJson` + `normalizeAnswer` (ResponseParsing.hpp, §2.2/§2.3).
///
/// Per D7 (plan §M4): throws `InferenceRuntimeFailure` on transport failure (unreachable
/// endpoint, non-2xx status) — that is the coordinator/worker's problem, not this row's. An
/// unparseable response body is not a transport failure: it default-fills every field instead.
class CurlLlmClient : public LlmClient
{
    SemanticModelConfig config;
    std::vector<std::string> outputFieldNames;
    /// Opaque `CURL*` — kept untyped here so this public header does not leak libcurl into
    /// every translation unit that constructs a `SemMapPhysicalOperator` (plan §M1).
    void* curlHandle;

public:
    /// `outputFieldNames` are the declared OUTPUT field base names (e.g. "sentiment"), without
    /// the "_confidence" suffix — see the naming convention note in plan §2.1.
    CurlLlmClient(SemanticModelConfig config, std::vector<std::string> outputFieldNames);
    ~CurlLlmClient() override;

    CurlLlmClient(const CurlLlmClient&) = delete;
    CurlLlmClient& operator=(const CurlLlmClient&) = delete;
    CurlLlmClient(CurlLlmClient&&) = delete;
    CurlLlmClient& operator=(CurlLlmClient&&) = delete;

    SemanticMapResult map(std::string_view inputText) override;

private:
    [[nodiscard]] std::string buildPrompt(std::string_view inputText) const;
    [[nodiscard]] std::string postChatCompletion(const std::string& requestBody) const;
};

}
