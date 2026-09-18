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

#include <MockLlmClient.hpp>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <ResponseParsing.hpp>

namespace NES
{

namespace
{

/// Behaviour selector: the first path segment after `mock://`. Unknown behaviours are a
/// configuration error (CannotLoadModel), mirroring how the curl client surfaces an unreachable
/// endpoint.
enum class Behaviour
{
    Echo,
    Unparseable,
    Unreachable
};

constexpr std::string_view ECHO_BEHAVIOUR = "echo";
constexpr std::string_view UNPARSEABLE_BEHAVIOUR = "unparseable";
constexpr std::string_view UNREACHABLE_BEHAVIOUR = "unreachable";
constexpr std::string_view SCHEME_PREFIX = "mock://";

Behaviour behaviourOf(const std::string& baseUrl)
{
    std::string_view rest = baseUrl;
    rest.remove_prefix(SCHEME_PREFIX.size());
    const auto behaviour = rest.substr(0, rest.find('/'));
    if (behaviour == ECHO_BEHAVIOUR)
    {
        return Behaviour::Echo;
    }
    if (behaviour == UNPARSEABLE_BEHAVIOUR)
    {
        return Behaviour::Unparseable;
    }
    if (behaviour == UNREACHABLE_BEHAVIOUR)
    {
        return Behaviour::Unreachable;
    }
    throw CannotLoadModel("Unknown mock behaviour 'mock://{}' (expected {}, {} or {})", behaviour, ECHO_BEHAVIOUR, UNPARSEABLE_BEHAVIOUR, UNREACHABLE_BEHAVIOUR);
}

}

MockLlmClient::MockLlmClient(SemanticModelConfig config, std::vector<std::string> outputFieldNames)
    : config(std::move(config)), outputFieldNames(std::move(outputFieldNames))
{
    /// Fail fast on an unknown behaviour — a typo'd mock URL is a configuration error, not
    /// something to discover on the first record.
    (void)behaviourOf(this->config.baseUrl);
}

SemanticMapResult MockLlmClient::map(const std::string_view inputText)
{
    const auto behaviour = behaviourOf(config.baseUrl);

    if (behaviour == Behaviour::Unreachable)
    {
        /// D7(a): transport failure — the endpoint is down. Throw; this is the query's problem,
        /// not this row's. Mirrors CurlLlmClient's curl_easy_perform failure path.
        throw InferenceRuntimeFailure("Semantic model request to {} failed: mock endpoint is unreachable", config.baseUrl);
    }

    SemanticMapResult result;
    for (const auto& fieldName : outputFieldNames)
    {
        if (behaviour == Behaviour::Echo)
        {
            /// Happy path: echo the input uppercased, confidence 1.0 — but through the real
            /// normalisation, so restricted models cascade exactly like against a live LLM.
            std::string upper;
            upper.reserve(inputText.size());
            std::ranges::transform(
                inputText, std::back_inserter(upper), [](const unsigned char c) { return static_cast<char>(std::toupper(c)); });
            result[fieldName] = SemanticFieldResult{.answer = normalizeAnswer(upper, config.outputValues, ""), .confidence = 1.0};
        }
        else
        {
            /// D7(b): unparseable response — default-fill every field instead of throwing.
            /// Mirrors CurlLlmClient's empty-envelope default-fill.
            result[fieldName] = SemanticFieldResult{.answer = normalizeAnswer("", config.outputValues, ""), .confidence = 0.0};
        }
    }
    return result;
}

}