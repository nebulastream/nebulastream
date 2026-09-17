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

#include <CurlLlmClient.hpp>

#include <cstddef>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <curl/curl.h>
#include <curl/easy.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp>

#include <ErrorHandling.hpp>
#include <ResponseParsing.hpp>

namespace NES
{

namespace
{

size_t appendToString(char* data, const size_t size, const size_t nmemb, void* userData)
{
    static_cast<std::string*>(userData)->append(data, size * nmemb);
    return size * nmemb;
}

/// Only row "row1" is ever sent — Phase 1 is one HTTP round-trip per record (plan §1).
/// Kept as a named row (rather than a bare string) so Phase 2 batching needs no prompt change.
constexpr std::string_view ROW_ID = "row1";

}

CurlLlmClient::CurlLlmClient(SemanticModelConfig config, std::vector<std::string> outputFieldNames)
    : config(std::move(config)), outputFieldNames(std::move(outputFieldNames)), curlHandle(curl_easy_init())
{
    PRECONDITION(curlHandle, "curl_easy_init failed");
}

CurlLlmClient::~CurlLlmClient()
{
    if (curlHandle)
    {
        curl_easy_cleanup(static_cast<CURL*>(curlHandle));
    }
}

std::string CurlLlmClient::buildPrompt(const std::string_view inputText) const
{
    /// System block (llm_operator.py::_build_fused_sysprompt, single-step case).
    const std::string outputFields = fmt::format("\"{}\"", fmt::join(outputFieldNames, "\", \""));
    std::string exampleRow = "{";
    for (size_t i = 0; i < outputFieldNames.size(); ++i)
    {
        if (i > 0)
        {
            exampleRow += ", ";
        }
        exampleRow += fmt::format(R"("{}": {{"answer": "...", "confidence": 0.9}})", outputFieldNames[i]);
    }
    exampleRow += "}";

    const std::string sysBlock = fmt::format(
        "You are a helpful AI assistant for semantic data operations.\n"
        "Always respond in JSON format.\n"
        "For each input row (_llm_call_id), return a JSON object where each key is an output field name "
        "and the value is a JSON object with 'answer' and 'confidence' (0-1).\n"
        "Output fields: {}\n"
        "Example output: {{\"{}\": {}}}\n"
        "Do not add explanations.",
        outputFields,
        ROW_ID,
        exampleRow);

    /// Operator block (llm_operator.py::_build_fused_operator_prompt, n=1: label is "operation").
    const std::string operatorBlock
        = fmt::format("Apply the following 1 operation to each row:\n  1. MAP: {} → output field: \"{}\"", config.prompt, outputFieldNames.front());

    /// Data block: `{"row1": "<content>"}`, JSON-escaped via nlohmann so embedded quotes/newlines
    /// in the row content cannot corrupt the payload.
    const nlohmann::json payload = {{std::string(ROW_ID), std::string(inputText)}};
    const std::string dataBlock = fmt::format("Data: {}", payload.dump());

    return fmt::format("{}\n{}\n{}", sysBlock, operatorBlock, dataBlock);
}

std::string CurlLlmClient::postChatCompletion(const std::string& requestBody) const
{
    auto* curl = static_cast<CURL*>(curlHandle);
    const std::string target = fmt::format("{}/chat/completions", config.baseUrl);

    curl_slist* headers = curl_slist_append(nullptr, "Content-Type: application/json");
    /// Suppresses the "Expect: 100-continue" handshake libcurl adds for larger bodies — the
    /// semantic prompt easily crosses that threshold, and a plain HTTP/1.1 test server (or a
    /// minimal `mock://` seam, plan §M4) has no reason to implement it.
    headers = curl_slist_append(headers, "Expect:");
    if (config.apiKeyEnv.has_value())
    {
        if (const char* apiKey = std::getenv(config.apiKeyEnv->c_str()))
        {
            headers = curl_slist_append(headers, fmt::format("Authorization: Bearer {}", apiKey).c_str());
        }
    }

    std::string responseBody;
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, requestBody.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(requestBody.size()));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, appendToString);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);

    const CURLcode result = curl_easy_perform(curl);
    long status = 0;
    if (result == CURLE_OK)
    {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    }
    curl_slist_free_all(headers);

    /// Transport failure: the endpoint is down or misbehaving. D7 (plan §M4) says throw here —
    /// distinct from "the model answered something we couldn't parse".
    if (result != CURLE_OK)
    {
        throw NES::InferenceRuntimeFailure("Semantic model request to {} failed: {}", target, curl_easy_strerror(result));
    }
    if (status < 200 || status >= 300)
    {
        throw NES::InferenceRuntimeFailure("Semantic model request to {} returned HTTP {}", target, status);
    }
    return responseBody;
}

SemanticMapResult CurlLlmClient::map(const std::string_view inputText)
{
    const nlohmann::json requestBody{
        {"model", config.model}, {"messages", nlohmann::json::array({{{"role", "user"}, {"content", buildPrompt(inputText)}}})}};

    const std::string responseBody = postChatCompletion(requestBody.dump());

    /// An unparseable completion envelope (not the model's JSON row, but the HTTP response
    /// itself) is treated the same as an unparseable row below: default-fill, don't throw.
    const nlohmann::json completion = nlohmann::json::parse(responseBody, nullptr, false);
    std::string content;
    if (!completion.is_discarded() && completion.contains("choices") && !completion["choices"].empty())
    {
        content = completion["choices"][0]["message"]["content"].get<std::string>();
    }

    const nlohmann::json parsed = parseLlmJson(content);
    const nlohmann::json row = parsed.contains(ROW_ID) ? parsed[std::string(ROW_ID)] : nlohmann::json::object();

    SemanticMapResult result;
    for (const auto& fieldName : outputFieldNames)
    {
        std::string rawAnswer;
        double confidence = 0.0;
        if (row.contains(fieldName))
        {
            const auto& fieldValue = row[fieldName];
            if (fieldValue.contains("answer"))
            {
                rawAnswer = fieldValue["answer"].is_string() ? fieldValue["answer"].get<std::string>() : fieldValue["answer"].dump();
            }
            if (fieldValue.contains("confidence") && fieldValue["confidence"].is_number())
            {
                confidence = fieldValue["confidence"].get<double>();
            }
        }
        result[fieldName] = SemanticFieldResult{.answer = normalizeAnswer(rawAnswer, config.outputValues, ""), .confidence = confidence};
    }
    return result;
}

}
