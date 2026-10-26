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

#include <SemanticCallAsyncFunction.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <nlohmann/json.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticCallStats.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES
{

namespace
{

/// The configuration that travelled here names only the environment variable; the key itself lives on the worker.
std::optional<std::string> resolveApiKey(const SemanticCallPayload& payload)
{
    if (!payload.config.apiKeyEnvVar.has_value())
    {
        return std::nullopt;
    }
    const char* const value = std::getenv(payload.config.apiKeyEnvVar->c_str()); /// NOLINT(concurrency-mt-unsafe) read once, at creation
    if (value == nullptr)
    {
        throw CannotOpenSource(
            "Semantic model '{}' reads its API key from environment variable {}, which is not set on this worker",
            payload.model,
            *payload.config.apiKeyEnvVar);
    }
    return std::string{value};
}

/// The columns the prompt asks for, as the mock answers them: the MAP outputs, and `__filter_<k>` per FILTER step of a
/// mixed list. A list of FILTER steps only is answered in the flat envelope, without columns.
std::vector<std::string> mockColumns(const SemanticModelConfig& config)
{
    const bool filterOnly
        = std::ranges::all_of(config.steps, [](const SemanticStep& step) { return step.kind == SemanticStep::Kind::FILTER; });
    if (filterOnly)
    {
        return {};
    }
    std::vector<std::string> columns;
    size_t filters = 0;
    for (const auto& step : config.steps)
    {
        columns.push_back(step.kind == SemanticStep::Kind::MAP ? step.outputColumn : "__filter_" + std::to_string(filters++));
    }
    return columns;
}

/// Instances alive in this process. Each Async source slot owns one, so the last one to go marks the end of a run, which
/// is when the measurement is reported.
std::atomic<size_t> liveInstances{0};

/// The `usage` block of a chat-completion envelope, as vLLM, Ollama and the hosted APIs send it.
void recordUsage(const nlohmann::json& envelope)
{
    const auto count = [&](const char* const field) -> std::optional<uint64_t>
    {
        if (!envelope.contains("usage") || !envelope["usage"].is_object() || !envelope["usage"].contains(field)
            || !envelope["usage"][field].is_number_unsigned())
        {
            return std::nullopt;
        }
        return envelope["usage"][field].get<uint64_t>();
    };
    const auto prompt = count("prompt_tokens");
    const auto completion = count("completion_tokens");
    const bool cutOff = envelope.contains("choices") && envelope["choices"].is_array() && !envelope["choices"].empty()
        && envelope["choices"][0].is_object() && envelope["choices"][0].value("finish_reason", "") == "length";
    SemanticCallStats::instance().recordUsage(prompt && completion, prompt.value_or(0), completion.value_or(0), cutOff);
}

/// `choices[0].message.content` of a chat-completion envelope, if it has one.
std::optional<std::string> messageContent(const std::string& body)
{
    const auto envelope = nlohmann::json::parse(body, nullptr, false);
    if (envelope.is_object())
    {
        recordUsage(envelope);
    }
    if (!envelope.is_object() || !envelope.contains("choices") || !envelope["choices"].is_array() || envelope["choices"].empty())
    {
        return std::nullopt;
    }
    const auto& choice = envelope["choices"][0];
    if (!choice.is_object() || !choice.contains("message") || !choice["message"].is_object())
    {
        return std::nullopt;
    }
    const auto& content = choice["message"].value("content", nlohmann::json{});
    return content.is_string() ? std::optional{content.get<std::string>()} : std::nullopt;
}

}

SemanticCallAsyncFunction::SemanticCallAsyncFunction(SemanticCallPayload decoded) : payload(std::move(decoded))
{
    const auto& config = payload.config;
    if (config.batchSize != 1)
    {
        throw CannotOpenSource("SEM_CALL sends one row per prompt, but model '{}' sets BATCH_SIZE {}", payload.model, config.batchSize);
    }
    const bool hasFilter
        = std::ranges::any_of(config.steps, [](const SemanticStep& step) { return step.kind == SemanticStep::Kind::FILTER; });
    if (hasFilter)
    {
        filterCodec.emplace(config);
    }
    else
    {
        mapCodec.emplace(config);
    }
    if (config.backend == "mock")
    {
        mock.emplace(config.endpoint, mockColumns(config));
    }
    else if (config.backend == "http")
    {
        http.emplace(config.endpoint + "/chat/completions", resolveApiKey(payload), std::chrono::milliseconds{config.requestTimeout});
    }
    else
    {
        throw CannotOpenSource("Semantic model '{}' has unknown backend '{}'", payload.model, config.backend);
    }
    /// Counted last, so that an instance whose construction threw is not counted.
    ++liveInstances;
}

SemanticCallAsyncFunction::~SemanticCallAsyncFunction()
{
    if (--liveInstances == 0)
    {
        SemanticCallStats::instance().reportAndReset("SEM_CALL");
    }
}

AsyncFunctionRegistryReturnType SemanticCallAsyncFunction::create(AsyncFunctionRegistryArguments arguments)
{
    return std::make_unique<SemanticCallAsyncFunction>(decodeSemanticCallPayload(arguments.configuration));
}

boost::asio::awaitable<AsyncCallResult> SemanticCallAsyncFunction::callValues(std::vector<std::string> arguments)
{
    PRECONDITION(
        arguments.size() == payload.inputFields.size(),
        "SEM_CALL of model '{}' takes {} inputs, got {}",
        payload.model,
        payload.inputFields.size(),
        arguments.size());
    RowPayload row{.rowId = "row1", .fields = {}};
    for (size_t field = 0; field < arguments.size(); ++field)
    {
        row.fields.emplace_back(payload.inputFields[field], std::move(arguments[field]));
    }
    const std::vector rows{std::move(row)};
    const auto prompt = mapCodec ? mapCodec->buildPrompt(rows) : filterCodec->buildPrompt(rows);
    const auto answer = co_await complete(prompt);
    co_return decode(answer, rows);
}

boost::asio::awaitable<std::string> SemanticCallAsyncFunction::complete(const std::string& prompt)
{
    if (mock)
    {
        if (mock->getDelay() > std::chrono::milliseconds{0})
        {
            boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, mock->getDelay()};
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        auto answer = mock->respond(prompt);
        if (!answer)
        {
            throw CannotReadFromSource("SEM_CALL of model '{}': {}", payload.model, answer.error().message);
        }
        co_return std::move(answer).value();
    }

    nlohmann::json request{
        {"model", payload.config.modelName},
        {"messages", nlohmann::json::array({{{"role", "user"}, {"content", prompt}}})},
        {"stream", false}};
    if (payload.config.maxTokens > 0)
    {
        request["max_tokens"] = payload.config.maxTokens;
    }
    const auto start = std::chrono::steady_clock::now();
    std::optional<SemanticHttpResponse> exchanged;
    try
    {
        exchanged = co_await http->post(request.dump());
    }
    catch (...)
    {
        SemanticCallStats::instance().recordCall(start, std::chrono::steady_clock::now(), true);
        throw;
    }
    const auto& response = *exchanged;
    SemanticCallStats::instance().recordCall(start, std::chrono::steady_clock::now(), response.status < 200 || response.status >= 300);
    if (response.status < 200 || response.status >= 300)
    {
        throw CannotReadFromSource("SEM_CALL of model '{}' got status {}: {}", payload.model, response.status, response.body);
    }
    if (auto content = messageContent(response.body))
    {
        co_return std::move(content).value();
    }
    /// An answer without content is unusable, not a transport failure: the codec fills in the defaults.
    NES_WARNING("SEM_CALL of model '{}' got a response without chat-completion content: {}", payload.model, response.body);
    co_return std::string{};
}

AsyncCallResult SemanticCallAsyncFunction::decode(const std::string& answer, const std::vector<RowPayload>& rows) const
{
    if (mapCodec)
    {
        return mapCodec->parse(answer, rows).front();
    }
    const auto result = filterCodec->parse(answer, rows).front();
    AsyncCallResult values;
    values.reserve(payload.config.steps.size());
    size_t mapStep = 0;
    for (const auto& step : payload.config.steps)
    {
        /// Fused FILTER steps share one verdict: the codec only reports whether the row passed all of them.
        values.push_back(step.kind == SemanticStep::Kind::MAP ? result.mapAnswers.at(mapStep++) : (result.passed ? "true" : "false"));
    }
    return values;
}

}
