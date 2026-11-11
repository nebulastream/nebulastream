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
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <MockSemanticBackend.hpp>
#include <ResponseParsing.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticCallStats.hpp>
#include <SemanticImage.hpp>
#include <SemanticKeyedCodec.hpp>
#include <SemanticLabelAnswer.hpp>
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
    columns.reserve(config.steps.size());
    size_t filters = 0;
    for (const auto& step : config.steps)
    {
        columns.push_back(step.kind == SemanticStep::Kind::MAP ? step.outputColumn : "__filter_" + std::to_string(filters++));
    }
    return columns;
}

/// The INPUT values of a row as one text, as the reference's `" ".join(str(row.get(c, "")) for c in depends_on)`.
std::string spaceJoined(const std::span<const std::string> values)
{
    std::string text;
    for (const auto& value : values)
    {
        if (!text.empty())
        {
            text.push_back(' ');
        }
        text += value;
    }
    return text;
}

/// Whether the answer has an entry for `rowId`; an answer that cannot be parsed has none.
bool mentions(const nlohmann::json& parsed, const std::string& rowId)
{
    return findMember(parsed, rowId) != nullptr;
}

/// What SEM_CALL's shared limit is shared by: one model on one endpoint.
std::string sharedResource(const SemanticModelConfig& config)
{
    return fmt::format("SEM_CALL {} {}", config.backend == "mock" ? "mock" : config.endpoint, config.modelName);
}

std::string batchRowId(const size_t position)
{
    return "row" + std::to_string(position + 1);
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
std::optional<std::string> messageContent(const nlohmann::json& envelope)
{
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
    const bool hasFilter
        = std::ranges::any_of(config.steps, [](const SemanticStep& step) { return step.kind == SemanticStep::Kind::FILTER; });
    if (config.keyed)
    {
        if (payload.stateId.empty())
        {
            throw CannotOpenSource("SEM_CALL of model '{}' keeps state per key, but its configuration names no state", payload.model);
        }
        keyedState = SemanticKeyedState::acquire(payload.stateId);
        if (config.keyed->kind == SemanticKeyedConfig::Kind::SCAN)
        {
            scanCodec.emplace(config);
        }
        else
        {
            windowCodec.emplace(config);
        }
    }
    else if (payload.agg)
    {
        if (payload.inputFields.size() != 1 || config.steps.size() != 1 || hasFilter)
        {
            throw CannotOpenSource("SEM_CALL of aggregation model '{}' needs one MAP step and one input", payload.model);
        }
        aggCodec.emplace(config, payload.inputFields.front());
    }
    else if (payload.join)
    {
        if (payload.inputFields.size() != 2 || config.steps.size() != 1 || !hasFilter)
        {
            throw CannotOpenSource("SEM_CALL of join model '{}' needs one FILTER step and two inputs", payload.model);
        }
        joinCodec.emplace(config);
    }
    else if (hasFilter)
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
        apiKey = resolveApiKey(payload);
        http.emplace(config.endpoint + "/chat/completions", apiKey, std::chrono::milliseconds{config.requestTimeout});
    }
    else
    {
        throw CannotOpenSource("Semantic model '{}' has unknown backend '{}'", payload.model, config.backend);
    }
    if (config.sharedConcurrency > 0)
    {
        sharedLimiter = AsyncResourceLimiter::of(sharedResource(config), config.sharedConcurrency);
    }
    for (const auto& field : config.imageFields)
    {
        const auto position = std::ranges::find(payload.inputFields, field);
        if (position == payload.inputFields.end() || config.keyed || payload.join || payload.agg)
        {
            throw CannotOpenSource(
                "SEM_CALL of model '{}': image field '{}' is not an INPUT field of a SEM_MAP or SEM_FILTER model", payload.model, field);
        }
        imageFieldPositions.push_back(static_cast<size_t>(position - payload.inputFields.begin()));
    }
    if (config.answerMode == AnswerMode::LABEL && (payload.join || payload.agg || config.keyed))
    {
        throw CannotOpenSource("SEM_CALL of model '{}': LLM.ANSWER_MODE LABEL is for SEM_MAP and SEM_FILTER only", payload.model);
    }
    /// Counted last, so that an instance whose construction threw is not counted.
    ++liveInstances;
}

std::optional<AsyncSharedLimit> SemanticCallAsyncFunction::sharedLimit() const
{
    if (payload.config.sharedConcurrency == 0)
    {
        return std::nullopt;
    }
    return AsyncSharedLimit{.resource = sharedResource(payload.config), .limit = payload.config.sharedConcurrency};
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

size_t SemanticCallAsyncFunction::maxBatchSize() const
{
    if (payload.config.keyed || payload.agg || payload.config.answerMode == AnswerMode::LABEL)
    {
        return 1;
    }
    return std::max<size_t>(1, payload.config.batchSize);
}

std::optional<size_t> SemanticCallAsyncFunction::orderingKey() const
{
    return payload.config.keyed ? std::optional<size_t>{0} : std::nullopt;
}

boost::asio::awaitable<AsyncCallResult> SemanticCallAsyncFunction::callValues(std::vector<std::string> arguments)
{
    /// SEM_SCAN's key and SEM_WINDOW's key and timestamp come before the INPUT fields.
    size_t leading = 0;
    if (payload.config.keyed)
    {
        leading = payload.config.keyed->kind == SemanticKeyedConfig::Kind::SCAN ? 1 : 2;
    }
    PRECONDITION(
        arguments.size() == payload.inputFields.size() + leading,
        "SEM_CALL of model '{}' takes {} arguments, got {}",
        payload.model,
        payload.inputFields.size() + leading,
        arguments.size());
    if (scanCodec)
    {
        co_return co_await callScan(std::move(arguments));
    }
    if (windowCodec)
    {
        co_return co_await callWindow(std::move(arguments));
    }
    if (aggCodec)
    {
        /// One prompt per window: the argument is the window's JSON_AGG array.
        const auto answer = co_await complete(aggCodec->buildPrompt(arguments[0]));
        co_return AsyncCallResult{aggCodec->parse(answer)};
    }
    if (payload.config.answerMode == AnswerMode::LABEL && !joinCodec)
    {
        co_return co_await callLabel(std::move(arguments));
    }
    if (joinCodec)
    {
        /// One pair per prompt; the reference numbers pairs `<window>_<i>_<j>`, a single pair needs no more than the row id.
        const std::vector pairs{JoinPairPayload{.pairId = "row1", .left = std::move(arguments[0]), .right = std::move(arguments[1])}};
        const auto answer = co_await complete(joinCodec->buildPrompt(pairs));
        co_return AsyncCallResult{joinCodec->parse(answer, pairs).front() ? "true" : "false"};
    }
    /// LLM.IMAGE_FIELDS: the file is read here, on the event loop. Images of 20-100 KB come from the page cache in tens of
    /// microseconds; an asynchronous file read would be a framework feature (docs/technical/semantic_images.md).
    std::vector<ImageAttachment> images;
    std::optional<std::string> unusable;
    for (size_t index = 0; index < imageFieldPositions.size(); ++index)
    {
        auto& value = arguments[imageFieldPositions[index]];
        auto image = loadImage(value, payload.config.imageRoot, payload.config.imageMaxBytes);
        if (!image)
        {
            unusable = std::move(image).error();
            break;
        }
        images.push_back(std::move(image).value());
        value = imagePlaceholder(index);
    }
    RowPayload row{.rowId = "row1", .fields = {}};
    for (size_t field = 0; field < arguments.size(); ++field)
    {
        row.fields.emplace_back(payload.inputFields[field], std::move(arguments[field]));
    }
    const std::vector rows{std::move(row)};
    if (unusable)
    {
        /// Like an unusable answer: the step's default value, a filter's 'false'; no request is sent.
        NES_WARNING("SEM_CALL of model '{}' cannot use the row's image, writing the default: {}", payload.model, *unusable);
        SemanticCallStats::instance().recordUnusableImage();
        co_return decode(std::string{}, rows).front();
    }
    const auto prompt = mapCodec ? mapCodec->buildPrompt(rows) : filterCodec->buildPrompt(rows);
    const auto answer = co_await complete(prompt, nullptr, std::move(images));
    co_return decode(answer, rows).front();
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncCallResult> SemanticCallAsyncFunction::callLabel(std::vector<std::string> arguments)
{
    const auto& config = payload.config;
    const auto& step = config.steps.front();
    const auto values = labelValues(step);
    const auto text = spaceJoined(arguments);
    const bool withConfidence = !config.confidenceField.empty();
    std::pair<std::string, TokenLogprobs> completion;
    if (mock)
    {
        if (mock->getDelay() > std::chrono::milliseconds{0})
        {
            boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, mock->getDelay()};
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        auto answer = mock->respondLabel(text);
        if (!answer)
        {
            throw CannotReadFromSource("SEM_CALL of model '{}': {}", payload.model, answer.error().message);
        }
        completion = std::move(answer).value();
    }
    else
    {
        const auto inputLabel = config.inputLabel.empty() ? fmt::format("{}", fmt::join(payload.inputFields, ", ")) : config.inputLabel;
        auto request = buildChatRequest(config.modelName, buildLabelPrompt(step.prompt, values, inputLabel, text), {}, 0);
        applyLabelRequestFields(request, config.maxTokens);
        if (withConfidence)
        {
            applyLogprobRequestFields(request);
        }
        completion = co_await exchange(std::move(request), nullptr);
    }
    const auto answer = parseLabelAnswer(completion.first, values);
    AsyncCallResult result;
    if (step.kind == SemanticStep::Kind::FILTER)
    {
        result.emplace_back(answer == "true" ? "true" : "false");
    }
    else
    {
        result.push_back(answer.value_or(step.defaultValue));
    }
    if (withConfidence)
    {
        /// A filter's p(true) whatever it answered; a map's probability of the value it gave.
        const auto target = step.kind == SemanticStep::Kind::FILTER ? std::optional<std::string>{"true"} : answer;
        const auto probability = target ? labelProbability(completion.second, values, *target) : std::nullopt;
        result.push_back(probability ? formatProbability(*probability) : std::string{});
    }
    co_return result;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncBatchResult> SemanticCallAsyncFunction::callBatch(std::vector<std::vector<std::string>> rows)
{
    INVARIANT(
        !scanCodec && !windowCodec && !aggCodec && imageFieldPositions.empty(),
        "SEM_CALL of model '{}' takes one row at a time",
        payload.model);
    AsyncBatchResult results;
    results.reserve(rows.size());
    if (joinCodec)
    {
        std::vector<JoinPairPayload> pairs;
        pairs.reserve(rows.size());
        for (size_t position = 0; position < rows.size(); ++position)
        {
            pairs.push_back(
                JoinPairPayload{
                    .pairId = batchRowId(position), .left = std::move(rows[position][0]), .right = std::move(rows[position][1])});
        }
        const auto answer = co_await complete(joinCodec->buildPrompt(pairs));
        const auto parsed = parseLlmJson(answer);
        const auto verdicts = joinCodec->parse(answer, pairs);
        size_t missing = 0;
        for (size_t position = 0; position < pairs.size(); ++position)
        {
            const bool mentioned = mentions(parsed, pairs[position].pairId);
            missing += mentioned ? 0 : 1;
            results.push_back(
                mentioned || !payload.config.retryMissingRows ? std::optional{AsyncCallResult{verdicts[position] ? "true" : "false"}}
                                                              : std::nullopt);
        }
        SemanticCallStats::instance().recordBatch(pairs.size(), missing);
        co_return results;
    }
    std::vector<RowPayload> payloads;
    payloads.reserve(rows.size());
    for (size_t position = 0; position < rows.size(); ++position)
    {
        RowPayload row{.rowId = batchRowId(position), .fields = {}};
        for (size_t field = 0; field < rows[position].size(); ++field)
        {
            row.fields.emplace_back(payload.inputFields[field], std::move(rows[position][field]));
        }
        payloads.push_back(std::move(row));
    }
    const auto prompt = mapCodec ? mapCodec->buildPrompt(payloads) : filterCodec->buildPrompt(payloads);
    const auto answer = co_await complete(prompt);
    const auto parsed = parseLlmJson(answer);
    auto decoded = decode(answer, payloads);
    size_t missing = 0;
    for (size_t position = 0; position < payloads.size(); ++position)
    {
        /// The codec already gave a row the answer leaves out its default value (a filter's 'false').
        const bool mentioned = mentions(parsed, payloads[position].rowId);
        missing += mentioned ? 0 : 1;
        results.push_back(mentioned || !payload.config.retryMissingRows ? std::optional{std::move(decoded[position])} : std::nullopt);
    }
    SemanticCallStats::instance().recordBatch(payloads.size(), missing);
    co_return results;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncCallResult> SemanticCallAsyncFunction::callScan(std::vector<std::string> arguments)
{
    const auto& keyed = *payload.config.keyed;
    const auto& key = arguments.front();
    const auto text = spaceJoined(std::span{arguments}.subspan(1));
    std::optional<std::string> prior;
    {
        const std::scoped_lock lock(keyedState->mutex);
        if (const auto state = keyedState->scans.find(key); state != keyedState->scans.end())
        {
            prior = state->second;
        }
    }
    /// Without an INIT_PROMPT, a key's first row starts from INITIAL_STATE without a call.
    if (!prior && keyed.initPrompt.empty())
    {
        const std::scoped_lock lock(keyedState->mutex);
        keyedState->scans[key] = keyed.initialState;
        co_return AsyncCallResult{keyed.initialState, ""};
    }
    const auto answer = scanCodec->parse(co_await complete(scanCodec->buildPrompt(text, prior)));
    if (!answer)
    {
        /// The reference keeps the key's state rather than fold every later row into a default.
        NES_WARNING("SEM_SCAN of model '{}' could not read the answer for key '{}'; its state stays as it was", payload.model, key);
        co_return AsyncCallResult{payload.config.steps.front().defaultValue, ""};
    }
    {
        const std::scoped_lock lock(keyedState->mutex);
        keyedState->scans[key] = answer->state;
    }
    co_return AsyncCallResult{answer->state, answer->confidence};
}

struct SemanticCallAsyncFunction::WindowScoring
{
    std::string text;
    size_t perCall = 1;
    std::vector<const SemanticWindow*> active;
    /// The scores of each chunk, in window order.
    std::vector<std::vector<SemanticWindowCodec::Score>> scores;
    size_t next = 0;
};

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncCallResult> SemanticCallAsyncFunction::callWindow(std::vector<std::string> arguments)
{
    const auto& keyed = *payload.config.keyed;
    const auto& key = arguments[0];
    const auto& start = arguments[1];
    const auto text = spaceJoined(std::span{arguments}.subspan(2));
    SemanticWindowState* state = nullptr;
    {
        /// Elements of an unordered_map stay where they are when other keys are added.
        const std::scoped_lock lock(keyedState->mutex);
        state = &keyedState->windows[key];
    }
    std::vector<const SemanticWindow*> active;
    active.reserve(state->active.size());
    for (const auto& id : state->active)
    {
        active.push_back(&state->windows.at(id));
    }
    /// One request per `WINDOWS_PER_CALL` windows, all in parallel as in the reference's `asyncio.gather`, on up to
    /// `MAX_CONCURRENCY` connections. The state only changes once all scores are in, so a retry starts over cleanly.
    auto scoring = std::make_shared<WindowScoring>();
    scoring->text = text;
    scoring->perCall = std::max<size_t>(1, keyed.windowsPerCall);
    scoring->active = std::move(active);
    const size_t chunks = (scoring->active.size() + scoring->perCall - 1) / scoring->perCall;
    scoring->scores.resize(chunks);
    const size_t connections = std::min(chunks, std::max<size_t>(1, payload.config.maxConcurrency));
    while (http && windowClients.size() + 1 < connections)
    {
        windowClients.push_back(
            std::make_unique<SemanticHttpClient>(
                payload.config.endpoint + "/chat/completions", apiKey, std::chrono::milliseconds{payload.config.requestTimeout}));
    }
    if (connections > 0)
    {
        /// The workers run on this coroutine's executor, the Async source's strand, so they never touch `scoring` at once.
        const auto executor = co_await boost::asio::this_coro::executor;
        using Operation = decltype(boost::asio::co_spawn(executor, scoreWindows(scoring, nullptr, false), boost::asio::deferred));
        std::vector<Operation> operations;
        operations.reserve(connections);
        for (size_t connection = 0; connection < connections; ++connection)
        {
            SemanticHttpClient* const client = connection == 0 || !http ? nullptr : windowClients[connection - 1].get();
            operations.push_back(boost::asio::co_spawn(executor, scoreWindows(scoring, client, connection > 0), boost::asio::deferred));
        }
        auto [order, failures] = co_await boost::asio::experimental::make_parallel_group(std::move(operations))
                                     .async_wait(boost::asio::experimental::wait_for_all(), boost::asio::use_awaitable);
        for (const auto& failure : failures)
        {
            if (failure)
            {
                std::rethrow_exception(failure);
            }
        }
    }
    std::vector<SemanticWindowCodec::Score> scores;
    for (auto& chunk : scoring->scores)
    {
        for (auto& score : chunk)
        {
            scores.push_back(std::move(score));
        }
    }
    co_return windowCodec->assign(*state, text, start, scores);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<void>
SemanticCallAsyncFunction::scoreWindows(std::shared_ptr<WindowScoring> scoring, SemanticHttpClient* client, const bool extra)
{
    while (scoring->next < scoring->scores.size())
    {
        /// The call itself holds one permit of the shared limit, which the first worker uses. A further one only works
        /// while a permit is free; waiting for one could wait on calls that wait for this one, so it stops instead and
        /// leaves the chunks to the others.
        std::optional<AsyncResourceLimiter::Permit> permit;
        if (extra && sharedLimiter)
        {
            permit = sharedLimiter->tryAcquire();
            if (!permit)
            {
                co_return;
            }
        }
        const size_t chunk = scoring->next++;
        const size_t begin = chunk * scoring->perCall;
        const std::vector windows(
            scoring->active.begin() + static_cast<std::ptrdiff_t>(begin),
            scoring->active.begin() + static_cast<std::ptrdiff_t>(std::min(scoring->active.size(), begin + scoring->perCall)));
        const auto answer = co_await complete(windowCodec->buildPrompt(scoring->text, windows), client);
        scoring->scores[chunk] = windowCodec->parse(answer, scoring->text, windows);
    }
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<std::string>
SemanticCallAsyncFunction::complete(std::string prompt, SemanticHttpClient* const client, std::vector<ImageAttachment> images)
{
    auto request = buildChatRequest(payload.config.modelName, prompt, images, payload.config.maxTokens);
    if (mock)
    {
        if (mock->getDelay() > std::chrono::milliseconds{0})
        {
            boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, mock->getDelay()};
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
        /// The images as the request carries them, so a test sees what the HTTP backend would send.
        auto answer = images.empty() ? mock->respond(prompt) : mock->respond(prompt, imagesOfChatRequest(request));
        if (!answer)
        {
            throw CannotReadFromSource("SEM_CALL of model '{}': {}", payload.model, answer.error().message);
        }
        co_return std::move(answer).value();
    }
    co_return (co_await exchange(std::move(request), client)).first;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<std::pair<std::string, TokenLogprobs>>
SemanticCallAsyncFunction::exchange(nlohmann::json request, SemanticHttpClient* const client)
{
    const auto start = std::chrono::steady_clock::now();
    std::optional<SemanticHttpResponse> exchanged;
    try
    {
        exchanged = co_await (client != nullptr ? *client : *http).post(request.dump());
    }
    /// Records the failed call and rethrows it unchanged; CPPTRACE_TRY cannot wrap a co_await.
    catch (...) /// NOLINT(no-raw-catch-all)
    {
        SemanticCallStats::instance().recordCall(start, std::chrono::steady_clock::now(), true);
        throw;
    }
    const auto& response = *exchanged;
    SemanticCallStats::instance().recordCall(start, std::chrono::steady_clock::now(), response.status < 200 || response.status >= 300);
    if (response.status < 200 || response.status >= 300)
    {
        if (isTransientStatus(response.status))
        {
            SemanticCallStats::instance().recordTransientStatus(response.status);
            throw AsyncTransientFailure(
                CannotReadFromSource("SEM_CALL of model '{}' got status {}: {}", payload.model, response.status, response.body),
                response.retryAfter ? parseRetryAfter(*response.retryAfter) : std::nullopt);
        }
        throw CannotReadFromSource("SEM_CALL of model '{}' got status {}: {}", payload.model, response.status, response.body);
    }
    const auto envelope = nlohmann::json::parse(response.body, nullptr, false);
    if (auto content = messageContent(envelope))
    {
        co_return std::pair{std::move(content).value(), firstTokenLogprobs(envelope)};
    }
    /// An answer without content is unusable, not a transport failure: the codec fills in the defaults.
    NES_WARNING("SEM_CALL of model '{}' got a response without chat-completion content: {}", payload.model, response.body);
    co_return std::pair{std::string{}, TokenLogprobs{}};
}

void SemanticCallAsyncFunction::retryScheduled(const std::chrono::milliseconds delay, bool /*transient*/)
{
    SemanticCallStats::instance().recordRetry(delay);
}

std::vector<AsyncCallResult> SemanticCallAsyncFunction::decode(const std::string& answer, const std::vector<RowPayload>& rows) const
{
    if (mapCodec)
    {
        return mapCodec->parse(answer, rows);
    }
    std::vector<AsyncCallResult> decoded;
    for (const auto& result : filterCodec->parse(answer, rows))
    {
        AsyncCallResult values;
        values.reserve(payload.config.steps.size());
        size_t mapStep = 0;
        for (const auto& step : payload.config.steps)
        {
            /// Fused FILTER steps share one verdict: the codec only reports whether the row passed all of them.
            if (step.kind == SemanticStep::Kind::MAP)
            {
                values.push_back(result.mapAnswers.at(mapStep++));
            }
            else
            {
                values.emplace_back(result.passed ? "true" : "false");
            }
        }
        decoded.push_back(std::move(values));
    }
    return decoded;
}

}
