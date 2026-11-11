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

#include <MockSemanticBackend.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <chrono>
#include <cstdint>
#include <exception>
#include <expected>
#include <initializer_list>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SemanticBackend.hpp>
#include <SemanticImage.hpp>

namespace NES
{

namespace
{

constexpr std::string_view LabelPrefix = "label:";

/// Separates the optional delay from the behaviour, as in 'echo@200'.
constexpr char DelaySeparator = '@';

struct Behaviour
{
    MockSemanticBackend::Mode mode;
    std::string label;
    /// How long one request takes. Zero unless the behaviour names a delay.
    std::chrono::milliseconds delay{0};
};

std::optional<Behaviour> parseBehaviour(std::string_view behaviour)
{
    /// A trailing '@<ms>' makes the mock take that long per request, which is what a model does
    /// and what nothing else in a hermetic test can stand in for: it is the only way to measure
    /// the asynchronous framework's throughput, or to show that a blocking call occupies a worker
    /// thread, without a model server. It composes with every behaviour below.
    std::chrono::milliseconds delay{0};
    if (const auto separator = behaviour.rfind(DelaySeparator); separator != std::string_view::npos)
    {
        const auto digits = behaviour.substr(separator + 1);
        int64_t milliseconds = 0;
        const auto* const end = digits.data() + digits.size();
        /// from_chars reads the range [data, end), no terminator needed.
        const auto parsed = std::from_chars(digits.data(), end, milliseconds); /// NOLINT(bugprone-suspicious-stringview-data-usage)
        /// Only a suffix that is entirely digits is a delay. Anything else belongs to the
        /// behaviour itself — a label is free text and may well contain an '@'.
        if (!digits.empty() && parsed.ec == std::errc{} && parsed.ptr == end)
        {
            delay = std::chrono::milliseconds{milliseconds};
            behaviour = behaviour.substr(0, separator);
        }
    }

    const auto withDelay = [delay](Behaviour parsed)
    {
        parsed.delay = delay;
        return std::make_optional(parsed);
    };

    if (behaviour == "echo")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::ECHO, .label = {}});
    }
    if (behaviour == "unparseable")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::UNPARSEABLE, .label = {}});
    }
    if (behaviour == "count")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::COUNT, .label = {}});
    }
    if (behaviour == "concat")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::CONCAT, .label = {}});
    }
    if (behaviour == "match")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::MATCH, .label = {}});
    }
    for (const auto& [name, mode] : std::initializer_list<std::pair<std::string_view, MockSemanticBackend::Mode>>{
             {"batchinfo", MockSemanticBackend::Mode::BATCH_INFO},
             {"echo-missing", MockSemanticBackend::Mode::ECHO_MISSING},
             {"echo-extra", MockSemanticBackend::Mode::ECHO_EXTRA},
             {"fail-batch", MockSemanticBackend::Mode::FAIL_BATCH},
             {"scan", MockSemanticBackend::Mode::SCAN},
             {"window", MockSemanticBackend::Mode::WINDOW},
             {"imageinfo", MockSemanticBackend::Mode::IMAGE_INFO},
             {"prob", MockSemanticBackend::Mode::PROB}})
    {
        if (behaviour == name)
        {
            return withDelay(Behaviour{.mode = mode, .label = {}});
        }
    }
    if (behaviour == "fail")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::FAIL, .label = {}});
    }
    if (behaviour.starts_with(LabelPrefix))
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::LABEL, .label = std::string(behaviour.substr(LabelPrefix.size()))});
    }
    return std::nullopt;
}

std::string toUpper(std::string text)
{
    std::ranges::transform(text, text.begin(), [](const unsigned char character) { return static_cast<char>(std::toupper(character)); });
    return text;
}

std::string toLower(std::string text)
{
    std::ranges::transform(text, text.begin(), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
    return text;
}

/// The row payload is the prompt's last line. JSON escapes every newline inside it, so the last
/// "\nData: " is the real block even when a row's text contains that string. Ordered, so a
/// JSON_OBJECT row keeps its fields in declared order.
nlohmann::ordered_json dataBlock(const std::string& prompt)
{
    constexpr std::string_view Marker = "\nData: ";
    const auto position = prompt.rfind(Marker);
    if (position == std::string::npos)
    {
        return nlohmann::ordered_json::object();
    }
    auto parsed = nlohmann::ordered_json::parse(prompt.substr(position + Marker.size()), nullptr, false);
    return parsed.is_object() ? parsed : nlohmann::ordered_json::object();
}

/// The JSON value on the line after `marker`, or null.
nlohmann::ordered_json jsonAfter(const std::string& prompt, const std::string_view marker)
{
    const auto position = prompt.rfind(marker);
    if (position == std::string::npos)
    {
        return nullptr;
    }
    const auto begin = position + marker.size();
    const auto end = prompt.find('\n', begin);
    return nlohmann::ordered_json::parse(prompt.substr(begin, end == std::string::npos ? std::string::npos : end - begin), nullptr, false);
}

/// The window mock's topic of a text: what comes before its first ':', or all of it.
std::string topicOf(const std::string_view text)
{
    return std::string{text.substr(0, text.find(':'))};
}

/// The row's text: a SPACE_JOINED payload is already a string, a JSON_OBJECT payload has its
/// values joined the same way.
std::string rowText(const nlohmann::ordered_json& payload)
{
    if (payload.is_string())
    {
        return payload.get<std::string>();
    }
    std::string joined;
    for (const auto& value : payload)
    {
        if (!joined.empty())
        {
            joined.push_back(' ');
        }
        joined += value.is_string() ? value.get<std::string>() : value.dump();
    }
    return joined;
}

}

namespace
{
/// "<mime>:<bytes>:<fnv1a64 hex>" per image, joined by ';', or "none".
std::string describeImages(const std::span<const ImageAttachment> images)
{
    if (images.empty())
    {
        return "none";
    }
    std::string description;
    for (const auto& image : images)
    {
        if (!description.empty())
        {
            description.push_back(';');
        }
        description += fmt::format("{}:{}:{:016x}", image.mime, image.bytes.size(), fnv1a64(image.bytes));
    }
    return description;
}
}

MockSemanticBackend::MockSemanticBackend(const std::string_view behaviour, std::vector<std::string> outputColumns)
    : mode(Mode::FAIL), outputColumns(std::move(outputColumns))
{
    auto parsed = parseBehaviour(behaviour);
    if (!parsed.has_value())
    {
        throw InvalidSemanticModel(
            "Unknown mock backend behaviour '{}' (expected echo, label:<X>, unparseable, fail, match, count, concat, batchinfo, "
            "echo-missing, echo-extra, fail-batch, scan, window, imageinfo or prob, each optionally followed by @<ms>)",
            behaviour);
    }
    mode = parsed->mode;
    label = std::move(parsed->label);
    delay = parsed->delay;
}

bool MockSemanticBackend::isValidBehaviour(const std::string_view behaviour)
{
    return parseBehaviour(behaviour).has_value();
}

std::expected<std::string, BackendError> MockSemanticBackend::complete(const CompletionRequest& request)
{
    if (delay > std::chrono::milliseconds{0})
    {
        std::this_thread::sleep_for(delay);
    }
    return respond(request.prompt);
}

std::expected<std::string, BackendError> MockSemanticBackend::respond(const std::string& prompt) const
{
    return respond(prompt, {});
}

std::expected<std::pair<std::string, TokenLogprobs>, BackendError> MockSemanticBackend::respondLabel(const std::string_view text) const
{
    switch (mode)
    {
        case Mode::FAIL:
            return std::unexpected{BackendError{.kind = BackendError::Kind::UNREACHABLE, .message = "mock endpoint is unreachable"}};
        case Mode::UNPARSEABLE:
            return std::pair{std::string{"I am sorry, but I cannot classify this row."}, TokenLogprobs{}};
        case Mode::LABEL:
            return std::pair{label, TokenLogprobs{{label, 0.0}}};
        case Mode::PROB: {
            const std::string number{text};
            double probability = 0;
            size_t parsed = 0;
            try
            {
                probability = std::stod(number, &parsed);
            }
            catch (const std::exception&)
            {
                parsed = 0;
            }
            if (parsed == 0 || parsed != number.size() || !(probability >= 0.0 && probability <= 1.0))
            {
                return std::pair{std::string{"not a probability"}, TokenLogprobs{}};
            }
            TokenLogprobs alternatives;
            if (probability > 0.0)
            {
                alternatives.emplace_back("true", std::log(probability));
            }
            if (probability < 1.0)
            {
                alternatives.emplace_back("false", std::log(1.0 - probability));
            }
            return std::pair{std::string{probability >= 0.5 ? "true" : "false"}, std::move(alternatives)};
        }
        default: {
            std::string lower{text};
            std::ranges::transform(lower, lower.begin(), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
            return std::pair{toUpper(std::string{text}), TokenLogprobs{{lower, 0.0}}};
        }
    }
}

std::expected<std::string, BackendError>
MockSemanticBackend::respond(const std::string& prompt, const std::span<const ImageAttachment> images) const
{
    switch (mode)
    {
        case Mode::FAIL:
            /// Fails on the first attempt regardless of maxRetries, so the system test stays fast.
            return std::unexpected{BackendError{.kind = BackendError::Kind::UNREACHABLE, .message = "mock endpoint is unreachable"}};
        case Mode::UNPARSEABLE:
            return "I am sorry, but I cannot classify these rows.";
        case Mode::COUNT:
        case Mode::CONCAT: {
            /// The SEM_AGG layout ends in "Window:\n" and a JSON list of one-field objects.
            constexpr std::string_view Marker = "\nWindow:\n";
            const auto position = prompt.rfind(Marker);
            const auto window = position == std::string::npos
                ? nlohmann::ordered_json::array()
                : nlohmann::ordered_json::parse(prompt.substr(position + Marker.size()), nullptr, false);
            if (!window.is_array())
            {
                return std::string{"not a window"};
            }
            if (mode == Mode::COUNT)
            {
                return std::to_string(window.size());
            }
            std::string joined;
            bool first = true;
            for (const auto& record : window)
            {
                if (!first)
                {
                    joined.push_back('|');
                }
                first = false;
                joined += rowText(record);
            }
            return joined;
        }
        case Mode::MATCH: {
            auto response = nlohmann::json::object();
            for (const auto& [rowId, payload] : dataBlock(prompt).items())
            {
                const bool isPair = payload.is_object() && payload.contains("left") && payload.contains("right")
                    && payload["left"].is_string() && payload["right"].is_string();
                const bool matched = isPair && toLower(payload["left"].get<std::string>()) == toLower(payload["right"].get<std::string>());
                response[rowId] = {{"answer", matched}, {"confidence", 1.0}};
            }
            return response.dump();
        }
        case Mode::SCAN: {
            const auto data = jsonAfter(prompt, "\nData: ");
            const auto prior = jsonAfter(prompt, "\nPrior state for this key: ");
            if (!data.is_object() || data.empty())
            {
                return std::string{"no row"};
            }
            const auto text = rowText(data.begin().value());
            const auto state = prior.is_string() ? prior.get<std::string>() + "|" + text : text;
            return nlohmann::json{{data.begin().key(), {{"answer", state}, {"confidence", 0.9}}}}.dump();
        }
        case Mode::WINDOW: {
            const auto windows = jsonAfter(prompt, "Windows to score:\n");
            constexpr std::string_view TupleMarker = "\n\nNew tuple: ";
            const auto tuple = prompt.rfind(TupleMarker);
            const auto topic
                = topicOf(tuple == std::string::npos ? std::string_view{} : std::string_view{prompt}.substr(tuple + TupleMarker.size()));
            auto response = nlohmann::json::object();
            if (windows.is_object())
            {
                for (const auto& [id, context] : windows.items())
                {
                    std::string windowTopic = context.value("description", "");
                    if (windowTopic.empty() && context.contains("recent_examples") && !context["recent_examples"].empty())
                    {
                        windowTopic = topicOf(context["recent_examples"].back().get<std::string>());
                    }
                    if (windowTopic.empty() && context.contains("most_recent_tuple"))
                    {
                        windowTopic = topicOf(context["most_recent_tuple"].get<std::string>());
                    }
                    const bool fits = windowTopic == topic;
                    response[id] = {
                        {"continuity_score", fits ? 0.95 : 0.05}, {"window_description", windowTopic}, {"new_window_description", topic}};
                }
            }
            return response.dump();
        }
        case Mode::FAIL_BATCH:
            if (dataBlock(prompt).size() > 1)
            {
                return std::unexpected{BackendError{.kind = BackendError::Kind::UNREACHABLE, .message = "mock endpoint rejects batches"}};
            }
            [[fallthrough]];
        case Mode::BATCH_INFO:
        case Mode::ECHO_MISSING:
        case Mode::ECHO_EXTRA:
        case Mode::ECHO:
        case Mode::IMAGE_INFO:
        case Mode::PROB:
        case Mode::LABEL: {
            auto response = nlohmann::json::object();
            const auto rows = dataBlock(prompt);
            size_t position = 0;
            for (const auto& [rowId, payload] : rows.items())
            {
                ++position;
                if (mode == Mode::ECHO_MISSING && rows.size() > 1 && position == rows.size())
                {
                    continue;
                }
                const auto answer = [&]
                {
                    if (mode == Mode::LABEL)
                    {
                        return label;
                    }
                    if (mode == Mode::IMAGE_INFO)
                    {
                        return describeImages(images);
                    }
                    return mode == Mode::BATCH_INFO ? fmt::format("{}/{}", position, rows.size()) : toUpper(rowText(payload));
                }();
                if (outputColumns.empty())
                {
                    /// A filter-only prompt asks for one verdict per row, without field names.
                    response[rowId] = {{"answer", answer}, {"confidence", 1.0}};
                    continue;
                }
                auto fields = nlohmann::json::object();
                for (const auto& column : outputColumns)
                {
                    fields[toLower(column)] = {{"answer", answer}, {"confidence", 1.0}};
                }
                response[rowId] = std::move(fields);
            }
            if (mode == Mode::ECHO_EXTRA)
            {
                response["row999"] = outputColumns.empty()
                    ? nlohmann::json{{"answer", "EXTRA"}, {"confidence", 1.0}}
                    : nlohmann::json{{toLower(outputColumns.front()), {{"answer", "EXTRA"}, {"confidence", 1.0}}}};
            }
            return response.dump();
        }
    }
    std::unreachable();
}

}
